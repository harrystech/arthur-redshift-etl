"""
Utilities to interact with Amazon S3 and occasionally the local file system.

The basic location of a file depends on the data source and can be one of:
.../schemas/{source_or_schema_name}/{source_schema_name}-{table_name}.yaml -- for table design files
.../schemas/{schema_name}/{source_schema_name}-{table_name}.sql -- for queries for CTAS or views
.../data/{source_name}/{source_schema_name}-{table_name}.manifest -- for a manifest of data files
.../data/{source_name}/{source_schema_name}-{table_name}/csv/part-* -- for the data files themselves.

If the files are in S3, then the start of the path is always s3://{bucket_name}/{prefix}/...

If the files are stored locally, then the start of the path is probably simply the current directory ('.').

For tables that are backed by upstream sources, the directory after 'schemas' or 'data' will be the
name of the source in the configuration file.

For CTAS or views, the directory after 'schemas' is the name of the schema in the data warehouse
configuration.  The 'source_schema_name' is only used for sorting.
"""

from collections import defaultdict
from datetime import datetime
from functools import partial
import logging
from itertools import groupby
from operator import attrgetter
import os
import os.path
import re
import threading

import boto3
import botocore.exceptions

import etl
import etl.config


_resources_for_thread = threading.local()


class BadSourceDefinitionError(etl.ETLException):
    pass


class S3ServiceError(etl.ETLException):
    pass


class TableFileSet:
    """
    Class to hold design file, SQL file and data files (CSV and manifest) belonging to a table.

    Note that tables are addressed using their target name, where the schema is equal
    to the source name.  To allow for sorting, the original schema name (in the source
    database) is kept.  For views and CTAS, the sort order is used to create a predictable
    instantiation order where one view or CTAS may depend on another being up-to-date already.
    """

    def __init__(self, source_table_name, target_table_name, design_file):
        self._source_table_name = source_table_name
        self._target_table_name = target_table_name
        self._design_file = design_file
        self._sql_file = None
        self._manifest_file = None
        self._data_files = []

    def __str__(self):
        extensions = [".yaml"]
        if self._sql_file:
            extensions.append(".sql")
        if self._manifest_file:
            extensions.append(".manifest")
        if self._data_files:
            extensions.append("/csv/part-*")
        return "{}('{}{{{}}}')".format(self.__class__.__name__, self.source_path_name, ','.join(extensions))

    @property
    def source_name(self):
        return self._target_table_name.schema

    @property
    def source_table_name(self):
        return self._source_table_name

    @property
    def target_table_name(self):
        return self._target_table_name

    @property
    def source_path_name(self):
        return "{}/{}-{}".format(self._target_table_name.schema,
                                 self._source_table_name.schema,
                                 self._source_table_name.table)

    @property
    def design_file(self):
        return self._design_file

    @property
    def sql_file(self):
        return self._sql_file

    @property
    def manifest_file(self):
        return self._manifest_file

    @property
    def data_files(self):
        return tuple(self._data_files)

    def __len__(self):
        return 1 + (self._sql_file is not None) + (self._manifest_file is not None) + len(self._data_files)

    def set_sql_file(self, filename):
        self._sql_file = filename

    def set_manifest_file(self, filename):
        self._manifest_file = filename

    def add_data_file(self, filename):
        self._data_files.append(filename)


def _get_bucket(name):
    """
    Return new Bucket object for a bucket that does exist (waits until it does)
    """
    s3 = getattr(_resources_for_thread, 's3', None)
    if s3 is None:
        # When multi-threaded, we can't use the default session.  So keep one per thread.
        session = boto3.session.Session()
        s3 = session.resource("s3")
        setattr(_resources_for_thread, 's3', s3)
    return s3.Bucket(name)


def upload_to_s3(filename, bucket_name, object_key, dry_run=False):
    """
    Upload a local file to S3 bucket with the given object key.
    """
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
    else:
        logger.info("Uploading '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
        bucket = _get_bucket(bucket_name)
        bucket.upload_file(filename, object_key)


def delete_in_s3(bucket_name: str, object_keys: list, dry_run: bool=False, retry: bool=True):
    """
    Delete objects from bucket.
    """
    logger = logging.getLogger(__name__)
    if dry_run:
        for key in object_keys:
            logger.info("Dry-run: Skipping deletion of 's3://%s/%s'", bucket_name, key)
    else:
        bucket = _get_bucket(bucket_name)
        keys = [{'Key': key} for key in object_keys]
        failed = []
        chunk_size = 1000
        while len(keys) > 0:
            result = bucket.delete_objects(Delete={'Objects': keys[:chunk_size]})
            del keys[:chunk_size]
            for deleted in result.get('Deleted', {}):
                logger.info("Deleted 's3://%s/%s'", bucket_name, deleted['Key'])
            for error in result.get('Errors', {}):
                logger.error("Failed to delete 's3://%s/%s' with %s: %s", bucket_name, error['Key'],
                             error['Code'], error['Message'])
                failed.append(error['Key'])
        if failed:
            if retry:
                logger.warning("Failed to delete %d objects, trying one more time", len(failed))
                delete_in_s3(bucket_name, failed, retry=False)
            else:
                raise S3ServiceError("Failed to delete %d files" % len(failed))


def get_last_modified(bucket_name, object_key):
    """
    Return last_modified timestamp from the object.  Returns None if object does not exist.

    Assuming that you're actually expecting this file to exist, this method helpfully waits first
    for the object to exist.
    """
    logger = logging.getLogger(__name__)
    bucket = _get_bucket(bucket_name)
    try:
        s3_object = bucket.Object(object_key)
        s3_object.wait_until_exists()
        timestamp = s3_object.last_modified
        logger.debug("Object in 's3://%s/%s' was last modified %s", bucket_name, object_key, timestamp)
    except botocore.exceptions.WaiterError:
        logger.debug("Waiting for object in 's3://%s/%s' failed", bucket_name, object_key)
        timestamp = None
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logger.debug("Object 's3://%s/%s' does not exist", bucket_name, object_key)
            timestamp = None
        else:
            raise
    return timestamp


def object_stat(bucket_name, object_key):
    """
    Return content_length and last_modified timestamp from the object.
    It is an error if the object does not exist.
    """
    bucket = _get_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    return s3_object.content_length, s3_object.last_modified


def local_file_stat(filename):
    """
    Return size in bytes and last modification timestamp from local file.
    """
    return os.path.getsize(filename), datetime.utcfromtimestamp(os.path.getmtime(filename)).isoformat(' ')


def get_file_content(bucket_name, object_key):
    """
    Return stream for content of s3://bucket_name/object_key

    You must close the stream when you're done with it.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Downloading 's3://%s/%s'", bucket_name, object_key)
    bucket = _get_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    response = s3_object.get()
    logger.debug("Received response from S3: last modified: %s, content length: %s, content type: %s",
                 response['LastModified'], response['ContentLength'], response['ContentType'])
    return response['Body']


def list_files_in_folder(bucket_name, prefix):
    """
    List all the files in "s3://{bucket_name}/{prefix}" (where prefix is probably a path and not an object key)
    or if prefix is a tuple, then list files in "s3://{bucket_name}/{common path} which start with one of
    the elements in the prefix tuple.
    """
    logging.getLogger(__name__).info("Looking for files at 's3://%s/%s'", bucket_name, prefix)
    bucket = _get_bucket(bucket_name)
    if isinstance(prefix, tuple):
        for obj in bucket.objects.filter(Prefix=os.path.commonprefix(prefix)):
            if obj.key.startswith(prefix):
                yield obj.key
    elif isinstance(prefix, str):
        for obj in bucket.objects.filter(Prefix=prefix):
            yield obj.key
    else:
        raise ValueError("prefix must be string or tuple (of strings)")


def list_local_files(directory):
    """
    List all files in and anywhere below this directory.

    It is an error if the directory does not exist.
    Ignore swap files along the way.
    """
    logger = logging.getLogger(__name__)
    normed_directory = os.path.normpath(directory)
    if not os.path.isdir(normed_directory):
        raise FileNotFoundError("Failed to find directory: '%s'" % normed_directory)
    logger.info("Looking for files locally in '%s'", normed_directory)
    for root, dirs, files in os.walk(os.path.normpath(normed_directory)):
        for filename in sorted(files):
            if not filename.endswith(('.swp', '~', '.DS_Store')):
                yield os.path.join(root, filename)


def find_files_for_schemas(bucket_name, prefix, schemas, pattern):
    """
    Discover design and data files in the given bucket and folder, for the given schemas,
    and apply pattern-based selection along the way.

    Only files in 'data' or 'schemas' sub-folders are examined (so this ignores 'config' or 'jars').
    """
    schema_names = [schema["name"] for schema in schemas]
    files = list_files_in_folder(bucket_name, (prefix + '/data', prefix + '/schemas'))
    return _find_files_from(files, schema_names, pattern)


def find_local_files_for_schemas(directory, schemas, pattern, orphans=False):
    """
    Discover local design and data files starting from the given directory, for the given schemas,
    and apply pattern-based selection along the way.

    The directory should be the './schemas' directory, probably.
    """
    schema_names = [schema["name"] for schema in schemas]
    files = list_local_files(directory)
    return _find_files_from(files, schema_names, pattern, orphans=orphans)


def delete_files_in_bucket(bucket_name, prefix, schema_names, selection, dry_run=False):
    """
    Delete all files that might be relevant given the choice of schemas and the target selection.
    """
    iterable = list_files_in_folder(bucket_name, (prefix + '/data', prefix + '/schemas'))
    deletable = [filename for filename, values in _find_matching_files_from(iterable, schema_names, selection,
                                                                            return_extra_files=True)]
    delete_in_s3(bucket_name, deletable, dry_run=dry_run)


def _find_matching_files_from(iterable, schema_names, pattern, return_extra_files=False):
    """
    Match file names against schemas list, the target pattern and expected path format,
    return file information based on source name, source schema, table name and file type.

    This generator provides all files that are possibly relevant and it
    is up to the consumer to ensure consistency (e.g. that a design
    file exists or that a SQL file is not present along with a manifest).

    Files ending in '_SUCCESS' or '_$folder$' are ignored (which are created by some Spark jobs).
    """
    file_names_re = re.compile(r"""(?:^schemas|/schemas|^data|/data)
                                   /(?P<source_name>\w+)
                                   /(?P<schema_name>\w+)-(?P<table_name>\w+)
                                   (?:(?P<file_ext>.yaml|.sql|.manifest|/csv/part-.*(:?\.gz)?)
                                      |.*(?P<ignorable_suffix>_SUCCESS|_\$folder\$))$
                               """, re.VERBOSE)

    for filename in iterable:
        match = file_names_re.search(filename)
        if match:
            values = match.groupdict()
            source_name = values['source_name']
            if source_name in schema_names:
                target_table_name = etl.TableName(source_name, values['table_name'])
                if pattern.match(target_table_name):
                    if values['ignorable_suffix']:
                        if not return_extra_files:
                            continue
                    elif values["file_ext"].startswith("/csv"):
                        values["file_type"] = "data"
                    else:
                        values["file_type"] = values["file_ext"][1:]
                    yield (filename, values)
        elif not filename.endswith("_$folder$"):
            logging.getLogger(__name__).warning("Found file not matching expected format: '%s'", filename)


def _find_files_from(iterable, schema_names, pattern, orphans=False):
    """
    Return list of file sets groupbed by source name, ordered by source_schema_name and source_table_name.

    Note that all tables must have a table design file. It's not ok to have a CSV or SQL file by itself.
    """
    logger = logging.getLogger(__name__)
    # First pass -- pick up all the design files, keep matches around for second pass
    targets = {}
    additional_files = defaultdict(list)
    for filename, values in _find_matching_files_from(iterable, schema_names, pattern):
        source_table_name = etl.TableName(values["schema_name"], values["table_name"])
        target_table_name = etl.TableName(values["source_name"], values["table_name"])
        if values['file_type'] == 'yaml':
            if target_table_name in targets:
                raise BadSourceDefinitionError("Target table '%s' has multiple sources" % target_table_name.identifier)
            targets[target_table_name] = TableFileSet(source_table_name, target_table_name, filename)
        else:
            additional_files[target_table_name].append((filename, values))
    # Second pass -- only store SQL and data files for tables that have design files from first pass
    for target_table_name in additional_files:
        if target_table_name in targets:
            file_set = targets[target_table_name]
            for filename, values in additional_files[target_table_name]:
                _add_file(file_set, filename, values)
        else:
            if orphans:
                source_table_name = etl.TableName(values["schema_name"], values["table_name"])
                target_table_name = etl.TableName(values["source_name"], values["table_name"])
                orphan_fileset = TableFileSet(source_table_name, target_table_name, None)
                for filename, values in additional_files[target_table_name]:
                    _add_file(orphan_fileset, filename, values)
                targets[target_table_name] = orphan_fileset
            else:
                logger.warning("Found files without corresponding table design: %s",
                               ", ".join("'{}'".format(filename) for filename, values in additional_files[target_table_name]))
    # Always return files sorted by sources (in original order) and target name
    schemas_order = {name: order for order, name in enumerate(schema_names)}
    file_sets = sorted(targets.values(), key=lambda fs: (schemas_order[fs.source_name], fs.source_path_name))
    logger.info("Found %d matching file(s) for %d table(s)", sum(len(fs) for fs in file_sets), len(file_sets))
    return file_sets


def _add_file(file_set, filename, values):
    """
    Use appropriate TableFileSet setter to attach the file described by `filename` and `values`
    to the TableFileSet `fileset`
    """
    file_type = values['file_type']
    if file_type == 'sql':
        file_set.set_sql_file(filename)
    elif file_type == 'manifest':
        file_set.set_manifest_file(filename)
    elif file_type == 'data':
        file_set.add_data_file(filename)


def approx_pretty_size(total_bytes):
    """
    Return a humane and pretty size approximation.

    This looks silly bellow 1KB but I'm OK with that.
    Don't call this with negative total_bytes or your pet hamster will go bald.

    >>> approx_pretty_size(50)
    '1KB'
    >>> approx_pretty_size(2000)
    '2KB'
    >>> approx_pretty_size(2048)
    '2KB'
    >>> approx_pretty_size(3000000)
    '3MB'
    >>> approx_pretty_size(4000000000)
    '4GB'
    >>> approx_pretty_size(-314)
    Traceback (most recent call last):
        ...
    ValueError: total_bytes may not be negative
    """
    if total_bytes < 0:
        raise ValueError("total_bytes may not be negative")
    for scale, unit in ((1024 * 1024 * 1024, 'GB'), (1024 * 1024, 'MB'), (1024, 'KB')):
        div, rem = divmod(total_bytes, scale)
        if div > 0:
            if rem > 0:
                div += 1  # always round up
            break
    else:
        div = 1
    return "{:d}{}".format(div, unit)


def list_files(file_sets, bucket_name=None, long_format=False):
    """
    List files in the given S3 bucket (or from current directory).

    With the long format, shows content length and tallies up the total size in bytes.
    """
    if bucket_name:
        scheme_netloc = "s3://{}/".format(bucket_name)
        stat_func = partial(object_stat, bucket_name)
    else:
        scheme_netloc = ""  # meaning we ignore the beauty of file://localhost/...
        stat_func = partial(local_file_stat)
    total_length = 0
    for schema_name, file_group in groupby(file_sets, attrgetter("source_name")):
        print("Schema: '{}'".format(schema_name))
        for file_set in file_group:
            if file_set.manifest_file is None:
                print("    Table: '{}'".format(file_set.target_table_name.table))
            else:
                print("    Table: '{}' (with data from '{}')".format(file_set.target_table_name.table,
                                                                     file_set.source_table_name.identifier))
            files = [file_set.design_file]
            if file_set.sql_file is not None:
                files.append(file_set.sql_file)
            if file_set.manifest_file is not None:
                files.append(file_set.manifest_file)
            if len(file_set.data_files) > 0:
                files.extend(file_set.data_files)
            for filename in files:
                if long_format:
                    content_length, last_modified = stat_func(filename)
                    total_length += content_length
                    print("        {}{} ({:d}, {})".format(scheme_netloc, filename, content_length, last_modified))
                else:
                    print("        {}{}".format(scheme_netloc, filename))
    if total_length > 0:
        print("Total size in bytes: {:d} ({})".format(total_length, approx_pretty_size(total_length)))
