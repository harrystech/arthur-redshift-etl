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

from datetime import datetime
import logging
from itertools import groupby
from operator import attrgetter
import os
import os.path
import re
import threading

import boto3
import botocore.exceptions

from etl import TableName
import etl.config


_resources_for_thread = threading.local()


class BadSourceDefinitionError(etl.ETLError):
    pass


class S3ServiceError(etl.ETLError):
    pass


class TableFileSet:
    """
    Class to hold design file, SQL file and data files (CSV and manifest) belonging to a table.

    Note that all tables are addressed using their "target name" within the data warehouse, where
    the schema name is equal to the source name and the table name is the same as in the upstream
    source. To allow for sorting, the original schema name (in the source
    database) is kept.  For CTAS and VIEWs, the "target name" consists of the schema name within
    the data warehouse and the table or view name assigned.  Since there is no source schema, that
    portion may be omitted.
    """

    def __init__(self, source_table_name, target_table_name):
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.design_file = None
        self.sql_file = None
        self.manifest_file = None
        self._data_files = []
        # Used when binding the files to either local filesystem or S3
        self.scheme = None
        self.netloc = None
        self.path = None

    def __str__(self):
        extensions = []
        if self.design_file:
            extensions.append(".yaml")
        if self.sql_file:
            extensions.append(".sql")
        if self.manifest_file:
            extensions.append(".manifest")
        if self._data_files:
            extensions.append("/csv/part-*")
        return "{}('{}{{{}}}')".format(self.__class__.__name__, self.source_path_name, ','.join(extensions))

    def bind_to_uri(self, scheme, netloc, path):
        self.scheme = scheme
        self.netloc = netloc
        self.path = path

    def uri(self, filename):
        """
        Return the full URI for the filename, which probably should be one of the files from this set
        """
        if self.scheme == "s3":
            return "{0.scheme}://{0.netloc}/{1}".format(self, filename)
        elif self.scheme == "file":
            return filename
        else:
            raise RuntimeError("Illegal scheme in file set")

    def stat(self, filename):
        """
        Return file size (in bytes) and timestamp of last modification for the file which should be one from this set.
        """
        if self.scheme == "s3":
            return object_stat(self.netloc, filename)
        elif self.scheme == "file":
            return local_file_stat(filename)
        else:
            raise RuntimeError("Illegal scheme in file set")

    @property
    def source_name(self):
        return self.target_table_name.schema

    @property
    def source_path_name(self):
        return "{}/{}-{}".format(self.target_table_name.schema,
                                 self.source_table_name.schema,
                                 self.source_table_name.table)

    @property
    def data_files(self):
        return tuple(self._data_files)

    def add_data_file(self, filename):
        self._data_files.append(filename)

    @property
    def files(self):
        _files = []
        if self.design_file:
            _files.append(self.design_file)
        if self.sql_file:
            _files.append(self.sql_file)
        if self.manifest_file:
            _files.append(self.manifest_file)
        if self._data_files:
            _files.extend(self._data_files)
        return _files

    def __len__(self):
        return len(self.files)


def _get_bucket(name):
    """
    Return new Bucket object for a bucket that does exist (waits until it does)
    """
    s3 = getattr(_resources_for_thread, 's3', None)
    if s3 is None:
        # When multi-threaded, we can't use the default session. So keep one per thread.
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


def delete_files_in_bucket(bucket_name, prefix, selection, dry_run=False):
    """
    Delete all files that might be relevant given the choice of schemas and the target selection.
    """
    iterable = list_files_in_folder(bucket_name, (prefix + '/data', prefix + '/schemas'))
    deletable = [filename for filename, v in _find_matching_files_from(iterable, selection, return_success_file=True)]
    delete_in_s3(bucket_name, deletable, dry_run=dry_run)


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


def find_file_sets(uri_parts, selector=None, error_if_empty=True):
    """
    Generic method to collect files from either s3://bucket/prefix or file://localhost/directory
    based on the tuple describing a parsed URI, which should be either
    ("s3", "bucket", "prefix", ...) or ("file", "localhost", "directory", ...)

    If the selector is used, it needs to be a TableNamePatterns instance to select matching files.
    """
    logger = logging.getLogger(__name__)
    scheme, netloc, path = uri_parts[:3]
    if selector is None:
        selector = etl.TableNamePatterns.from_list([])
    if scheme == "s3":
        file_sets = _find_file_sets_from(list_files_in_folder(netloc, (path + '/data', path + '/schemas')), selector)
        if not file_sets:
            raise FileNotFoundError("Found no matching files in 's3://{}/{}' for '{}'".format(netloc, path, selector))
    else:
        if os.path.exists(path):
            file_sets = _find_file_sets_from(list_local_files(path), selector)
            if not file_sets:
                if error_if_empty:
                    raise FileNotFoundError("Found no matching files in '{}' for '{}'".format(path, selector))
                else:
                    logger.warning("Found no matching files in '%s' for '%s' (proceeding recklessly...)",
                                   path, selector)
        elif not error_if_empty:
            file_sets = []
        else:
            raise FileNotFoundError("Failed to find directory: '%s'" % path)
    # Bind the files that were found to where they were found
    for file_set in file_sets:
        file_set.bind_to_uri(scheme, netloc, path)
    return file_sets


def _find_matching_files_from(iterable, pattern, return_success_file=False):
    """
    Match file names against the target pattern and expected path format,
    return file information based on source name, source schema, table name and file type.

    This generator provides all files that are possibly relevant and it
    is up to the consumer to ensure consistency (e.g. that a design
    file exists or that a SQL file is not present along with a manifest).

    Files ending in '_SUCCESS' or '_$folder$' are ignored (which are created by some Spark jobs).
    """
    file_names_re = re.compile(r"""(?:^schemas|/schemas|^data|/data)
                                   /(?P<source_name>\w+)
                                   /(?P<schema_name>\w+)-(?P<table_name>\w+)
                                   (?:(?P<file_ext>.yaml|.sql|.manifest|/csv/(:?part-.*(:?\.gz)?|_SUCCESS)))$
                               """, re.VERBOSE)

    for filename in iterable:
        match = file_names_re.search(filename)
        if match:
            values = match.groupdict()
            target_table_name = TableName(values['source_name'], values['table_name'])
            if pattern.match(target_table_name):
                file_ext = values["file_ext"]
                if file_ext in [".yaml", ".sql", ".manifest"]:
                    values["file_type"] = file_ext[1:]
                elif file_ext.endswith("_SUCCESS"):
                    values["file_type"] = "success"
                elif file_ext.startswith("/csv"):
                    values["file_type"] = "data"
                # E.g. when deleting files out of a folder we want to know about the /csv/_SUCCESS file.
                if return_success_file or values["file_type"] != "success":
                    yield (filename, values)
        elif not filename.endswith("_$folder$"):
            logging.getLogger(__name__).warning("Found file not matching expected format: '%s'", filename)


def _find_file_sets_from(iterable, pattern):
    """
    Return list of file sets ordered by source name, source_schema_name and source_table_name.
    """
    logger = logging.getLogger(__name__)
    targets = {}
    for filename, values in _find_matching_files_from(iterable, pattern):
        source_table_name = etl.TableName(values["schema_name"], values["table_name"])
        target_table_name = etl.TableName(values["source_name"], values["table_name"])
        if target_table_name not in targets:
            targets[target_table_name] = TableFileSet(source_table_name, target_table_name)
        file_set = targets[target_table_name]

        file_type = values['file_type']
        if file_type == 'yaml':
            file_set.design_file = filename
        elif file_type == 'sql':
            file_set.sql_file = filename
        elif file_type == 'manifest':
            file_set.manifest_file = filename
        elif file_type == 'data':
            file_set.add_data_file(filename)

    # Always return files sorted by sources (in original order) and target name
    file_sets = sorted(targets.values(), key=attrgetter("source_path_name"))
    logger.info("Found %d matching file(s) for %d table(s)", sum(len(fs) for fs in file_sets), len(file_sets))
    return file_sets


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
        div, unit = 1, 'KB'
    return "{:d}{}".format(div, unit)


def list_files(file_sets, long_format=False):
    """
    List files in the given S3 bucket or from current directory.

    With the long format, shows content length and tallies up the total size in bytes.
    """
    total_length = 0
    for schema_name, file_group in groupby(file_sets, attrgetter("source_name")):
        print("Schema: '{}'".format(schema_name))
        for file_set in file_group:
            if file_set.manifest_file is None:
                print("    Table: '{}'".format(file_set.target_table_name.table))
            else:
                print("    Table: '{}' (with data from '{}')".format(file_set.target_table_name.table,
                                                                     file_set.source_table_name.identifier))
            for filename in file_set.files:
                if long_format:
                    content_length, last_modified = file_set.stat(filename)
                    total_length += content_length
                    print("        {} ({:d}, {})".format(file_set.uri(filename), content_length, last_modified))
                else:
                    print("        {}".format(file_set.uri(filename)))
    if total_length > 0:
        print("Total size in bytes: {:d} ({})".format(total_length, approx_pretty_size(total_length)))
