"""
Utilities to interact with Amazon S3 (and occasionally, the local file system).

The layout of files is something like this for CSV files:

s3://{bucket_name}/{prefix}/data/{source_name}/{schema_name}-{table_name}/csv/part_0000.gz

Where bucket_name and prefix should be obvious. The source_name refers back to the
name of the source in the configuration file. The schema_name is the original schema, meaning
the name of the schema in the source database. The table_name is, eh, the table name.
If the data is written out in multiple files, then there will be part_0001.gz, part_0002.gz etc.

The location of the manifest file pointing to CSV files is

s3://{bucket_name}/{prefix}/data/{source_name}/{schema_name}-{table_name}.manifest

The table design files reside in a separate folder:

s3://{bucket_name}/{prefix}/schemas/{source_name}/{schema_name}-{table_name}.yaml

If there are SQL files for CTAS or views, they need to be here:

s3://{bucket_name}/{prefix}/schemas/{source_name}/{schema_name}-{table_name}.sql

Note that for tables or views which are not from upstream sources but are instead
built using SQL, there's a free choice of the schema_name. So this is best used
to create a sequence, meaning evaluation order in the ETL.
"""

from collections import defaultdict, OrderedDict
import logging
from operator import attrgetter
import os
import os.path
import re
import threading

import boto3
import botocore.exceptions

import etl.commands
import etl.config
from etl import TableName, AssociatedTableFiles


_resources_for_thread = threading.local()


class BadSourceDefinitionError(etl.ETLException):
    pass


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


def upload_to_s3(filename, bucket_name, prefix=None, object_key=None, dry_run=False):
    """
    Upload a file to S3 bucket.  If the object key is provided, then the
    file's content are uploaded for that object.  If a prefix is provided
    instead, then the object key is the prefix plus the basename of the
    local filename.
    """
    if object_key is None and prefix is None:
        raise ValueError("One of object_key or prefix must be specified")
    logger = logging.getLogger(__name__)
    if object_key is None:
        object_key = "{}/{}".format(prefix, os.path.basename(filename))
    if dry_run:
        logger.info("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
    else:
        logger.info("Uploading '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
        bucket = _get_bucket(bucket_name)
        bucket.upload_file(filename, object_key)


def delete_in_s3(bucket_name: str, object_keys, dry_run: bool=False):
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
        chunk_size = 1000
        while len(keys) > 0:
            result = bucket.delete_objects(Delete={'Objects': keys[:chunk_size]})
            del keys[:chunk_size]
            for deleted in result.get('Deleted', {}):
                logger.info("Deleted 's3://%s/%s'", bucket_name, deleted['Key'])
            for error in result.get('Errors', {}):
                logger.warning("Failed to delete 's3://%s/%s': %s %s", bucket_name, error['Key'],
                               error['Code'], error['Message'])


def get_last_modified(bucket_name, object_key):
    """
    Return last_modified timestamp from the object.  Returns None if object does not exist.
    """
    logger = logging.getLogger(__name__)
    bucket = _get_bucket(bucket_name)
    try:
        s3_object = bucket.Object(object_key)
        timestamp = s3_object.last_modified
        logger.debug("Object in 's3://%s/%s' was last modified %s", bucket_name, object_key, timestamp)
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


def get_file_content(bucket_name, object_key):
    """
    Return stream for content of s3://bucket_name/object_key

    You must close the stream when you're done with it.
    """
    logger = logging.getLogger(__name__)
    logger.info("Downloading 's3://%s/%s'", bucket_name, object_key)
    bucket = _get_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    response = s3_object.get()
    logger.debug("Received response from S3: last modified: %s, content length: %s, content type: %s",
                 response['LastModified'], response['ContentLength'], response['ContentType'])
    return response['Body']


def list_files_in_folder(bucket_name, prefix):
    """
    List all the files in "s3://{bucket_name}/{prefix}" (where prefix is probably a path and not an object)
    or if prefix is a tuple, then list files in "s3://{bucket_name}/{common path} which start with one of
    the elements in the prefix tuple.
    """
    logging.getLogger(__name__).debug("Looking for files at 's3://%s/%s'", bucket_name, prefix)
    bucket = _get_bucket(bucket_name)
    if isinstance(prefix, tuple):
        return [obj.key for obj in bucket.objects.filter(Prefix=os.path.commonprefix(prefix))
                if obj.key.startswith(prefix)]
    elif isinstance(prefix, str):
        return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
    else:
        raise ValueError("prefix must be string or tuple (of strings)")


def find_files_for_schemas(bucket_name, prefix, schemas, pattern):
    """
    Discover design and data files in the given bucket and folder, organize by schema,
    and apply pattern-based selection along the way.

    Only files in 'data' or 'schemas' sub-folders are examined (so that ignores 'config' or 'jars').
    """
    return _find_files_from(list_files_in_folder(bucket_name, (prefix + '/data', prefix + '/schemas')),
                            schemas, pattern)


def list_local_files(directory):
    """
    List all files in and anywhere below this directory.

    Ignore swap files along the way.
    """
    logging.getLogger(__name__).info("Looking for files in '%s'", directory)
    for root, dirs, files in os.walk(os.path.normpath(directory)):
        for filename in sorted(files):
            if not filename.endswith(('.swp', '~', '.DS_Store')):
                yield os.path.join(root, filename)


def find_local_files(directory, schemas, pattern):
    """
    Discover local design and data files starting from the given directory, organize by schema,
    and apply pattern-based selection along the way.

    The directory should be the 'schemas' directory, probably.
    """
    return _find_files_from(list_local_files(directory), schemas, pattern)


def _find_matching_files_from(iterable, schemas, pattern, return_extra_files=False):
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
                                      |.*(?P<suffix>_SUCCESS|_\$folder\$))$
                               """, re.VERBOSE)

    for filename in iterable:
        match = file_names_re.search(filename)
        if match:
            values = match.groupdict()
            source_name = values['source_name']
            if source_name in schemas:
                target_table_name = TableName(source_name, values['table_name'])
                if pattern.match(target_table_name):
                    if values['suffix']:
                        if not return_extra_files:
                            continue
                    elif values["file_ext"].startswith("/csv"):
                        values["file_type"] = "data"
                    else:
                        values["file_type"] = values["file_ext"][1:]
                    yield (filename, values)
        elif not filename.endswith("_$folder$"):
            logging.getLogger(__name__).warning("Found file not matching expected format: '%s'", filename)


def delete_files_in_bucket(bucket_name, prefix, schemas, selection, dry_run=False):
    """
    Delete all files that might be relevant given the choice of schemas and the target selection.
    """
    iterable = list_files_in_folder(bucket_name, (prefix + '/data', prefix + '/schemas'))
    deletable = [filename for filename, values in _find_matching_files_from(iterable, schemas, selection,
                                                                            return_extra_files=True)]
    delete_in_s3(bucket_name, deletable, dry_run=dry_run)


def _attach_table_files(known_files: AssociatedTableFiles, extra_files):
    """
    Helper function to safely (and verbosely) attach SQL and data files to design files.
    It is ok for `known_files` to be None.
    """
    logger = logging.getLogger(__name__)
    for filename, file_type in extra_files:
        if file_type == 'sql':
            if known_files:
                known_files.set_sql_file(filename)
            else:
                logger.warning("Found SQL file without table design: '%s'", filename)
        elif file_type == 'manifest':
            if known_files:
                known_files.set_manifest_file(filename)
            else:
                logger.warning("Found manifest file without table design: '%s'", filename)
        elif file_type == 'data':
            if known_files:
                known_files.add_data_file(filename)
            else:
                logger.warning("Found data file without table design: '%s'", filename)


def _find_files_from(iterable, schemas, pattern):
    """
    Return (ordered) dictionary that maps schemas to lists of table meta data ('associated table files').

    Note that all tables must have a table design file. It's not ok to have a CSV or
    SQL file by itself.

    The associated file information is sorted by source schema (same order as in config file)
    and table (alphabetically based on fully qualified name in the source, meaning "{schema}.{table}").
    """
    logger = logging.getLogger(__name__)
    tables_by_source = defaultdict(dict)
    additional_files = defaultdict(list)
    # First pass -- pick up all the design files, keep matches around for second pass
    errors = 0
    for filename, values in _find_matching_files_from(iterable, schemas, pattern):
        source_name = values['source_name']
        source_table_name = TableName(values['schema_name'], values['table_name'])
        target_table_name = TableName(source_name, values['table_name'])
        if values['file_type'] == 'yaml':
            if target_table_name in tables_by_source[source_name]:
                logger.error("Target table '%s' has more than one upstream source: %s",
                             target_table_name.identifier,
                             [os.path.basename(filename)
                              for filename in [tables_by_source[source_name][target_table_name].design_file, filename]])
                errors += 1
            else:
                tables_by_source[source_name][target_table_name] = AssociatedTableFiles(source_table_name,
                                                                                        target_table_name,
                                                                                        filename)
        else:
            additional_files[(source_name, target_table_name)].append((filename, values["file_type"]))
    if errors:
        raise BadSourceDefinitionError("Target table has multiple source tables")
    # Second pass -- only store SQL and data files for tables that have design files from first pass
    for source_name, target_table_name in additional_files:
        assoc_table = tables_by_source[source_name].get(target_table_name)
        _attach_table_files(assoc_table, additional_files[(source_name, target_table_name)])
    # Always return files sorted by source table name (which includes the schema in the source).
    ordered_found = OrderedDict()
    for source_name in schemas:
        if source_name in tables_by_source:
            ordered_found[source_name] = sorted(tables_by_source[source_name].values(),
                                                key=attrgetter('source_table_name'))
    logger.info("Found %d matching file(s) for %d schema(s) with %d table(s) total",
                sum(len(table) for tables in ordered_found.values() for table in tables), len(ordered_found),
                sum(len(tables) for tables in ordered_found.values()))
    return ordered_found


def list_files(settings, prefix, table, long_format=False):
    """
    List files in the S3 bucket.

    Useful to discover whether pattern matching works.
    """
    logger = logging.getLogger(__name__)
    selection = etl.TableNamePatterns.from_list(table)
    combined_schemas = settings("sources") + settings("data_warehouse", "schemas")
    schemas = selection.match_field(combined_schemas, "name")
    schema_names = [s["name"] for s in schemas]
    bucket_name = settings("s3", "bucket_name")
    found = find_files_for_schemas(bucket_name, prefix, schema_names, selection)
    if not found:
        logger.error("No applicable files found in 's3://%s/%s' for '%s'", bucket_name, prefix, selection)
        return

    total_length = 0
    for source_name in found:
        print("Source: {}".format(source_name))
        for info in found[source_name]:
            if info.manifest_file is not None:
                print("    Table: {} (with data from {})".format(info.target_table_name.table,
                                                                 info.source_table_name.identifier))
            else:
                print("    Table: {}".format(info.target_table_name.table))
            files = [("Design", info.design_file)]
            if info.sql_file is not None:
                files.append(("SQL", info.sql_file))
            if info.manifest_file is not None:
                files.append(("Manifest", info.manifest_file))
            if len(info.data_files) > 0:
                files.extend(("Data", filename) for filename in info.data_files)
            for file_type, filename in files:
                if long_format:
                    content_length, last_modified = object_stat(bucket_name, filename)
                    total_length += content_length
                    print("        {}: s3://{}/{} ({:d}, {})".format(file_type, bucket_name, filename,
                                                                     content_length, last_modified))
                else:
                    print("        {}: s3://{}/{}".format(file_type, bucket_name, filename))
    if total_length != 0:
        print("Total size in bytes: {:d}".format(total_length))
