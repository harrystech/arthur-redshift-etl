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
import concurrent.futures
import logging
from operator import attrgetter
import os
import os.path
import re
import subprocess
import threading

import boto3
import simplejson as json

import etl.commands
import etl.config
from etl import TableName, AssociatedTableFiles


# Split file names into new schema, old schema, table name, and file type
# XXX Must anchor RE in front against prefix otherwise might pick up wrong files
TABLE_RE = re.compile(r"""(?:^schemas|/schemas|^data|/data)
                          /(?P<source_name>\w+)
                          /(?P<schema_name>\w+)-(?P<table_name>\w+)
                          [\./](?P<file_type>yaml|sql|manifest|csv/part-\d+(:?\.gz)?)$
                      """, re.VERBOSE)

_resources_for_thread = threading.local()


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


def upload_to_s3(filename, bucket_name, prefix, dry_run=False):
    """
    Upload file to S3 bucket.

    Filename must be either name of file or a future that will return the name
    of a file. Exceptions from futures are propagated. If filename is None,
    then no upload will be attempted.
    """
    logger = logging.getLogger(__name__)
    if isinstance(filename, concurrent.futures.Future):
        try:
            filename = filename.result()
        except Exception:
            logger.exception("Something terrible happened in the future's past")
            raise
    if filename is not None:
        object_key = "{}/{}".format(prefix, os.path.basename(filename))
        if dry_run:
            logger.info("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
        else:
            try:
                logger.info("Uploading '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
                bucket = _get_bucket(bucket_name)
                bucket.upload_file(filename, object_key)
            except Exception:
                logger.exception('S3 upload error:')
                raise


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


def find_files_in_folder(bucket_name, prefix):
    """
    List all the files in "s3://{bucket_name}/{folder_name}" (where folder is probably a path)
    """
    logging.getLogger(__name__).info("Looking for files matching 's3://%s/%s*'", bucket_name, prefix)
    bucket = _get_bucket(bucket_name)
    return list(obj.key for obj in bucket.objects.filter(Prefix=prefix))


def find_files_in_bucket(bucket_name, prefix, schemas, pattern):
    """
    Discover files in the given bucket and folder by schema,
    apply pattern-based selection along the way.
    """
    logging.getLogger(__name__).info("Looking for files in 's3://%s/%s'", bucket_name, prefix)
    bucket = _get_bucket(bucket_name)
    return _find_files_from((obj.key for obj in bucket.objects.filter(Prefix=prefix)), schemas, pattern)


def find_local_files(directory, schemas, pattern):
    """
    Discover all local files from the given directory,
    apply pattern-based selection along the way.
    """
    logging.getLogger(__name__).info("Looking for files in %s", directory)

    def list_local_files():
        for root, dirs, files in os.walk(os.path.normpath(directory)):
            if len(dirs) == 0:  # bottom level
                for filename in sorted(files):
                    yield os.path.join(root, filename)

    return _find_files_from(list_local_files(), schemas, pattern)


def find_modified_files(schemas, pattern):
    """
    Find files that have been modified in your work tree (as identified by git status).

    For SQL files, the corresponding design file (.yaml) is picked up even if the design
    itself has not been modified.
    """
    logger = logging.getLogger(__name__)
    logger.info("Looking for modified files in work tree")
    # The str() is needed to shut up PyCharm.
    status = str(subprocess.check_output(['git', 'status', '--porcelain'], universal_newlines=True))
    modified_files = frozenset(line[3:] for line in status.split('\n') if line.startswith(" M"))
    combined_files = set(modified_files)
    for name in modified_files:
        path, extension = os.path.splitext(name)
        if extension == ".sql":
            design_file = path + ".yaml"
            if os.path.exists(design_file):
                combined_files.add(design_file)
    logger.debug("Found modified files in work tree: %s", sorted(modified_files))
    logger.debug("Added design files although not new: %s", sorted(combined_files.difference(modified_files)))
    return _find_files_from(sorted(combined_files), schemas, pattern)


def _find_files_from(iterable, schemas, pattern):
    """
    Return (ordered) dictionary that maps schemas to lists of table meta data ('associated table files').

    Note that all tables must have a table design file. It's not ok to have a CSV or
    SQL file by itself.

    The associate file information is sorted by source schema and table.
    """
    logger = logging.getLogger(__name__)
    found = defaultdict(dict)
    maybe = []
    # First pass -- pick up all the design files, keep matches around for second pass
    for filename in iterable:
        match = TABLE_RE.search(filename)
        if match:
            values = match.groupdict()
            source_name = values['source_name']
            if source_name in schemas:
                source_table_name = TableName(values['schema_name'], values['table_name'])
                target_table_name = TableName(source_name, values['table_name'])
                # Select based on table name from commandline args
                if not pattern.match(target_table_name):
                    continue
                if values['file_type'] == 'yaml':
                    found[source_name][target_table_name] = AssociatedTableFiles(source_table_name,
                                                                                 target_table_name,
                                                                                 filename)
                else:
                    maybe.append((filename, source_name, target_table_name, values['file_type']))
    # Second pass -- only store SQL and data files for tables that have design files from first pass
    for filename, source_name, target_table_name, file_type in maybe:
        assoc_table = found[source_name].get(target_table_name)
        if file_type == 'sql':
            if assoc_table:
                assoc_table.set_sql_file(filename)
            else:
                logger.warning("Found SQL file without table design: '%s'", filename)
        elif file_type == 'manifest':
            if assoc_table:
                # Record the manifest here but note that we always create a new manifest anyways.
                assoc_table.set_manifest_file(filename)
            else:
                logger.warning("Found manifest file without table design: '%s'", filename)
        elif file_type.startswith('csv'):
            if assoc_table:
                assoc_table.add_data_file(filename)
            else:
                logger.warning("Found data file without table design: '%s'", filename)
    # XXX Refactor so that the len (over assoc table files) depends on what file types should actually be counted
    logger.info("Found %d matching file(s) for %d schema(s) with %d table(s) total",
                sum(len(table) for tables in found.values() for table in tables.values()),
                len(found),
                sum(len(tables) for tables in found.values()))
    # Always return files sorted by source table name (which includes the schema in the source).
    ordered_found = OrderedDict()
    for source_name in schemas:
        if source_name in found:
            ordered_found[source_name] = sorted(found[source_name].values(), key=attrgetter('source_table_name'))
    return ordered_found


def list_files(settings, prefix, table):
    """
    List files in the S3 bucket.

    Useful to discover whether pattern matching works.
    """
    bucket_name = settings("s3", "bucket_name")
    selection = etl.TableNamePatterns.from_list(table)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    found = find_files_in_bucket(bucket_name, prefix, schemas, selection)
    for source_name in found:
        print("Source: {}".format(source_name))
        for info in found[source_name]:
            if info.source_table_name.schema in ('CTAS', 'VIEW'):
                print("    Table: {} ({})".format(info.target_table_name.table, info.source_table_name.schema))
            else:
                print("    Table: {} (from: {})".format(info.target_table_name.table,
                                                       info.source_table_name.identifier))
            files = [("Design", info.design_file)]
            if info.sql_file is not None:
                files.append(("SQL", info.sql_file))
            if info.manifest_file is not None:
                files.append(("Manifest", info.manifest_file))
            if len(info.data_files) > 0:
                files.extend(("Data", filename) for filename in info.data_files)
            for file_type, filename in files:
                print("        {}: s3://{}/{}".format(file_type, bucket_name, filename))
