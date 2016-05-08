from collections import defaultdict
import concurrent.futures
import logging
import os
import os.path
import re
import subprocess
import threading

import boto3
import simplejson as json

from etl import TableName


# Split file names into schema name, old schema, table name, and file types:
TABLE_RE = re.compile(r"""/(?P<schema>\w+)
                          /(?P<old_schema>\w+)-(?P<table>\w+)
                          (?P<filetype>\.yaml|\.sql|\.csv(?:\.part_\d+)?(:?\.gz|\.manifest))$
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
    Upload local file to S3 bucket.

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
            logger.info("Dry-run: Skipping upload to 's3://%s/%s'", bucket_name, object_key)
        else:
            logger.info("Uploading '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
            bucket = _get_bucket(bucket_name)
            bucket.upload_file(filename, object_key)


def find_files(bucket_name, prefix, schemas, pattern):
    """
    Find all table design (.yaml), SQL (.sql) and data (.csv.gz) files in the
    given bucket and folder, return organized by table.
    """
    logging.getLogger(__name__).info("Looking for files in 's3://%s/%s'", bucket_name, prefix)
    bucket = _get_bucket(bucket_name)
    return find_files_from((obj.key for obj in bucket.objects.filter(Prefix=prefix)), schemas, pattern)


def find_local_files(directories, schemas, pattern):
    """
    Find all table design, SQL and data files starting from either local directory.
    """
    logging.getLogger(__name__).info("Looking for files in %s", directories)

    def list_files():
        for directory in directories:
            for root, dirs, files in os.walk(os.path.normpath(directory)):
                if len(dirs) == 0:  # bottom level
                    for filename in sorted(files):
                        yield os.path.join(root, filename)

    return find_files_from(list_files(), schemas, pattern)


def find_modified_files(schemas, pattern):
    """
    Find files that have been modified in your work tree (as identified by git status).
    For SQL files, the corresponding design file (YAML) is picked up even if the design
    has not been modified.
    """
    logger = logging.getLogger(__name__)
    logger.info("Looking for modified files in work tree")
    # The str() is needed to shut up PyCharm.
    status = str(subprocess.check_output(['git', 'status', '--porcelain'], universal_newlines=True))
    modified = {line[3:] for line in status.split('\n') if line.startswith(" M")}
    design_files = set()
    for name in modified:
        path, extension = os.path.splitext(name)
        if extension == ".sql":
            if os.path.exists(path + ".yaml"):
                design_files.add(path + ".yaml")
    logger.debug("Modified files in work tree: %s", modified)
    logger.debug("Adding design files as needed: %s", design_files.difference(modified))
    return find_files_from(modified.union(design_files), schemas, pattern)


def find_files_from(iterable, schemas, pattern):
    """
    Return a list of tuples (table name, dictionary of files) with dictionary
    containing {"Design": ..., "Data": ..., "SQL": ...} where
    - the value of "Design" is the name of table design file,
    - the value of "Data" is a (possibly empty) list of compressed CSV files
      or a list of a manifest file followed by CSV files,
    - the value of "SQL" is the name of a file with a DML which may be used
      within a DDL (see CTAS) or None if no .sql file was found.

    Tables must always have a table design file. It's not ok to have a CSV or
    SQL file by itself.
    """
    logger = logging.getLogger(__name__)
    source_index = dict((schema, i) for i, schema in enumerate(schemas))
    found = {}
    sql_files = {}
    data_files = defaultdict(list)
    for filename in iterable:
        match = TABLE_RE.search(filename)
        if match:
            values = match.groupdict()
            if values['schema'] in source_index:
                table_name = TableName(values['schema'], values['table'])
                # Select based on table name from commandline args
                if not pattern.match(table_name):
                    continue
                if values['filetype'] == '.yaml':
                    sort_key = (source_index[table_name.schema], values['old_schema'], table_name.table)
                    found[table_name] = {"Design": filename, "Data": [], "SQL": None, "_sort_key": sort_key}
                elif values['filetype'] == '.sql':
                    sql_files[table_name] = filename
                elif values['filetype'].endswith('.manifest'):
                    data_files[table_name][:0] = [filename]
                elif values['filetype'].startswith('.csv'):
                    data_files[table_name].append(filename)
    for table_name in sql_files:
        if table_name in found:
            found[table_name]["SQL"] = sql_files[table_name]
        else:
            logger.warning("Found SQL file without table design for '%s'", table_name.identifier)
    for table_name in data_files:
        if table_name in found:
            # If there are multiple partitions, make sure to skip original file.
            if any("csv.part_" in name for name in data_files[table_name]):
                found[table_name]["Data"] = [name for name in data_files[table_name] if not name.endswith(".csv.gz")]
            else:
                found[table_name]["Data"] = data_files[table_name]
        else:
            logger.warning("Found data file(s) without table design for '%s'", table_name.identifier)
    logger.debug("Found files for %d table(s)", len(found))
    # Turn dictionary into sorted list of tuples so that order of schemas (and sort of tables) is preserved.
    return sorted([(table_name, found[table_name]) for table_name in found], key=lambda t: found[t[0]]["_sort_key"])


def get_file_content(bucket_name, object_key):
    """
    Return stream for content of s3://bucket_name/object_key

    Close the stream when you're done with it.
    """
    logger = logging.getLogger(__name__)
    logger.info("Downloading 's3://%s/%s'", bucket_name, object_key)
    bucket = _get_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    response = s3_object.get()
    logger.debug("Received response from S3: last modified: %s, content length: %s, content type: %s",
                 response['LastModified'], response['ContentLength'], response['ContentType'])
    return response['Body']


def write_manifest_file(local_files, bucket_name, prefix, dry_run=False):
    """
    Create manifest file to load all the given files (after upload
    to S3) and return name of new manifest file. If there's only one local
    file, then skip writing a manifest and return None.
    """
    logger = logging.getLogger(__name__)
    parts = os.path.commonprefix(local_files)
    filename = parts[:parts.rfind(".part_")] + ".manifest"
    remote_files = ["s3://{}/{}/{}".format(bucket_name, prefix, os.path.basename(name)) for name in local_files]
    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}
    if dry_run:
        logger.info("Dry-run: Skipping writing new manifest file to '%s'", filename)
    else:
        logger.info("Writing new manifest file for %d file(s) to '%s'", len(local_files), filename)
        with open(filename, 'wt') as o:
            json.dump(manifest, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.debug("Done writing '%s'", filename)
    return filename
