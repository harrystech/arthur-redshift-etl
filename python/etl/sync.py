"""
Sync publishes the data warehouse code (and optionally configuration).

While we copy data into S3, we'll validate the table design files to minimize
chances of failing ETLs.  (It's still best to run the validate command!)

When the table design changes for a table extracted from an upstream data
source, it is best to also delete the data.

And if you change the configuration for your data warehouse (like upstream
source or additional schemas for transformations) you should "deploy" the
configuration file by copying it up to S3.
"""

import concurrent.futures
import logging
import os.path
from typing import List

import etl.config
import etl.file_sets
import etl.s3
from etl.errors import MissingQueryError
from etl.relation import RelationDescription
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def upload_settings(config_files, bucket_name, prefix, dry_run=False) -> None:
    """
    Upload warehouse configuration files (minus credentials) to target bucket/prefix's "config" dir.

    It is an error to try to upload files with the same name (coming from different config directories).
    """
    settings_files = etl.config.gather_setting_files(config_files)
    logger.info("Found %d settings file(s) to deploy", len(settings_files))

    uploader = etl.s3.S3Uploader(bucket_name, dry_run=dry_run)
    for fullname in settings_files:
        object_key = os.path.join(prefix, 'config', os.path.basename(fullname))
        uploader(fullname, object_key)


def sync_with_s3(relations: List[RelationDescription], bucket_name: str, prefix: str, dry_run: bool=False) -> None:
    """
    Copy (validated) table design and SQL files from local directory to S3 bucket.
    """
    logger.info("Validating %d table design(s) before upload", len(relations))
    RelationDescription.load_in_parallel(relations)

    files = []  # typing: List[Tuple[str, str]]
    for relation in relations:
        relation_files = [relation.design_file_name]
        if relation.is_transformation:
            if relation.sql_file_name:
                relation_files.append(relation.sql_file_name)
            else:
                raise MissingQueryError("Missing matching SQL file for '%s'" % relation.design_file_name)
        for file_name in relation_files:
            local_filename = relation.norm_path(file_name)
            remote_filename = os.path.join(prefix, local_filename)
            files.append((local_filename, remote_filename))

    uploader = etl.s3.S3Uploader(bucket_name, dry_run=dry_run)
    with Timer() as timer:
        futures = []  # typing: List[concurrent.futures.Future]
        # TODO With Python 3.6, we should pass in a thread_name_prefix
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            for local_filename, remote_filename in files:
                futures.append(executor.submit(uploader.__call__, local_filename, remote_filename))
        errors = 0
        for future in concurrent.futures.as_completed(futures):
            exception = future.exception()
            if exception is not None:
                logger.error("Failed to upload file: %s", exception)
                errors += 1
    if errors:
        logger.critical("There were %d error(s) during upload", errors)
    if not dry_run:
        logger.info("Uploaded %d of %d file(s) to 's3://%s/%s (%s)",
                    len(files) - errors, len(files), bucket_name, prefix, timer)
