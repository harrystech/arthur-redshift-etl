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
from typing import List, Sequence, Tuple

from tqdm import tqdm

import etl.config
import etl.file_sets
import etl.s3
import etl.timer
from etl.errors import ETLRuntimeError, MissingQueryError
from etl.relation import RelationDescription

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def upload_settings(config_files, bucket_name, prefix, dry_run=False) -> None:
    """
    Upload warehouse configuration files (minus credentials) to target bucket/prefix's "config" dir.

    It is an error to try to upload files with the same name (coming from different config
    directories).
    """
    settings_files = etl.config.gather_setting_files(config_files)
    logger.info("Found %d settings file(s) to deploy", len(settings_files))

    uploader = etl.s3.S3Uploader(bucket_name, dry_run=dry_run)
    for fullname in settings_files:
        object_key = os.path.join(prefix, "config", os.path.basename(fullname))
        uploader(fullname, object_key)


def sync_with_s3(
    relations: Sequence[RelationDescription], bucket_name: str, prefix: str, dry_run: bool = False
) -> None:
    """Copy (validated) table design and SQL files from local directory to S3 bucket."""
    logger.info("Validating %d table design(s) before upload", len(relations))
    RelationDescription.load_in_parallel(relations)

    # Collect list of (local file, remote file) pairs to send to the uploader.
    files: List[Tuple[str, str]] = []
    for relation in relations:
        if relation.is_transformation:
            if relation.sql_file_name is None:
                raise MissingQueryError("missing matching SQL file for '%s'" % relation.design_file_name)
            relation_files = [relation.design_file_name, relation.sql_file_name]
        else:
            relation_files = [relation.design_file_name]

        for file_name in relation_files:
            local_filename = relation.norm_path(file_name)
            remote_filename = os.path.join(prefix, local_filename)
            files.append((local_filename, remote_filename))

    timer = etl.timer.Timer()
    tqdm_bar = tqdm(desc="Uploading files to S3", disable=None, leave=False, total=len(files), unit="file")

    uploader = etl.s3.S3Uploader(bucket_name, callback=tqdm_bar.update, dry_run=dry_run)
    max_workers = 8

    # We break out the futures to be able to easily tally up errors.
    futures: List[concurrent.futures.Future] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="sync-parallel") as executor:
        for local_filename, remote_filename in files:
            futures.append(executor.submit(uploader.__call__, local_filename, remote_filename))

    errors = 0
    for future in concurrent.futures.as_completed(futures):
        exception = future.exception()
        if exception is not None:
            logger.error("Failed to upload file: %s", exception)
            errors += 1

    tqdm_bar.close()
    if dry_run:
        logger.info("Dry-run: Skipped uploading %d file(s) to 's3://%s/%s (%s)", len(files), bucket_name, prefix, timer)
    else:
        logger.info(
            "Uploaded %d of %d file(s) to 's3://%s/%s' using %d threads (%s)",
            len(files) - errors,
            len(files),
            bucket_name,
            prefix,
            max_workers,
            timer,
        )
    if errors:
        raise ETLRuntimeError("There were {:d} error(s) during upload".format(errors))
