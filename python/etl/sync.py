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

import logging
from typing import List
import os.path

import etl.config
from etl.errors import MissingQueryError
import etl.file_sets
from etl.names import TableSelector
from etl.relation import RelationDescription
import etl.s3

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def upload_settings(config_files, bucket_name, prefix, dry_run=False):
    """
    Upload warehouse configuration files (*.yaml) to target bucket/prefix's "config" dir

    This will not try to upload the default file since that comes along within the package's deployment.

    It is an error to try to upload files with the same name (coming from different config directories).
    """
    settings_files = etl.config.gather_setting_files(config_files)
    logger.info("Found %d settings file(s) to deploy", len(settings_files))

    uploader = etl.s3.S3Uploader(bucket_name, dry_run=dry_run)
    for fullname in settings_files:
        object_key = os.path.join(prefix, 'config', os.path.basename(fullname))
        uploader(fullname, object_key)


def sync_with_s3(config_files: List[str], descriptions: List[RelationDescription], bucket_name: str, prefix: str,
                 pattern: TableSelector, force: bool, deploy_config: bool, dry_run: bool=False) -> None:
    """
    Copy (validated) table design and SQL files from local directory to S3 bucket.
    """
    if force:
        etl.file_sets.delete_files_in_bucket(bucket_name, prefix, pattern, dry_run=dry_run)

    if deploy_config:
        upload_settings(config_files, bucket_name, prefix, dry_run=dry_run)

    uploader = etl.s3.S3Uploader(bucket_name, dry_run=dry_run)
    for description in descriptions:
        files = [description.design_file_name]
        if description.is_ctas_relation or description.is_view_relation:
            if description.sql_file_name:
                files.append(description.sql_file_name)
            else:
                raise MissingQueryError("Missing matching SQL file for '%s'" % description.design_file_name)

        for local_filename in files:
            object_key = os.path.join(prefix, description.norm_path(local_filename))
            uploader(local_filename, object_key)
    if not dry_run:
        logger.info("Synced %d file(s) to 's3://%s/%s/'", len(descriptions), bucket_name, prefix)
