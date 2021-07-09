"""
Sync publishes the data warehouse code and its configuration.

Example usage:
  arthur.py sync
  arthur.py sync dw

While we copy files into S3, we'll validate the table design files to minimize
chances of failing ETLs. It's still best to run the validate command first.

If you have removed files locally, you can cleanup the files in S3 using the
delete option:
  arthur.py sync --delete

When the table design changes for a table extracted from an upstream data
source, it is best to also delete the data.  This makes sure that new columns
or modified column types do not prevent data from loading:
  arthur.py sync --force

If you change the configuration for your data warehouse (like upstream
source or additional schemas for transformations) you need to "deploy" the
configuration files by copying them up to S3. This is done by default but
you can also skip:
  arthur.py sync --without-deploy-config
"""

import logging
import os.path
from typing import Iterable, List, Optional, Sequence, Tuple

import etl.config
import etl.file_sets
import etl.s3
import etl.timer
from etl.errors import MissingQueryError
from etl.names import TableSelector
from etl.relation import RelationDescription

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def upload_settings(config_paths: Iterable[str], bucket_name: str, prefix: str, dry_run=False) -> None:
    """
    Upload warehouse configuration files (minus credentials) to target bucket/prefix's "config" dir.

    It is an error to try to upload files with the same name (which would be coming from different
    config directories).
    """
    settings_files = etl.config.gather_setting_files(config_paths)
    logger.info(
        "Found %d settings file(s) to deploy to 's3://%s/%s/config'", len(settings_files), bucket_name, prefix
    )

    files = [(local_file, f"config/{os.path.basename(local_file)}") for local_file in settings_files]
    etl.s3.upload_files(files, bucket_name, prefix, dry_run=dry_run)


def sync_with_s3(
    relations: Sequence[RelationDescription],
    config_paths: Iterable[str],
    bucket_name: str,
    prefix: str,
    deploy_config: bool,
    delete_schemas_pattern: Optional[TableSelector],
    delete_data_pattern: Optional[TableSelector],
    dry_run=False,
) -> None:
    """
    Copy (validated) table design and SQL files from local directory to S3 bucket.

    To make sure that we don't upload "bad" files or leave files in S3 in an incomplete state,
    we first load to validate all table design files to be uploaded.
    """
    logger.info("Validating %d table design(s) before upload", len(relations))
    RelationDescription.load_in_parallel(relations)

    # Collect list of (local file, remote file) tuples to send to the uploader.
    files: List[Tuple[str, str]] = []
    for relation in relations:
        if relation.is_transformation:
            if relation.sql_file_name is None:
                raise MissingQueryError("missing matching SQL file for '%s'" % relation.design_file_name)
            relation_files = [relation.design_file_name, relation.sql_file_name]
        else:
            relation_files = [relation.design_file_name]

        for file_name in relation_files:
            filename = relation.norm_path(file_name)
            files.append((filename, filename))

    # OK. Things appear to have worked out so far. Now make actual uploads to S3.
    if deploy_config:
        upload_settings(config_paths, bucket_name, prefix, dry_run=dry_run)
    else:
        logger.info("As you wish: Configuration files are not changed in S3")

    if delete_data_pattern is not None:
        etl.file_sets.delete_data_files_in_s3(bucket_name, prefix, delete_data_pattern, dry_run=dry_run)

    if delete_schemas_pattern is not None:
        etl.file_sets.delete_schemas_files_in_s3(bucket_name, prefix, delete_schemas_pattern, dry_run=dry_run)

    etl.s3.upload_files(files, bucket_name, prefix, dry_run)
