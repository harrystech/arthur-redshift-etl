#! /usr/bin/env python3

"""
Upload local table design and SQL files to S3 bucket.

This should be run after making changes to the table design files or the
(CTAS) SQL files in the local repo.
"""

import concurrent.futures
import logging
import os.path

import etl
import etl.arguments
import etl.config
import etl.s3


def copy_to_s3(args, settings):
    """
    Copy table design and SQL files from directory to S3 bucket.
    """
    # TODO Think about uploading CSV files as well.
    schemas = [source["name"] for source in settings("sources") if source.get("CTAS", False)]
    bucket_name = settings("s3", "bucket_name")
    tables_with_files = etl.s3.find_local_files(args.table_design_dir, schemas=schemas, pattern=args.table)
    if len(tables_with_files) == 0:
        logging.error("No applicable files found in directory '%s'", args.table_design_dir)
    else:
        # Three threads sounds about right with no real numbers behind it.
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for table_name, files in tables_with_files:
                for filetype in ("Design", "SQL"):
                    # TODO Validate table design files before upload?
                    local_filename = files[filetype]
                    if local_filename is not None:
                        remote_directory = os.path.basename(os.path.dirname(local_filename))
                        prefix = "{}/{}".format(args.prefix, remote_directory)
                        executor.submit(etl.s3.upload_to_s3,
                                        local_filename, bucket_name, prefix, dry_run=args.dry_run)
        if not args.dry_run:
            logging.info("Uploaded %d files to 's3://%s/%s", len(tables_with_files), bucket_name, args.prefix)


def build_parser():
    return etl.arguments.argument_parser(["config", "prefix", "table-design-dir", "dry-run", "table"],
                                         description=__doc__)


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    copy_to_s3(main_args, main_settings)
