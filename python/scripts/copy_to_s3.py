#! /usr/bin/env python3

"""
Upload local table design, SQL and (split) CSV files to S3 bucket.

This should be run after making changes to the table design files or the
(CTAS) SQL files in the local repo.  Or splitting CSV files.
The table design files are validated even during a dry-run.
"""

import concurrent.futures
import logging
import os.path

import etl
import etl.arguments
import etl.config
import etl.load
import etl.s3


def copy_to_s3(args, settings):
    """
    Copy table design and SQL files from directory to S3 bucket.
    """
    schemas = [source["name"] for source in settings("sources")]
    bucket_name = settings("s3", "bucket_name")
    tables_with_files = etl.s3.find_local_files(args.table_design_dir, args.data_dir,
                                                schemas=schemas, pattern=args.table)
    if len(tables_with_files) == 0:
        logging.error("No applicable files found in directory '%s'", args.table_design_dir)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for table_name, files in tables_with_files:
                for file_type in ("Design", "SQL", "Data"):
                    if file_type == "Data":
                        if args.with_data:
                            for local_filename in files[file_type]:
                                remote_directory = os.path.basename(os.path.dirname(local_filename))
                                prefix = "{}/{}".format(args.prefix, remote_directory)
                                executor.submit(etl.s3.upload_to_s3,
                                                local_filename, bucket_name, prefix, dry_run=args.dry_run)
                    else:
                        local_filename = files[file_type]
                        if local_filename is not None:
                            if file_type == "Design":
                                table_design = etl.load.load_table_design(open(local_filename, 'r'), table_name)
                                logging.debug("Validated table design %s", table_design["name"])
                            remote_directory = os.path.basename(os.path.dirname(local_filename))
                            prefix = "{}/{}".format(args.prefix, remote_directory)
                            executor.submit(etl.s3.upload_to_s3,
                                            local_filename, bucket_name, prefix, dry_run=args.dry_run)
        if not args.dry_run:
            logging.info("Uploaded %d files to 's3://%s/%s", len(tables_with_files), bucket_name, args.prefix)


def build_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "data-dir", "table-design-dir", "dry-run", "table"],
                                           description=__doc__)
    parser.add_argument("-w", "--with-data", help="Copy data files", action="store_true")
    return parser


if __name__ == "__main__":
    import etl.pg
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        copy_to_s3(main_args, main_settings)
