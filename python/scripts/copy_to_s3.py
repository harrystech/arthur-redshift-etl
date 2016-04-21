#! /usr/bin/env python3

"""
Upload local table design, SQL and (split) CSV files to S3 bucket.

This should be run after making changes to the table design files or the
SQL files describing CTAS or VIEWs in the local repo.  Or after splitting CSV
files. The table design files are validated even during a dry-run.
"""

import concurrent.futures
import logging
import os.path

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def copy_to_s3(args, settings):
    """
    Copy table design and SQL files from directory to S3 bucket.
    """
    bucket_name = settings("s3", "bucket_name")
    local_dirs = [args.table_design_dir, args.data_dir]
    selection = etl.TableNamePatterns.from_list(args.table)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    tables_with_files = etl.s3.find_local_files(local_dirs, schemas, selection)

    if len(tables_with_files) == 0:
        logging.error("No applicable files found %s", local_dirs)
    else:
        logging.info("Found files for %d tables.", len(tables_with_files))
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for table_name, files in tables_with_files:
                for file_type in ("Design", "SQL"):
                    local_filename = files[file_type]
                    if file_type == "Design":
                        # find_local_files will always return at least a design file, so local_filename != None
                        table_design = etl.load.load_table_design(open(local_filename, 'r'), table_name)
                        logging.debug("Validated table design for '%s'", table_design["name"])
                    if local_filename is not None:
                        remote_directory = os.path.basename(os.path.dirname(local_filename))
                        prefix = "{}/{}".format(args.prefix, remote_directory)
                        executor.submit(etl.s3.upload_to_s3, local_filename, bucket_name, prefix, dry_run=args.dry_run)
                if args.with_data:
                    for local_filename in files["Data"]:
                        remote_directory = os.path.basename(os.path.dirname(local_filename))
                        prefix = "{}/{}".format(args.prefix, remote_directory)
                        executor.submit(etl.s3.upload_to_s3, local_filename, bucket_name, prefix, dry_run=args.dry_run)
        if not args.dry_run:
            logging.info("Uploaded all files to 's3://%s/%s/'", bucket_name, args.prefix)


def build_argument_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "data-dir", "table-design-dir", "dry-run", "table"],
                                           description=__doc__)
    parser.add_argument("-w", "--with-data", help="Copy data files", action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.verbose)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        copy_to_s3(main_args, main_settings)
