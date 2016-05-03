#! /usr/bin/env python3

"""
Load data into DW tables from S3. Expects for every table a pair of files in S3
(table design and data).

The data needs to be in a single or in multiple CSV-formatted and compressed file(s).
"""

from contextlib import closing
import logging
import os.path

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def load_to_redshift(args, settings):
    dw = etl.env_value(settings("data_warehouse", "etl_access"))
    table_owner = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    bucket_name = settings("s3", "bucket_name")
    selection = etl.TableNamePatterns.from_list(args.table)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    files_in_s3 = etl.s3.find_files(bucket_name, args.prefix, schemas, selection)
    tables_with_data = [(table_name, table_files["Design"], table_files["Data"])
                        for table_name, table_files in files_in_s3
                        if len(table_files["Data"])]
    if len(tables_with_data) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket_name, args.prefix)
    else:
        # Need to read user's credentials for the COPY command
        credentials = etl.load.read_aws_credentials()

        with closing(etl.pg.connection(dw)) as conn:
            for table_name, design_file, csv_files in tables_with_data:
                with closing(etl.s3.get_file_content(bucket_name, design_file)) as content:
                    table_design = etl.load.load_table_design(content, table_name)
                if table_design["source_name"] in ("CTAS", "VIEW"):
                    continue
                with conn:
                    etl.load.create_table(conn, table_design, table_name, table_owner,
                                          drop_table=args.drop_table, dry_run=args.dry_run)
                    etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)
                    if csv_files[0].endswith(".manifest"):
                        csv_file = csv_files[0]
                    else:
                        # Extract basename for partitions in lieu of manifest file.
                        csv_file = os.path.commonprefix(csv_files)
                    location = "s3://{}/{}".format(bucket_name, csv_file)
                    etl.load.copy_data(conn, credentials, table_name, location, dry_run=args.dry_run)
                    etl.load.vacuum_analyze(conn, table_name, dry_run=args.dry_run)


def build_argument_parser():
    return etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-table", "table"], description=__doc__)


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        load_to_redshift(main_args, main_settings)
