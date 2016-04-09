#! /usr/bin/env python3

"""
Load data into DW tables from S3. Expects for every table a pair of files in S3
(table design and data).

The data needs to be in a single or in multiple CSV formatted and compressed file(s).
"""

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
    bucket = etl.s3.get_bucket(settings("s3", "bucket_name"))
    schemas = [source["name"] for source in settings("sources")]
    files = etl.s3.find_files(bucket, args.prefix, schemas=schemas, pattern=args.table)

    tables_with_data = [(table_name, table_files["Design"], table_files["Data"])
                        for table_name, table_files in files
                        if len(table_files["Data"])]
    # Need to read user's credentials for the COPY command
    credentials = etl.load.read_aws_credentials()

    if len(tables_with_data) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket.name, args.prefix)
    else:
        conn = etl.pg.connection(dw)
        for table_name, design_file, csv_files in tables_with_data:
            with conn:
                etl.load.create_table(conn, table_name, table_owner, bucket, design_file,
                                      drop_table=args.drop_table, dry_run=args.dry_run)
                etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)
                # TODO Use manifest file with multiple CSV files
                csv_file = os.path.commonprefix(csv_files)
                etl.load.copy_data(conn, table_name, bucket, csv_file, credentials, dry_run=args.dry_run)
                etl.load.analyze(conn, table_name, dry_run=args.dry_run)
        conn.close()


def build_parser():
    return etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-table", "table"], description=__doc__)


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        load_to_redshift(main_args, main_settings)
