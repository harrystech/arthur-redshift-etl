#! /usr/bin/env python3

"""
Load data into DW tables from S3. Expects for every table a pair of files in S3
(table design and data).

The data needs to be in a single or in multiple CSV formatted and compressed file(s).
"""

import logging

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def load_to_redshift(args, settings):
    dw = etl.env_value(settings("data-warehouse", "etl_user", "ENV"))
    user_group = settings("data-warehouse", "groups", "users")
    bucket = etl.s3.get_bucket(settings("s3", "bucket_name"))
    schemas = [source["name"] for source in settings("sources")]
    files = etl.s3.find_files(bucket, args.prefix, schemas=schemas, pattern=args.table)
    tables_with_data = [(table_name, table_files["Design"], table_files["CSV"])
                        for table_name, table_files in files
                        if table_files["CSV"] is not None]
    # Need to read user's credentials for the COPY command
    credentials = etl.load.read_aws_credentials()

    if len(tables_with_data) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket.name, args.prefix)
    else:
        with etl.pg.connection(dw) as conn:
            for table_name, design_file, csv_file in tables_with_data:
                etl.load.create_table(conn, table_name, bucket, design_file,
                                      drop_table=args.drop_table, dry_run=args.dry_run)
                etl.load.copy_data(conn, table_name, bucket, csv_file, credentials, dry_run=args.dry_run)
                etl.load.grant_access(conn, table_name, user_group, dry_run=args.dry_run)
                etl.load.vacuum_analyze(conn, table_name, dry_run=args.dry_run)


def build_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-table", "table"], description=__doc__)
    parser.add_argument("-d", "--drop-table",
                        help="DROP TABLE first TO FORCE UPDATE OF TABLE definition (INSTEAD OF truncating the TABLE)",
                        default=False, action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        load_to_redshift(main_args, main_settings)
