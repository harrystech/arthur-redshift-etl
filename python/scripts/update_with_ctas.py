#! /usr/bin/env python3

"""
Load data using CTAS expressions into DW tables.

Expects for every table a SQL file in the S3 bucket with a valid expression to
create the content of the table (meaning: just the select without closing ';').
The actual DDL statement (CREATE TABLE AS ...) and the table attributes / constraints
are added from the matching table design file.
"""

import logging

import psycopg2

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def update_with_ctas(args, settings):
    dw = etl.env_value(settings("data-warehouse", "etl_user", "ENV"))
    user_group = settings("data-warehouse", "groups", "users")
    bucket = etl.s3.get_bucket(settings("s3", "bucket_name"))
    schemas = [source["name"] for source in settings("sources")]
    files = etl.s3.find_files(bucket, args.prefix, schemas=schemas, pattern=args.table)
    tables_with_data = [(table_name, table_files["Design"], table_files["SQL"])
                        for table_name, table_files in files
                        if table_files["SQL"] is not None]
    if len(tables_with_data) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket.name, args.prefix)
    else:
        with etl.pg.connection(dw) as conn:
            etl.pg.set_search_path(conn, schemas, debug=True)
            for table_name, design_file, sql_file in tables_with_data:
                try:
                    etl.load.create_table_as(conn, table_name, bucket, design_file, sql_file, dry_run=args.dry_run)
                    etl.load.grant_access(conn, table_name, user_group, dry_run=args.dry_run)
                    etl.load.vacuum_analyze(conn, table_name, dry_run=args.dry_run)
                except psycopg2.Error as exc:
                    if args.keep_going:
                        etl.pg.log_sql_error(exc, as_warning=True)
                        logging.critical("Error while evaluating CTAS--proceeding as instructed!")
                    else:
                        raise


def build_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "dry-run", "table"], description=__doc__)
    parser.add_argument("-k", "--keep-going", help="keep going even if tables fail to load (for testing)",
                        default=False, action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        update_with_ctas(main_args, main_settings)
