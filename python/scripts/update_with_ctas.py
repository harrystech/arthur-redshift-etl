#! /usr/bin/env python3

"""
Load data using CTAS expressions into DW tables.

Expects for every table a SQL file in the S3 bucket with a valid expression to
create the content of the table (meaning: just the select without closing ';').
The actual DDL statement (CREATE TABLE AS ...) and the table attributes / constraints
are added from the matching table design file.

Note that the table is actually created empty, then CTAS is used for a temporary
table which is then inserted into the table.  This is needed to attach constraints,
attributes, and encodings.
"""

import logging

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def update_with_ctas(args, settings):
    dw = etl.env_value(settings("data_warehouse", "etl_access"))
    table_owner = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    bucket = etl.s3.get_bucket(settings("s3", "bucket_name"))
    schemas = [source["name"] for source in settings("sources")]
    files = etl.s3.find_files(bucket, args.prefix, schemas=schemas, pattern=args.table)

    tables_with_data = [(table_name, table_files["Design"], table_files["SQL"])
                        for table_name, table_files in files
                        if table_files["SQL"] is not None]
    if len(tables_with_data) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket.name, args.prefix)
    else:
        conn = etl.pg.connection(dw)
        # etl.pg.set_search_path(conn, schemas)
        for table_name, design_file, sql_file in tables_with_data:
            with conn:
                etl.load.create_table(conn, table_name, table_owner, bucket, design_file,
                                      drop_table=args.drop_table, dry_run=args.dry_run)
                etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)
                etl.load.create_temp_table_as_and_copy(conn, table_name, bucket, design_file, sql_file,
                                                       dry_run=args.dry_run)
                etl.load.analyze(conn, table_name, dry_run=args.dry_run)
        conn.close()


def build_parser():
    return etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-table", "table"], description=__doc__)


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        update_with_ctas(main_args, main_settings)
