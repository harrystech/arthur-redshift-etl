#! /usr/bin/env python3

"""
Create (or replace) CTASs and views in the data warehouse.

Expects for every view a SQL file in the S3 bucket with a valid expression to
create the view. The "source_name" in the table design must be set to "VIEW".

Expects for every derived table (CTAS) a SQL file in the S3 bucket with a valid
expression to create the content of the table (meaning: just the select without
closing ';'). The actual DDL statement (CREATE TABLE AS ...) and the table
attributes / constraints are added from the matching table design file.

Note that the table is actually created empty, then CTAS is used for a temporary
table which is then inserted into the table.  This is needed to attach
constraints, attributes, and encodings.
"""

from contextlib import closing
import logging

import etl
import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.s3


def update_ctas_or_views(args, settings):

    dw = etl.env_value(settings("data_warehouse", "etl_access"))
    table_owner = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    bucket_name = settings("s3", "bucket_name")
    selection = etl.TableNamePatterns.from_list(args.ctas_or_view)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    files_in_s3 = etl.s3.find_files(bucket_name, args.prefix, schemas, selection)
    tables_with_queries = [(table_name, table_files["Design"], table_files["SQL"])
                           for table_name, table_files in files_in_s3
                           if table_files["SQL"] is not None]
    if len(tables_with_queries) == 0:
        logging.error("No applicable files found in 's3://%s/%s'", bucket_name, args.prefix)
    else:
        with closing(etl.pg.connection(dw)) as conn:
            for table_name, design_file, sql_file in tables_with_queries:
                with closing(etl.s3.get_file_content(bucket_name, design_file)) as content:
                    table_design = etl.load.load_table_design(content, table_name)
                if table_design["source_name"] not in ("CTAS", "VIEW"):
                    continue
                with closing(etl.s3.get_file_content(bucket_name, sql_file)) as content:
                    query = content.read().decode()
                with conn:
                    if table_design["source_name"] == "CTAS" and not args.skip_ctas:
                        etl.load.create_table(conn, table_design, table_name, table_owner,
                                              drop_table=args.drop, dry_run=args.dry_run)
                        etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)
                        etl.load.create_temp_table_as_and_copy(conn, table_name, table_design, query,
                                                               add_explain_plan=args.add_explain_plan, dry_run=args.dry_run)
                        etl.load.analyze(conn, table_name, dry_run=args.dry_run)
                    elif table_design["source_name"] == "VIEW" and not args.skip_views:
                        etl.load.create_view(conn, table_design, table_name, table_owner, query,
                                             drop_view=args.drop, dry_run=args.dry_run)
                        etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)


def build_argument_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop", "ctas_or_view"], description=__doc__)
    parser.add_argument("-x", "--add-explain-plan", help="Add explain plan to log", action="store_true")
    parser.add_argument("-a", "--skip-views", help="Skip updating views, only reload CTAS", action="store_true")
    parser.add_argument("-k", "--skip-ctas", help="Skip updating CTAS, only reload views", action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        update_ctas_or_views(main_args, main_settings)
