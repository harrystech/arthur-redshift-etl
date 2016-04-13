#! /usr/bin/env python3

"""
Load data using CTAS expressions into data warehouse tables.

Expects for every table a SQL file in the S3 bucket with a valid expression to
create the content of the table (meaning: just the select without closing ';').
The actual DDL statement (CREATE TABLE AS ...) and the table attributes / constraints
are added from the matching table design file.

Note that the table is actually created empty, then CTAS is used for a temporary
table which is then inserted into the table.  This is needed to attach constraints,
attributes, and encodings.
"""

import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.update


def update_with_ctas(args, settings):
    etl.update.update_table_or_view(args, settings, "CTAS", _update_sequence)


def _update_sequence(conn, table_design, table_name, table_owner, etl_group, user_group, query, args):
    etl.load.create_table(conn, table_design, table_name, table_owner,
                          drop_table=args.drop_table, dry_run=args.dry_run)
    etl.load.grant_access(conn, table_name, etl_group, user_group, dry_run=args.dry_run)
    etl.load.create_temp_table_as_and_copy(conn, table_name, table_design, query,
                                           add_explain_plan=args.add_explain_plan, dry_run=args.dry_run)
    etl.load.analyze(conn, table_name, dry_run=args.dry_run)


def build_argument_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-table", "table"], description=__doc__)
    parser.add_argument("-x", "--add-explain-plan", help="Add explain plan to log", action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.verbose)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        update_with_ctas(main_args, main_settings)
