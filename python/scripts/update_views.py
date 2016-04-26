#! /usr/bin/env python3

"""
Create (or replace) views in data warehouse.

Expects for every view a SQL file in the S3 bucket with a valid expression to
create the view. The "source_name" in the table design must be set to "VIEW".
"""

import etl.arguments
import etl.config
import etl.load
import etl.pg
import etl.update


def update_views(args, settings):
    etl.update.update_table_or_view(args, settings, "VIEW", _update_sequence)


def _update_sequence(conn, table_design, view_name, table_owner, etl_group, user_group, query, args):
    etl.load.create_view(conn, table_design, view_name, table_owner, query,
                         drop_view=args.drop_view, dry_run=args.dry_run)
    etl.load.grant_access(conn, view_name, etl_group, user_group, dry_run=args.dry_run)


def build_argument_parser():
    return etl.arguments.argument_parser(["config", "prefix", "dry-run", "drop-view", "view"], description=__doc__)


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        update_views(main_args, main_settings)
