#! /usr/bin/env python3

"""Determine how many rows were updated in the last month."""

import logging

import etl
import etl.arguments
import etl.config
import etl.pg


def find_tables(conn, schemas):
    """
    Find all tables (with their schemas) which have an 'updated_at' attribute.
    """
    # TODO Test for 'created_at' as well
    tables = etl.pg.query(conn, """SELECT ns.nspname as "schemaname", cls.relname as "tablename"
                                     FROM pg_catalog.pg_class cls
                                     JOIN pg_catalog.pg_namespace ns on cls.relnamespace = ns.oid
                                     JOIN pg_catalog.pg_attribute att on cls.oid = att.attrelid
                                    WHERE ns.nspname in %s
                                      AND att.attname = 'updated_at'
                                      AND NOT att.attisdropped
                                    ORDER BY ns.nspname, cls.relname""", (tuple(schemas),))
    logging.info("Found %d table(s) in %d schema(s)", len(tables), len(schemas))
    return [etl.TableName(schema, table) for schema, table in tables]


def count_updated_rows(conn, tables, begin_time, end_time, check_modified=False):
    """"
    Count how many rows have an updated_at timestamp in the given interval
    """
    total = 0
    for table_name in tables:
        stmt = """SELECT count(*) as count
                   FROM {}
                  WHERE updated_at between %s and %s""".format(table_name)
        if check_modified:
            stmt += """AND updated_at - created_at > '1s'::interval"""
        rows = etl.pg.query(conn, stmt, (begin_time, end_time), debug=True)
        count = int(rows[0]['count'])
        if check_modified:
            logging.info("Table %s has %d modified row(s)", table_name.identifier, count)
        else:
            logging.info("Table %s has %d updated row(s)", table_name.identifier, count)
        total += count
    return total


def modified_rows(args, settings):
    dw = etl.env_value(settings("data_warehouse", "etl_user", "ENV"))
    schemas = [source["name"] for source in settings("sources")]
    with etl.pg.connection(dw, readonly=True) as cx:
        tables = find_tables(cx, schemas)
        total_update = count_updated_rows(cx, tables, args.begin_time, args.end_time)
        total_modified = count_updated_rows(cx, tables, args.begin_time, args.end_time, check_modified=True)
        logging.info("Number of rows updated between %s and %s:  %10.d", args.begin_time, args.end_time, total_update)
        logging.info("Number of rows modified between %s and %s: %10.d", args.begin_time, args.end_time, total_modified)


def build_parser():
    parser = etl.arguments.argument_parser(["config"], description=__doc__)
    parser.add_argument("begin_time", help="start of time window")
    parser.add_argument("end_time", help="start of time window")
    return parser


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        modified_rows(main_args, main_settings)
