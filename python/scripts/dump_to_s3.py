#!  /usr/bin/env python3

"""
Connect to source databases and download table definitions and content,
store them in design files and CSV files locally, then upload files to S3.

While the table names remain the same, the target schema will be named after
the source name in the configuration file with the goal of assembling data from
multiple databases into one without name clashes.
"""

import argparse
import concurrent.futures
from fnmatch import fnmatch
import logging
import os
import os.path
import threading

import etl
import etl.arguments
import etl.config
import etl.dump
import etl.load
import etl.pg
import etl.s3


def normalize_and_create(directory):
    name = os.path.normpath(directory)
    if not os.path.exists(name):
        logging.debug("Creating data directory '%s'", name)
        os.makedirs(name)
    return name


def dump_to_s3(args, settings):
    bucket_name = settings("s3", "bucket_name")
    if args.table and '.' in args.table:
        schema_selector, table_selector = args.table.split('.', 1)
    elif args.table:
        schema_selector, table_selector = args.table, None
    else:
        schema_selector, table_selector = None, None

    for source in settings("sources"):
        source_name = source["name"]
        if "config" not in source:
            logging.info("Skipping empty source %s", source_name)
            continue
        if not (schema_selector is None or fnmatch(source_name, schema_selector)):
            logging.info("Skipping source %s which does not match '%s'", source_name, schema_selector)
            continue
        logging.info("Tackling source %s", source_name)
        conn = etl.pg.connection(etl.env_value(source["config"]), autocommit=True, readonly=True)

        tables = etl.dump.fetch_tables(conn,
                                       source["include_tables"],
                                       source.get("exclude_tables", []),
                                       table_selector)
        columns = etl.dump.fetch_columns(conn, tables)
        table_def = etl.dump.map_types_in_ddl(columns,
                                              settings("type_maps", "as_is_att_type"),
                                              settings("type_maps", "cast_needed_att_type"))
        design_dir = normalize_and_create(os.path.join(args.table_design_dir, source_name))
        output_dir = normalize_and_create(os.path.join(args.data_dir, source_name))
        source_prefix = "{}/{}".format(args.prefix, source["name"])
        # There are four steps per table of which one is bounded by a semaphore so max_workers=4 seems best.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            single_copy_at_a_time = threading.BoundedSemaphore(1)
            for table_name in sorted(table_def):
                columns = table_def[table_name]
                design_file_future = executor.submit(etl.dump.save_table_design,
                                                     source_name, table_name, columns, design_dir,
                                                     dry_run=args.dry_run)
                csv_file_future = executor.submit(etl.dump.download_table_data_bounded,
                                                  single_copy_at_a_time,
                                                  conn, source_name, table_name, columns, output_dir,
                                                  limit=args.limit, overwrite=args.force, dry_run=args.dry_run)
                for file_future in (design_file_future, csv_file_future):
                    executor.submit(etl.s3.upload_to_s3,
                                    file_future, bucket_name, source_prefix, dry_run=args.dry_run)
        logging.info("Done with %d table(s) from %s", len(table_def), source_name)
        conn.close()
    if args.limit:
        logging.warning("The row limit was set to %d!", args.limit)


def check_positive_int(s):
    """
    Helper method for argument parser to make sure optional arg with value 's'
    is a positive integer (meaning, s > 0)
    """
    try:
        i = int(s)
        if i <= 0:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("%s is not a positive int" % s)
    return i


def build_parser():
    parser = etl.arguments.argument_parser(["config", "prefix", "data-dir", "table-design-dir", "dry-run", "force",
                                            "table"], description=__doc__)
    parser.add_argument("-l", "--limit", help="limit number of rows copied (useful for testing)",
                        default=None, type=check_positive_int, action="store")
    return parser


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    with etl.pg.measure_elapsed_time():
        dump_to_s3(main_args, main_settings)
