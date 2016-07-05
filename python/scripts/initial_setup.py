#! /usr/bin/env python3

"""
Create groups, users, schemas in the data warehouse.

Assumes that the database has already been created in the data warehouse.
Drops PUBLIC schema. Requires passing the password for the new ETL user on
the command line or entering it when prompted.

If you need to re-run this (after adding available schemas in the
configuration), you should skip the user and group creation.
"""

from contextlib import closing
import getpass
import logging

import etl
import etl.arguments
import etl.config
import etl.pg


def initial_setup(args, settings):

    dsn_admin = etl.env_value(settings("data_warehouse", "admin_access"))
    etl_user = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    schemas = [source["name"] for source in settings("sources")]

    with closing(etl.pg.connection(dsn_admin)) as conn:
        if not args.skip_user_creation:
            with conn:
                logging.info("Creating groups ('%s', '%s') and user ('%s')", etl_group, user_group, etl_user)
                etl.pg.create_group(conn, etl_group)
                etl.pg.create_group(conn, user_group)
                etl.pg.create_user(conn, etl_user, args.password, etl_group)
        with conn:
            database_name = etl.pg.dbname(conn)
            logging.info("Changing database '%s' to belong to the ETL owner '%s'", database_name, etl_user)
            etl.pg.execute(conn, """ALTER DATABASE "{}" OWNER TO "{}" """.format(database_name, etl_user))
            logging.info("Dropping public schema in database '%s'", database_name)
            etl.pg.execute(conn, """DROP SCHEMA IF EXISTS PUBLIC CASCADE""")
            etl.pg.execute(conn, """REVOKE TEMPORARY ON DATABASE "{}" FROM PUBLIC""".format(database_name))
            # N.B. CTEs use temporary tables so to allow users WITH clause access, grant temp.
            etl.pg.execute(conn, """GRANT TEMPORARY ON DATABASE "{}" TO GROUP "{}" """.format(database_name, etl_group))
            etl.pg.execute(conn, """GRANT TEMPORARY ON DATABASE "{}" TO GROUP "{}" """.format(database_name, user_group))
            # Create one schema for every source database
            for schema in schemas:
                logging.info("Creating schema '%s' with owner '%s' and usage grant for '%s'",
                             schema, etl_user, user_group)
                etl.pg.create_schema(conn, schema, etl_user)
                etl.pg.grant_all_on_schema(conn, schema, etl_group)
                etl.pg.grant_usage(conn, schema, user_group)
            logging.info("Setting search path to: %s", schemas)
            etl.pg.alter_search_path(conn, etl_user, schemas)


def build_argument_parser():
    parser = etl.arguments.argument_parser(["config", "password"], description=__doc__)
    parser.add_argument("-k", "--skip-user-creation", help="Skip user and groups, only create schemas",
                        default=False, action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    if main_args.password is None and not main_args.skip_user_creation:
        main_args.password = getpass.getpass("Password for %s: " % main_settings("data_warehouse", "owner"))
    with etl.measure_elapsed_time(), etl.pg.log_error():
        initial_setup(main_args, main_settings)
