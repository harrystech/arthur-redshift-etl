#! /usr/bin/env python3

"""
Create groups, users, schemas in database.

Assumes that the database has already been created.  Drops PUBLIC schema.
Requires passing the password for the new ETL user on the command line
or from stdin.
"""

import getpass
import logging

import etl
import etl.arguments
import etl.config
import etl.pg


def initial_setup(args, settings):

    dsn_admin = etl.env_value(settings("data-warehouse", "admin_access", "ENV"))
    etl_user = settings("data-warehouse", "etl_user", "name")
    etl_group = settings("data-warehouse", "groups", "etl")
    users_group = settings("data-warehouse", "groups", "users")
    schemas = [source["name"] for source in settings("sources")]

    with etl.pg.connection(dsn_admin) as conn:
        logging.info("Creating groups (%s, %s) and user (%s)", etl_group, users_group, etl_user)
        etl.pg.create_group(conn, etl_group, debug=True)
        etl.pg.create_group(conn, users_group, debug=True)
        etl.pg.create_user(conn, etl_user, args.password, etl_group, debug=True)

        logging.info("Changing database %s to belong to the ETL user (%s)", etl_user, etl_user)
        dbname = etl.pg.dbname(conn)
        etl.pg.execute(conn, """ALTER DATABASE "{}" OWNER TO "{}" """.format(dbname, etl_user), debug=True)
        etl.pg.execute(conn, """REVOKE TEMP ON DATABASE "{}" FROM PUBLIC""".format(etl_user), debug=True)
        etl.pg.execute(conn, """DROP SCHEMA IF EXISTS PUBLIC CASCADE""", debug=True)

        # Create one schema for every source database
        for schema in schemas:
            logging.info("Creating schema %s with owner %s and usage grant for %s", schema, etl_user, users_group)
            etl.pg.create_schema(conn, schema, etl_user, debug=True)
            etl.pg.grant_all(conn, schema, etl_group, debug=True)
            etl.pg.grant_usage(conn, schema, users_group, debug=True)


def build_parser():
    return etl.arguments.argument_parser(["config", "password"], description=__doc__)


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    if main_args.password is None:
        main_args.password = getpass.getpass("Password for %s: " % main_settings("data-warehouse", "etl_user", "name"))
    with etl.pg.measure_elapsed_time():
        initial_setup(main_args, main_settings)
