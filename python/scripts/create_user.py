#! /usr/bin/env python3

"""
Create new user in the "users group" and add a personal schema in the database.

The search path is set to the user's own schema and all the schemas from the
configuration.  (Note the order.)
"""

import getpass
import logging

import etl
import etl.arguments
import etl.config
import etl.pg


def create_user(args, settings):
    """
    Add new user to database within user group and with given password
    """
    dsn_admin = etl.env_value(settings("data-warehouse", "admin_access", "ENV"))
    new_user = args.username
    user_group = settings("data-warehouse", "groups", "users")
    search_path = ["'$user'"] + [source["name"] for source in settings("sources")]

    with etl.pg.connection(dsn_admin) as conn:
        logging.info("Creating user %s in user group %s", new_user, user_group)
        etl.pg.create_user(conn, new_user, args.password, user_group, debug=True)
        logging.info("Creating schema %s with owner %s", new_user, new_user)
        etl.pg.create_schema(conn, new_user, new_user, debug=True)
        logging.info("Setting search path to: %s", search_path)
        etl.pg.alter_search_path(conn, new_user, search_path)


def build_parser():
    return etl.arguments.argument_parser(["config", "username", "password"], description=__doc__)


if __name__ == "__main__":
    main_args = build_parser().parse_args()
    etl.config.configure_logging()
    main_settings = etl.config.load_settings(main_args.config)
    if main_args.password is None:
        main_args.password = getpass.getpass("Password for %s: " % main_args.username)
    with etl.pg.measure_elapsed_time():
        create_user(main_args, main_settings)
