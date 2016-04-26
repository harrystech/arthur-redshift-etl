#! /usr/bin/env python3

"""
Create new user in the "users group" and add a personal schema in the database.

The search path is set to the user's own schema (if created) and all the schemas
from the configuration in the order they are defined.  (Note that the user's
schema comes first.)

Oddly enough, it is possible to skip the "create user" step which comes in
handy when you want to update the user's search path. (But you have to make
sure to be consistent with the user schema option so that you don't lose
the '$user' from the search path accidentally.)
"""

from contextlib import closing
import getpass
import logging

import etl
import etl.arguments
import etl.config
import etl.pg


def create_user(args, settings):
    """
    Add new user to database within user group and with given password.
    If so advised, creates a schema for the user.
    If so advised, adds the user to the ETL group, giving R/W access.
    """
    dsn_admin = etl.env_value(settings("data_warehouse", "admin_access"))
    new_user = args.username
    user_group = settings("data_warehouse", "groups", "users")
    etl_group = settings("data_warehouse", "groups", "etl")
    search_path = [source["name"] for source in settings("sources")]

    with closing(etl.pg.connection(dsn_admin)) as conn:
        logging.info("Creating user '%s' in user group '%s'", new_user, user_group)
        with conn:
            if not args.skip_user_creation:
                etl.pg.create_user(conn, new_user, args.password, user_group)
            if args.etl_user:
                logging.info("Adding user '%s' to ETL group '%s'", new_user, etl_group)
                etl.pg.alter_group_add_user(conn, etl_group, new_user)
            if args.add_user_schema:
                logging.info("Creating schema '%s' with owner '%s'", new_user, new_user)
                etl.pg.create_schema(conn, new_user, new_user)
                etl.pg.grant_usage(conn, new_user, user_group)
                etl.pg.grant_usage(conn, new_user, etl_group)
                search_path[:0] = ["'$user'"]
            logging.info("Setting search path to: %s", search_path)
            etl.pg.alter_search_path(conn, new_user, search_path)


def build_argument_parser():
    parser = etl.arguments.argument_parser(["config", "username", "password"], description=__doc__)
    parser.add_argument("-e", "--etl-user", help="Add user also to ETL group", action="store_true")
    parser.add_argument("-a", "--add-user-schema", help="Add new schema, writable for the user", action="store_true")
    parser.add_argument("-k", "--skip-user-creation", help="Skip new user; only change search path of existing user",
                        default=False, action="store_true")
    return parser


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    if main_args.password is None and not main_args.skip_user_creation:
        main_args.password = getpass.getpass("Password for %s: " % main_args.username)
    with etl.pg.measure_elapsed_time():
        create_user(main_args, main_settings)
