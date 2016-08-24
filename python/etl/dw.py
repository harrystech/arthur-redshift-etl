#! /usr/bin/env python3

"""
initial_setup:

Create groups, users, schemas in the data warehouse.

Assumes that the database has already been created in the data warehouse.
Drops PUBLIC schema. Requires passing the password for the new ETL user on
the command line or entering it when prompted.

If you need to re-run this (after adding available schemas in the
configuration), you should skip the user and group creation.


create_user:

Create new user in the "users group".  Optionally add a personal schema in the database.

The search path is set to the user's own schema (if created) and all the schemas
from the configuration in the order they are defined.  (Note that the user's
schema comes first.)

Oddly enough, it is possible to skip the "create user" step but that comes in
handy when you want to update the user's search path. (But you have to make
sure to be consistent with the user schema option so that you don't lose
the '$user' from the search path accidentally.)
"""

from contextlib import closing
import getpass
import logging

import etl
import etl.commands
import etl.config
import etl.pg


def get_password(username):
    password = getpass.getpass("Password for %s: " % username)
    if len(password) < 1:
        raise RuntimeError("Empty password")
    check = getpass.getpass("Re-enter password: ")
    if password != check:
        raise RuntimeError("Passwords do not match")
    return password


def initial_setup(settings, password, skip_user_creation):
    """
    Initialize data warehouse with schemas and users
    """
    logger = logging.getLogger(__name__)

    dsn_admin = etl.config.env_value(settings("data_warehouse", "admin_access"))
    etl_user = settings("data_warehouse", "owner")
    etl_group = settings("data_warehouse", "groups", "etl")
    user_group = settings("data_warehouse", "groups", "users")
    schemas = [source["name"] for source in settings("sources") + settings("data_warehouse", "schemas")]

    if password is None and not skip_user_creation:
        password = get_password(settings("data_warehouse", "owner"))

    with closing(etl.pg.connection(dsn_admin)) as conn:
        if not skip_user_creation:
            with conn:
                logger.info("Creating groups ('%s', '%s') and user ('%s')", etl_group, user_group, etl_user)
                etl.pg.create_group(conn, etl_group)
                etl.pg.create_group(conn, user_group)
                etl.pg.create_user(conn, etl_user, password, etl_group)
        with conn:
            database_name = etl.pg.dbname(conn)
            logger.info("Changing database '%s' to belong to the ETL owner '%s'", database_name, etl_user)
            etl.pg.execute(conn, """ALTER DATABASE "{}" OWNER TO "{}" """.format(database_name, etl_user))
            logger.info("Dropping public schema in database '%s'", database_name)
            etl.pg.execute(conn, """DROP SCHEMA IF EXISTS PUBLIC CASCADE""")
            etl.pg.execute(conn, """REVOKE TEMPORARY ON DATABASE "{}" FROM PUBLIC""".format(database_name))
            # N.B. CTEs use temporary tables so to allow users WITH clause access, grant temp.
            etl.pg.execute(conn, """GRANT TEMPORARY ON DATABASE "{}" TO GROUP "{}" """.format(database_name, etl_group))
            etl.pg.execute(conn, """GRANT TEMPORARY ON DATABASE "{}" TO GROUP "{}" """.format(database_name, user_group))
            # Create one schema for every source database
            for schema in schemas:
                logger.info("Creating schema '%s' with owner '%s' and usage grant for '%s'",
                             schema, etl_user, user_group)
                etl.pg.create_schema(conn, schema, etl_user)
                etl.pg.grant_all_on_schema(conn, schema, etl_group)
                etl.pg.grant_usage(conn, schema, user_group)
            logger.info("Clearing search path for user '%s'", etl_user)
            # Note that 'public' in the search path is ignored when 'public' does not exist.
            etl.pg.alter_search_path(conn, etl_user, ['public'])


def create_user(settings, new_user, password, is_etl_user, add_user_schema, skip_user_creation):
    """
    Add new user to database within user group and with given password.
    If so advised, creates a schema for the user.
    If so advised, adds the user to the ETL group, giving R/W access.
    """
    logger = logging.getLogger(__name__)

    dsn_admin = etl.config.env_value(settings("data_warehouse", "admin_access"))
    user_group = settings("data_warehouse", "groups", "users")
    etl_group = settings("data_warehouse", "groups", "etl")
    schemas = [source["name"] for source in settings("sources") + settings("data_warehouse", "schemas")]

    if password is None and not skip_user_creation:
        password = get_password(new_user)

    with closing(etl.pg.connection(dsn_admin)) as conn:
        with conn:
            if not skip_user_creation:
                logger.info("Creating user '%s' in user group '%s'", new_user, user_group)
                etl.pg.create_user(conn, new_user, password, user_group)
            if is_etl_user:
                logger.info("Adding user '%s' to ETL group '%s'", new_user, etl_group)
                etl.pg.alter_group_add_user(conn, etl_group, new_user)
            if add_user_schema:
                logger.info("Creating schema '%s' with owner '%s'", new_user, new_user)
                etl.pg.create_schema(conn, new_user, new_user)
                etl.pg.grant_usage(conn, new_user, user_group)
                etl.pg.grant_usage(conn, new_user, etl_group)
            # Always lead with the user's schema (even if it doesn't exist) to deal with schema updates gracefully.
            search_path = ["'$user'"] + list(reversed(schemas))
            logger.info("Setting search path to: %s", search_path)
            etl.pg.alter_search_path(conn, new_user, search_path)


def ping(settings):
    """
    Send a test query to the data warehouse
    """
    dsn_admin = etl.config.env_value(settings("data_warehouse", "admin_access"))
    with closing(etl.pg.connection(dsn_admin)) as conn:
        if etl.pg.ping(conn):
            print("{} lives".format(etl.pg.dbname(conn)))
