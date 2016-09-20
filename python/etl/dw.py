"""
Module for data warehouse configuration, initialization and user micro management.

initial_setup: Create groups, users, schemas in the data warehouse.

Assumes that the database has already been created in the data warehouse.
Drops PUBLIC schema. Requires entering the password for the ETL on the command
line or having a password in the .pgpass file.

If you need to re-run this (after adding available schemas in the
configuration), you should skip the user and group creation.


create_user: Create new user.  Optionally add a personal schema in the database.

The search path is set to the user's own schema and all the schemas
from the configuration in the order they are defined.  (Note that the user's
schema (as in "$user") comes first.)

Oddly enough, it is possible to skip the "create user" step but that comes in
handy when you want to update the user's search path.
"""

from contextlib import closing
import logging

from etl import join_with_quotes
import etl.commands
import etl.config
import etl.pg


def initial_setup(config, skip_user_creation=False):
    """
    Initialize data warehouse with schemas and users

    This is safe to re-run as long as you skip creating users and groups the second time around.
    """
    logger = logging.getLogger(__name__)

    with closing(etl.pg.connection(config.dsn_admin)) as conn:
        with conn:
            if not skip_user_creation:
                logger.info("Creating required groups: %s", join_with_quotes(config.groups))
                for group in config.groups:
                    etl.pg.create_group(conn, group)
                for user in config.users:
                    logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                    etl.pg.create_user(conn, user.name, user.group)
        with conn:
            database_name = etl.pg.dbname(conn)
            logger.info("Changing database '%s' to belong to the ETL owner '%s'", database_name, config.owner)
            etl.pg.execute(conn, """ALTER DATABASE "{}" OWNER TO "{}" """.format(database_name, config.owner))
            logger.info("Dropping public schema in database '%s'", database_name)
            etl.pg.execute(conn, """DROP SCHEMA IF EXISTS PUBLIC CASCADE""")

            for schema in config.schemas:
                logger.info("Creating schema '%s', granting access to %s", schema.name, join_with_quotes(schema.groups))
                etl.pg.create_schema(conn, schema.name, config.owner)
                etl.pg.grant_all_on_schema(conn, schema.name, schema.groups[0])
                for group in schema.groups[1:]:
                    etl.pg.grant_usage(conn, schema.name, group)

            # Note that 'public' in the search path is ignored when 'public' does not exist.
            logger.info("Clearing search path for users: %s", join_with_quotes(user.name for user in config.users))
            for user in config.users:
                etl.pg.alter_search_path(conn, user.name, ['public'])


def create_new_user(config, new_user, is_etl_user=False, add_user_schema=False, skip_user_creation=False):
    """
    Add new user to database within default user group and with new password.
    If so advised, creates a schema for the user (with the schema name the same as the name of the user).
    If so advised, adds the user to the ETL group, giving R/W access. Use wisely.

    This is safe to re-run as long as you skip creating users and groups the second time around.
    """
    logger = logging.getLogger(__name__)

    # Find user in the list of pre-defined users or create new user instance with default settings
    for user in config.users:
        if user.name == new_user:
            break
    else:
        user = etl.config.DataWarehouseUser({"name": new_user,
                                             "group": config.default_group,
                                             "schema": new_user})
    if user.name in ("default", config.owner):
        raise ValueError("Illegal user name '%s'" % user.name)

    with closing(etl.pg.connection(config.dsn_admin)) as conn:
        with conn:
            if not skip_user_creation:
                logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                etl.pg.create_user(conn, user.name, user.group)
            if is_etl_user:
                logger.info("Adding user '%s' to ETL group '%s'", user.name, config.groups[0])
                etl.pg.alter_group_add_user(conn, config.groups[0], user.name)
            if add_user_schema:
                logger.info("Creating schema '%s' with owner '%s'", user.schema, user.name)
                etl.pg.create_schema(conn, user.schema, user.name)
                etl.pg.grant_all_on_schema(conn, user.schema, config.groups[0])
                etl.pg.grant_usage(conn, user.schema, user.group)
            # Non-system users have "their" schema in the search path, others get nothing (meaning just public).
            search_path = ["public"]
            if user.schema == user.name:
                search_path[:0] = ["'$user'"]  # needs to be quoted
            logger.info("Setting search path for user '%s' to: %s", user.name, ", ".join(search_path))
            etl.pg.alter_search_path(conn, user.name, search_path)


def ping(dsn):
    """
    Send a test query to the data warehouse
    """
    with closing(etl.pg.connection(dsn)) as conn:
        if etl.pg.ping(conn):
            print("{} is alive".format(etl.pg.dbname(conn)))
