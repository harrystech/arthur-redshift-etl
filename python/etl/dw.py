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
    """
    logger = logging.getLogger(__name__)

    with closing(etl.pg.connection(config.dsn_admin)) as conn:
        with conn:
            # FIXME download the list of users and groups from the database, then create those that don't exist already
            if not skip_user_creation:
                logger.info("Creating required groups: %s", join_with_quotes(config.groups))
                for group in config.groups:
                    etl.pg.create_group(conn, group)
                for user in config.users:
                    logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                    etl.pg.create_user(conn, user.name, user.group)
                # Note that 'public' in the search path is ignored when 'public' does not exist.
                logger.info("Clearing search path for user '%s'", config.owner)
                etl.pg.alter_search_path(conn, config.owner, ['public'])

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


def create_new_user(config, new_user, is_etl_user=False, add_user_schema=False, skip_user_creation=False):
    """
    Add new user to database within default user group and with new password.
    If so advised, creates a schema for the user.
    If so advised, adds the user to the ETL group, giving R/W access.
    """
    logger = logging.getLogger(__name__)

    with closing(etl.pg.connection(config.dsn_admin)) as conn:
        with conn:
            if not skip_user_creation:
                logger.info("Creating user '%s' in group '%s'", new_user, config.default_group)
                etl.pg.create_user(conn, new_user, config.default_group)
            if is_etl_user:
                logger.info("Adding user '%s' to ETL group '%s'", new_user, config.groups[0])
                etl.pg.alter_group_add_user(conn, config.groups[0], new_user)
            if add_user_schema:
                # FIXME Lookup schema in config?
                logger.info("Creating schema '%s' with owner '%s'", new_user, new_user)
                etl.pg.create_schema(conn, new_user, new_user)
                etl.pg.grant_usage(conn, new_user, config.default_group)
                etl.pg.grant_usage(conn, new_user, config.groups[0])
            if new_user != config.owner:
                # Always lead with the user's schema (even if it doesn't exist) to deal with schema updates gracefully.
                # FIXME If user is a "system user" only put their schema into the search path.
                search_path = ["'$user'"] + list(reversed([schema.name for schema in config.schemas]))
                logger.info("Setting search path to: %s", search_path)
                etl.pg.alter_search_path(conn, new_user, search_path)


def ping(dsn):
    """
    Send a test query to the data warehouse
    """
    with closing(etl.pg.connection(dsn)) as conn:
        if etl.pg.ping(conn):
            print("{} is alive".format(etl.pg.dbname(conn)))
