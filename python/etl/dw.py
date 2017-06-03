"""
Module for data warehouse configuration, initialization and user micro management.

initial_setup: Create groups, users; (re-)create database and schemas in the data warehouse.

Note that it is important that the admin access to the ETL is using the `dev` database
and not the data warehouse.

Requires having the password for all declared users in a ~/.pgpass file.

If you need to re-run this (after adding available schemas in the
configuration), you should skip the user and group creation.

create_user: Create new user.  Optionally add a personal schema in the database.

Oddly enough, it is possible to skip the "create user" step but that comes in
handy when you want to update the user's search path.
"""

from contextlib import closing
import logging

import etl.commands
import etl.config
import etl.config.dw
from etl.errors import ETLError
from etl.names import join_with_quotes
import etl.pg

import psycopg2

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def create_schemas(conn, schemas, owner=None, staging=False, after_backup=True):
    """
    Create schemas where relations will be built
    """
    if after_backup:
        logger.info("Backing up schemas before creation")
        backup_schemas(conn, schemas)
    for schema in schemas:
        name = schema.staging_name if staging else schema.name
        logger.info("Creating schema '%s'", name)
        etl.pg.create_schema(conn, name, owner)
        etl.pg.grant_all_on_schema_to_user(conn, name, schema.owner)
        if staging:
            # Don't grant usage on staging schemas to readers/writers
            continue
        logger.info("Granting access to %s", join_with_quotes(schema.groups))
        for group in schema.groups:
            # Readers/writers are differentiated in table permissions, not schema permissions
            etl.pg.grant_usage(conn, name, group)


def backup_schemas(conn, schemas):
    """
    Rename schemas from their standard names to their backup_names
    """
    for schema in schemas:
        if not etl.pg.schema_exists(conn, schema.name):
            logger.info("Skipping backup of '%s' as it does not exist", schema.name)
            continue
        logger.info("Revoking read access to schema '%s' before backup", schema.name)
        for reader_group in schema.reader_groups:
            etl.pg.revoke_usage(conn, schema.name, reader_group)
            etl.pg.revoke_select_in_schema(conn, schema.name, reader_group)
        logger.info("Renaming schema '%s' to backup '%s'", schema.name, schema.backup_name)
        etl.pg.execute(conn, """DROP SCHEMA IF EXISTS "{}" CASCADE""".format(schema.backup_name))
        etl.pg.alter_schema_rename(conn, schema.name, schema.backup_name)


def _promote_schemas(conn, schemas, from_name_attr):
    """
    Promote (staging or backup) schemas into their standard names and permissions
    Changes schema.from_name_attr -> schema.name; expects 'backup_name' or 'staging_name'
    """
    for schema in schemas:
        from_name = getattr(schema, from_name_attr)
        if not etl.pg.schema_exists(conn, from_name):
            logger.warning("Could not promote of '%s' as the %s does not exist",
                           schema.name, from_name_attr)
            continue
        logger.info("Renaming schema '%s' from %s '%s'", schema.name, from_name_attr, from_name)
        etl.pg.execute(conn, """DROP SCHEMA IF EXISTS "{}" CASCADE""".format(schema.name))
        etl.pg.alter_schema_rename(conn, from_name, schema.name)
        logger.info("Granting reader access to schema '%s' after rename", schema.name)
        for reader_group in schema.reader_groups:
            etl.pg.grant_usage(conn, schema.name, reader_group)
            etl.pg.grant_select_in_schema(conn, schema.name, reader_group)


def restore_schemas(conn, schemas):
    """
    Put backed-up schemas into their standard configuration
    Useful if bad data is in standard schemas
    """
    _promote_schemas(conn, schemas, 'backup_name')


def publish_schemas(conn, schemas):
    """
    Put staging schemas into their standard configuration
    (First backs up current occupants of standard position)
    """
    backup_schemas(conn, schemas)
    _promote_schemas(conn, schemas, 'staging_name')


def initial_setup(config, with_user_creation=False, force=False, dry_run=False):
    """
    Place named data warehouse database into initial state

    This destroys the contents of the targeted database.
    You have to set `force` to true if the name of the database doesn't start with 'validation'.
    Optionally use `with_users` flag to create users and groups.
    """
    try:
        database_name = config.dsn_etl['database']
    except KeyError:
        logger.critical("Could not identify database initialization target: ETL connection string not set")
        raise

    if database_name.startswith('validation'):
        logger.info("Initializing validation database '%s'", database_name)
    elif force:
        logger.info("Initializing non-validation database '%s' forcefully as requested", database_name)
    else:
        raise ETLError(
            "Refused to initialize non-validation database '%s' without the --force option" % database_name
        )

    if with_user_creation:
        if dry_run:
            logger.info("Dry-run: Skipping creation of required groups: %s", join_with_quotes(config.groups))
            logger.info("Dry-run: Skipping creation of required users: %s",
                        join_with_quotes(u.name for u in config.users))
        else:
            with closing(etl.pg.connection(config.dsn_admin)) as conn:
                with conn:
                    logger.info("Creating required groups: %s", join_with_quotes(config.groups))
                    for group in config.groups:
                        etl.pg.create_group(conn, group)
                    for user in config.users:
                        logger.info("Creating user '%s' in group '%s' with empty search path", user.name, user.group)
                        etl.pg.create_user(conn, user.name, user.group)
                        etl.pg.alter_search_path(conn, user.name, ['public'])
    if not database_name:
        logger.info("No database specified to initialize")
        return

    if dry_run:
        logger.info("Dry-run: Skipping drop and recreate of database '%s'", database_name)
        logger.info("Dry-run: skipping change of ownership over '%s' to ETL owner '%s'", database_name, config.owner)
        logger.info("Dry-run: skipping drop of PUBLIC schema in '%s'", database_name)
    else:
        logger.info("Dropping and recreating database '%s'", database_name)
        admin_dev_conn = etl.pg.connection(config.dsn_admin, autocommit=True)
        etl.pg.drop_and_create_database(admin_dev_conn, database_name)
        logger.info("Changing ownership over '%s' to ETL owner '%s'", database_name, config.owner)
        etl.pg.execute(admin_dev_conn, """ALTER DATABASE "{}" OWNER TO "{}" """.format(database_name, config.owner))
        # Connect as admin to new database just to drop `public`
        admin_target_db_conn = etl.pg.connection(dict(config.dsn_admin, database=database_name), autocommit=True)
        with closing(admin_target_db_conn):
            logger.info("Dropping PUBLIC schema in '%s'", database_name)
            etl.pg.execute(admin_target_db_conn, """DROP SCHEMA IF EXISTS "PUBLIC" CASCADE""")


def create_new_user(config, new_user, group=None, add_user_schema=False, skip_user_creation=False, dry_run=False):
    """
    Add new user to database, with provided or default group.
    If so advised, creates a schema for the user. (Making sure that the ETL user keeps read access).
    If the group was not initialized initially, then we create the user's group here.

    This is safe to re-run as long as you skip creating users and groups the second time around.
    """
    # Find user in the list of pre-defined users or create new user instance with default settings
    for user in config.users:
        if user.name == new_user:
            break
    else:
        info = {"name": new_user, "group": config.default_group}
        if add_user_schema:
            info["schema"] = new_user
        user = etl.config.dw.DataWarehouseUser(info)

    if user.name in ("default", config.owner):
        raise ValueError("Illegal user name '%s'" % user.name)

    with closing(etl.pg.connection(config.dsn_admin_on_etl_db)) as conn:
        with conn:
            if not skip_user_creation:
                if dry_run:
                    logger.info("Dry-run: Skipping creating user '%s' in group '%s'", user.name, user.group)
                else:
                    logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                    try:
                        etl.pg.create_user(conn, user.name, user.group)
                    except psycopg2.ProgrammingError:
                        etl.pg.create_group(conn, user.group)
                        etl.pg.create_user(conn, user.name, user.group)

            if group is not None:
                # FIXME This check should come before creating the user
                if group not in config.groups:
                    raise ValueError("Specified group ('%s') not present in DataWarehouseConfig" % group)
                if dry_run:
                    logger.info("Dry-run: Skipping adding user '%s' to group '%s'", user.name, group)
                else:
                    logger.info("Adding user '%s' to group '%s'", user.name, group)
                    etl.pg.alter_group_add_user(conn, group, user.name)
            if add_user_schema:
                user_schema = etl.config.dw.DataWarehouseSchema({"name": user.schema,
                                                                 "owner": user.name,
                                                                 "readers": [user.group, config.groups[0]]})
                if dry_run:
                    logger.info("Dry-run: Skipping creating schema '%s' with access for %s",
                                user_schema.name, join_with_quotes(user_schema.groups))
                else:
                    create_schemas(conn, [user_schema], owner=user.name)
            elif user.schema is not None:
                logger.warning("User '%s' has schema '%s' configured but adding that was not requested",
                               user.name, user.schema)
            # Non-system users have "their" schema in the search path, others get nothing (meaning just public).
            search_path = ["public"]
            if user.schema == user.name:
                search_path[:0] = ["'$user'"]  # needs to be quoted per documentation
            if dry_run:
                logger.info("Dry-run: Skipping setting search path for user '%s' to: %s", user.name, search_path)
            else:
                logger.info("Setting search path for user '%s' to: %s", user.name, search_path)
                etl.pg.alter_search_path(conn, user.name, search_path)
