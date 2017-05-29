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


def create_schemas(dsn, schemas, dry_run=False) -> None:
    """
    Create schemas and grant access.
    It's ok if any of the schemas already exist (in which case the owner and privileges are updated).
    """
    with closing(etl.pg.connection(dsn, autocommit=True, readonly=dry_run)) as conn:
        for schema in schemas:
            create_schema_and_grant_access(conn, schema, dry_run=dry_run)


def create_schema_and_grant_access(conn, schema, owner=None, dry_run=False) -> None:
    group_names = join_with_quotes(schema.groups)
    if dry_run:
        logger.info("Dry-run: Skipping creating schema '%s' and granting access to '%s'", schema.name, group_names)
    else:
        logger.info("Creating schema '%s', granting access to %s", schema.name, group_names)
        etl.pg.create_schema(conn, schema.name, owner)
        etl.pg.grant_all_on_schema_to_user(conn, schema.name, schema.owner)
        for group in schema.groups:
            # Readers/writers are differentiated in table permissions, not schema permissions
            etl.pg.grant_usage(conn, schema.name, group)


def backup_schemas(dsn, schemas, dry_run=False) -> None:
    """
    For existing schemas, rename them and drop access.
    Once the access is revoked, the backup schemas "disappear" from BI tools.
    """
    with closing(etl.pg.connection(dsn, autocommit=True, readonly=dry_run)) as conn:
        names = [schema.name for schema in schemas]
        found = etl.pg.select_schemas(conn, names)
        need_backup = [schema for schema in schemas if schema.name in found]
        if not need_backup:
            logger.info("Found no existing schemas to backup")
            return
        selected_names = join_with_quotes(name for name in names if name in found)
        if dry_run:
            logger.info("Dry-run: Skipping backup of schema(s) %s", selected_names)
            return
        logger.info("Creating backup of schema(s) %s", selected_names)
        for schema in need_backup:
            logger.info("Revoking access from readers to schema '%s' before backup", schema.name)
            for reader_group in schema.reader_groups:
                etl.pg.revoke_usage(conn, schema.name, reader_group)
                etl.pg.revoke_select_on_all_tables_in_schema(conn, schema.name, reader_group)
            logger.info("Renaming schema '%s' to backup '%s'", schema.name, schema.backup_name)
            etl.pg.drop_schema(conn, schema.backup_name)
            etl.pg.alter_schema_rename(conn, schema.name, schema.backup_name)


def restore_schemas(dsn, schemas, dry_run=False) -> None:
    """
    For the schemas that we need / want, rename the backups and restore access.
    This is the inverse of backup_schemas.
    """
    with closing(etl.pg.connection(dsn, autocommit=True, readonly=dry_run)) as conn:
        names = [schema.backup_name for schema in schemas]
        found = etl.pg.select_schemas(conn, names)
        need_restore = [schema for schema in schemas if schema.backup_name in found]
        if not need_restore:
            logger.info("Found no backup schemas to restore")
            return
        selected_names = join_with_quotes(schema.name for schema in need_restore)
        if dry_run:
            logger.info("Dry-run: Skipping restore of schema(s) %s", selected_names)
            return
        logger.info("Restoring from backup schema(s) %s", selected_names)
        for schema in need_restore:
            logger.info("Renaming schema '%s' from backup '%s'", schema.name, schema.backup_name)
            etl.pg.drop_schema(conn, schema.name)
            etl.pg.alter_schema_rename(conn, schema.backup_name, schema.name)
            logger.info("Granting readers access to schema '%s' after restore", schema.name)
            for reader_group in schema.reader_groups:
                etl.pg.grant_usage(conn, schema.name, reader_group)
                etl.pg.grant_select_in_schema(conn, schema.name, reader_group)


def initial_setup(config, with_user_creation=False, force=False, dry_run=False):
    """
    Place named data warehouse database into initial state.

    This destroys the contents of the targeted database.
    You have to set `force` to true if the name of the database doesn't start with 'validation'.
    Optionally use `with_user_creation` flag to create users and groups.
    """
    try:
        database_name = config.dsn_etl['database']
    except (KeyError, ValueError):
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

    if dry_run:
        logger.info("Dry-run: Skipping drop and create of database '%s' with owner '%s'", database_name, config.owner)
        logger.info("Dry-run: Skipping drop of PUBLIC schema in '%s'", database_name)
    else:
        logger.info("Dropping and creating database '%s' with owner '%s'", database_name, config.owner)
        admin_dev_conn = etl.pg.connection(config.dsn_admin, autocommit=True)
        with closing(admin_dev_conn):
            etl.pg.drop_and_create_database(admin_dev_conn, database_name, config.owner)
        # Connect as admin to new database to drop `public`
        admin_target_db_conn = etl.pg.connection(dict(config.dsn_admin, database=database_name), autocommit=True)
        with closing(admin_target_db_conn):
            logger.info("Dropping PUBLIC schema in '%s'", database_name)
            etl.pg.drop_schema(admin_target_db_conn, "PUBLIC")


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
        raise ValueError("illegal user name '%s'" % user.name)

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
                create_schema_and_grant_access(conn, user_schema, owner=user.name, dry_run=dry_run)
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
