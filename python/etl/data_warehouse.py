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

import logging
from contextlib import closing
from typing import List

import psycopg2
from psycopg2.extensions import connection  # only for type annotation

import etl.commands
import etl.config
import etl.config.dw
import etl.db
from etl.config.dw import DataWarehouseSchema
from etl.errors import ETLError
from etl.names import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def create_schemas(schemas: List[DataWarehouseSchema], use_staging=False, dry_run=False) -> None:
    """
    Create schemas and grant access.
    It's ok if any of the schemas already exist (in which case the owner and privileges are updated).
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for schema in schemas:
            create_schema_and_grant_access(conn, schema, use_staging=use_staging, dry_run=dry_run)


def create_schema_and_grant_access(conn, schema, owner=None, use_staging=False, dry_run=False) -> None:
    group_names = join_with_quotes(schema.groups)
    name = schema.staging_name if use_staging else schema.name
    if dry_run:
        logger.info("Dry-run: Skipping creating schema '%s' and granting access to '%s'", name, group_names)
    else:
        logger.info("Creating schema '%s'", name)
        etl.db.create_schema(conn, name, owner)
        etl.db.grant_all_on_schema_to_user(conn, name, schema.owner)
        if use_staging:
            # Don't grant usage on staging schemas to readers/writers
            return None
        logger.info("Granting access to %s", group_names)
        for group in schema.groups:
            # Readers/writers are differentiated in table permissions, not schema permissions
            etl.db.grant_usage(conn, name, group)


def _promote_schemas(schemas: List[DataWarehouseSchema],
                     from_name_attr: str, dry_run=False) -> None:
    """
    Promote (staging or backup) schemas into their standard names and permissions
    Changes schema.from_name_attr -> schema.name; expects from_name_attr to be 'backup_name' or 'staging_name'
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        from_name_schema_lookup = {getattr(schema, from_name_attr): schema for schema in schemas}
        names = list(from_name_schema_lookup.keys())
        found = etl.db.select_schemas(conn, names)
        need_promotion = {name: schema for name, schema in from_name_schema_lookup.items() if name in found}
        if not need_promotion:
            logger.info("Found no %s schemas to promote", from_name_attr)
            return
        selected_names = join_with_quotes(need_promotion.keys())
        if dry_run:
            logger.info("Dry-run: Skipping promotion of schema(s): %s", selected_names)
            return

        logger.info("Promoting from %s schema(s) %s", from_name_attr, selected_names)
        for from_name, schema in need_promotion.items():
            logger.info("Renaming schema '%s' from '%s'", schema.name, from_name)
            etl.db.drop_schema(conn, schema.name)
            etl.db.alter_schema_rename(conn, from_name, schema.name)
            logger.info("Granting readers and writers access to schema '%s' after promotion", schema.name)
            grant_schema_permissions(conn, schema)


def backup_schemas(schemas: List[DataWarehouseSchema], dry_run=False) -> None:
    """
    For existing schemas, rename them and drop access.
    Once the access is revoked, the backup schemas "disappear" from BI tools.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        names = [schema.name for schema in schemas]
        found = etl.db.select_schemas(conn, names)
        need_backup = [schema for schema in schemas if schema.name in found]
        if not need_backup:
            logger.info("Found no existing schemas to backup")
            return
        selected_names = join_with_quotes(name for name in names if name in found)
        if dry_run:
            logger.info("Dry-run: Skipping backup of schema(s): %s", selected_names)
            return

        logger.info("Creating backup of schema(s) %s", selected_names)
        for schema in need_backup:
            logger.info("Revoking access from readers and writers to schema '%s' before backup", schema.name)
            revoke_schema_permissions(conn, schema)
            logger.info("Renaming schema '%s' to backup '%s'", schema.name, schema.backup_name)
            etl.db.drop_schema(conn, schema.backup_name)
            etl.db.alter_schema_rename(conn, schema.name, schema.backup_name)


def restore_schemas(schemas: List[DataWarehouseSchema], dry_run=False) -> None:
    """
    For the schemas that we need / want, rename the backups and restore access.
    This is the inverse of backup_schemas.
    Useful if bad data is in standard schemas
    """
    _promote_schemas(schemas, 'backup_name', dry_run=dry_run)


def publish_schemas(schemas: List[DataWarehouseSchema], dry_run=False) -> None:
    """
    Put staging schemas into their standard configuration
    (First backs up current occupants of standard position)
    """
    backup_schemas(schemas, dry_run=dry_run)
    _promote_schemas(schemas, 'staging_name', dry_run=dry_run)


def grant_schema_permissions(conn: connection, schema: DataWarehouseSchema) -> None:
    """
    Grant usage to readers and writers
    Grant select to readers and select & write to writers
    """
    for reader_group in schema.reader_groups:
        etl.db.grant_usage(conn, schema.name, reader_group)
        etl.db.grant_select_on_all_tables_in_schema(conn, schema.name, reader_group)
    for writer_group in schema.writer_groups:
        etl.db.grant_usage(conn, schema.name, writer_group)
        etl.db.grant_select_and_write_on_all_tables_in_schema(conn, schema.name, writer_group)


def revoke_schema_permissions(conn: connection, schema: DataWarehouseSchema) -> None:
    """
    Revoke usage to readers and writers
    Revoke select to readers and select & write to writers
    """
    for reader_group in schema.reader_groups:
        etl.db.revoke_usage(conn, schema.name, reader_group)
        etl.db.revoke_select_on_all_tables_in_schema(conn, schema.name, reader_group)
    for writer_group in schema.writer_groups:
        etl.db.revoke_usage(conn, schema.name, writer_group)
        etl.db.revoke_select_and_write_on_all_tables_in_schema(conn, schema.name, writer_group)


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
            with closing(etl.db.connection(config.dsn_admin)) as conn:
                with conn:
                    logger.info("Creating required groups: %s", join_with_quotes(config.groups))
                    for group in config.groups:
                        etl.db.create_group(conn, group)
                    for user in config.users:
                        logger.info("Creating user '%s' in group '%s' with empty search path", user.name, user.group)
                        etl.db.create_user(conn, user.name, user.group)
                        etl.db.alter_search_path(conn, user.name, ['public'])

    if dry_run:
        logger.info("Dry-run: Skipping drop and create of database '%s' with owner '%s'", database_name, config.owner)
        logger.info("Dry-run: Skipping drop of PUBLIC schema in '%s'", database_name)
    else:
        logger.info("Dropping and creating database '%s' with owner '%s'", database_name, config.owner)
        admin_dev_conn = etl.db.connection(config.dsn_admin, autocommit=True)
        with closing(admin_dev_conn):
            etl.db.drop_and_create_database(admin_dev_conn, database_name, config.owner)
        # Connect as admin to new database to drop `public`
        admin_target_db_conn = etl.db.connection(dict(config.dsn_admin, database=database_name), autocommit=True)
        with closing(admin_target_db_conn):
            logger.info("Dropping PUBLIC schema in '%s'", database_name)
            etl.db.drop_schema(admin_target_db_conn, "PUBLIC")


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

    with closing(etl.db.connection(config.dsn_admin_on_etl_db, autocommit=True, readonly=dry_run)) as conn:
        with conn:
            if not skip_user_creation:
                if dry_run:
                    logger.info("Dry-run: Skipping creating user '%s' in group '%s'", user.name, user.group)
                else:
                    logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                    try:
                        etl.db.create_user(conn, user.name, user.group)
                    except psycopg2.ProgrammingError:
                        etl.db.create_group(conn, user.group)
                        etl.db.create_user(conn, user.name, user.group)

            if group is not None:
                # FIXME This check should come before creating the user
                if group not in config.groups:
                    raise ValueError("Specified group ('%s') not present in DataWarehouseConfig" % group)
                if dry_run:
                    logger.info("Dry-run: Skipping adding user '%s' to group '%s'", user.name, group)
                else:
                    logger.info("Adding user '%s' to group '%s'", user.name, group)
                    etl.db.alter_group_add_user(conn, group, user.name)
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
                etl.db.alter_search_path(conn, user.name, search_path)
