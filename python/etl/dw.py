"""
Module for data warehouse configuration, initialization and user micro management.

initial_setup: Create groups, users; (re-)create database and schemas in the data warehouse.

Note that it is important that the admin access to the ETL is using the `dev` database
and not the data warehouse.

Requires entering the password for the ETL on the command
line or having a password in the .pgpass file.

If you need to re-run this (after adding available schemas in the
configuration), you should skip the user and group creation.

create_user: Create new user.  Optionally add a personal schema in the database.

Oddly enough, it is possible to skip the "create user" step but that comes in
handy when you want to update the user's search path.
"""

from contextlib import closing
import logging

from etl import join_with_quotes
import etl.commands
import etl.config
from etl.errors import ETLError
import etl.pg


def create_schemas(conn, schemas, owner=None):
    logger = logging.getLogger(__name__)

    for schema in schemas:
        logger.info("Creating schema '%s', granting access to %s", schema.name, join_with_quotes(schema.groups))
        etl.pg.create_schema(conn, schema.name, owner)
        etl.pg.grant_all_on_schema_to_user(conn, schema.name, schema.owner)
        for group in schema.groups:
            # Readers/writers are differentiated in table permissions, not schema permissions
            etl.pg.grant_usage(conn, schema.name, group)


def backup_schemas(conn, schemas):
    logger = logging.getLogger(__name__)

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


def restore_schemas(conn, schemas):
    logger = logging.getLogger(__name__)

    for schema in schemas:
        if not etl.pg.schema_exists(conn, schema.backup_name):
            logger.warning("Could not restore backup of '%s' as the backup does not exist", schema.name)
            continue
        logger.info("Renaming schema '%s' from backup '%s'", schema.name, schema.backup_name)
        etl.pg.execute(conn, """DROP SCHEMA IF EXISTS "{}" CASCADE""".format(schema.name))
        etl.pg.alter_schema_rename(conn, schema.backup_name, schema.name)
        logger.info("Granting reader access to schema '%s' after backup", schema.name)
        for reader_group in schema.reader_groups:
            etl.pg.grant_usage(conn, schema.name, reader_group)
            etl.pg.grant_select_in_schema(conn, schema.name, reader_group)


def initial_setup(config, with_user_creation=False, force=False, dry_run=False):
    """
    Place named data warehouse database into initial state

    This destroys the contents of the targeted database.
    You have to set `force` to true if the name of the database doesn't start with 'validation'.
    Optionally use `with_users` flag to create users and groups.
    """
    logger = logging.getLogger(__name__)

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
    Add new user to database within default user group and with new password.
    If so advised, creates a schema for the user.
    If so advised, adds the user to the ETL group, giving R/W access. Use wisely.

    This is safe to re-run as long as you skip creating users and groups the second time around.
    """
    logger = logging.getLogger(__name__)

    # Find user in the list of pre-defined users or create new user instance with default settings
    for user in config.users:
        if user.name == new_user:
            break
    else:
        info = {"name": new_user, "group": config.default_group}
        if add_user_schema:
            info["schema"] = new_user
        user = etl.config.DataWarehouseUser(info)

    if user.name in ("default", config.owner):
        raise ValueError("Illegal user name '%s'" % user.name)

    with closing(etl.pg.connection(config.dsn_admin_on_etl_db)) as conn:
        with conn:
            if not skip_user_creation:
                if dry_run:
                    logger.info("Dry-run: Skipping creating user '%s' in group '%s'", user.name, user.group)
                else:
                    logger.info("Creating user '%s' in group '%s'", user.name, user.group)
                    etl.pg.create_user(conn, user.name, user.group)
            if group is not None:
                if group not in config.groups:
                    raise ValueError("Specified group ('%s') not present in DataWarehouseConfig" % group)
                if dry_run:
                    logger.info("Dry-run: Skipping adding user '%s' to group '%s'", user.name, group)
                else:
                    logger.info("Adding user '%s' to group '%s'", user.name, group)
                    etl.pg.alter_group_add_user(conn, group, user.name)
            if add_user_schema:
                user_schema = etl.config.DataWarehouseSchema({"name": user.schema,
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
                logger.info("Dry-run: Skipping setting search path for user '%s' to: %s",
                            user.name, ", ".join(search_path))
            else:
                logger.info("Setting search path for user '%s' to: %s", user.name, ", ".join(search_path))
                etl.pg.alter_search_path(conn, user.name, search_path)


def ping(dsn):
    """
    Send a test query to the data warehouse
    """
    with closing(etl.pg.connection(dsn)) as conn:
        if etl.pg.ping(conn):
            print("{} is alive".format(etl.pg.dbname(conn)))
