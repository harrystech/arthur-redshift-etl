#! /usr/bin/env python3

"""
Thin wrapper around PostgreSQL API, adding niceties like logging,
transaction handling, simple DSN strings, as well as simple commands
(for new users, schema creation, etc.)

For a description of the connection string, take inspiration from:
https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
"""

from contextlib import contextmanager
import logging
import os
import re
import textwrap

import psycopg2
import psycopg2.extras
import pgpasslib

from etl.timer import Timer


def parse_connection_string(dsn: str) -> dict:
    """
    Extract connection value from JDBC-style connection string.

    The fields "user", "host", "database" will be set.
    The fields "password", "port" and "sslmode" may be set.

    >>> dsn = parse_connection_string("postgresql://john_doe:secret@pg.example.com/xyzzy")
    >>> unparse_connection(dsn)
    'host=pg.example.com dbname=xyzzy user=john_doe password=***'
    """
    # Some people, when confronted with a problem, think "I know, I'll use regular expressions."
    # Now they have two problems.
    dsn_re = re.compile(r"""(?:jdbc:)?(redshift|postgresql|postgres)://  # be nice and accept either connection type
                            (?P<user>\w+)(?::(?P<password>[-\w]+))?@  # user information with optional password
                            (?P<host>[-\w.]+)(:?:(?P<port>\d+))?/  # host and optional port information
                            (?P<database>\w+)  # database (and not dbname)
                            (?:\?sslmode=(?P<sslmode>\w+))?$""",  # sslmode is the only option currently supported
                        re.VERBOSE)
    match = dsn_re.match(os.path.expandvars(dsn))
    if match is None:
        raise ValueError("Value of connection string does not conform to expected format.")
    values = match.groupdict()
    # Remove optional key/value pairs
    for key in ["password", "port", "sslmode"]:
        if values[key] is None:
            del values[key]
    return values


def unparse_connection(dsn: dict) -> str:
    """
    Return connection string for pretty printing or copying when starting psql
    """
    if "port" in dsn:
        return "host={host} port={port} dbname={database} user={user} password=***".format(**dsn)
    else:
        return "host={host} dbname={database} user={user} password=***".format(**dsn)


def connection(dsn_dict, application_name=psycopg2.__name__, autocommit=False, readonly=False):
    """
    Open a connection to the database described by dsn_string which needs to
    be of the format "postgresql://user:password@host:port/database"

    By default, this turns on autocommit on the connection.
    """
    logger = logging.getLogger(__name__)
    dsn_values = dict(dsn_dict)  # so as to not mutate the argument
    dsn_values["application_name"] = application_name
    logger.info("Connecting to: %s", unparse_connection(dsn_values))
    cx = psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **dsn_values)
    cx.set_session(autocommit=autocommit, readonly=readonly)
    logger.debug("Connected successfully (backend pid: %d, server version: %s, is_superuser: %s)",
                 cx.get_backend_pid(), cx.server_version, cx.get_parameter_status("is_superuser"))
    return cx


def extract_dsn(dsn_dict):
    """
    Break the connection string into a JDBC URL and connection properties.

    This is necessary since a JDBC URL may not contain all the properties needed
    to successfully connect, e.g. username, password.  These properties must
    be passed in separately.
    """
    dsn_properties = dict(dsn_dict)  # so as to not mutate the argument
    dsn_properties.update({
        "ApplicationName": __name__,
        "readOnly": "true",
        "driver": "org.postgresql.Driver"  # necessary, weirdly enough
        # FIXME can ssl.props be done here?
    })
    if "port" in dsn_properties:
        jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(**dsn_properties)
    else:
        jdbc_url = "jdbc:postgresql://{host}/{database}".format(**dsn_properties)
    return jdbc_url, dsn_properties


def dbname(cx):
    """
    Return name of database that this connection points to.
    """
    dsn = dict(kv.split('=') for kv in cx.dsn.split(" "))
    return dsn["dbname"]


def remove_credentials(s):
    """
    Remove the AWS credentials information from a query string.

    >>> s = '''copy listing from 's3://mybucket/data/listing/' credentials 'aws_access_key_id=...';'''
    >>> remove_credentials(s)
    "copy listing from 's3://mybucket/data/listing/' credentials '';"
    >>> s = '''COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS 'aws_iam_role=...';'''
    >>> remove_credentials(s)
    "COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS '';"
    >>> s = '''CREATE USER dw_user IN GROUP etl PASSWORD 'horse_staple_battery';'''
    >>> remove_credentials(s)
    "CREATE USER dw_user IN GROUP etl PASSWORD '';"
    """
    match = re.search("(CREDENTIALS|PASSWORD)\s*'([^']*)'", s, re.IGNORECASE)
    if match:
        start, end = match.span()
        creds = match.groups()[0]
        s = s[:start] + creds + " ''" + s[end:]
    return s


def query(cx, stmt, args=()):
    """
    Send query stmt to connection (with parameters) and return rows.
    """
    return execute(cx, stmt, args, return_result=True)


def execute(cx, stmt, args=(), return_result=False):
    """
    Execute query in 'stmt' over connection 'cx' (with parameters in 'args').

    Be careful with query statements that have a '%' in them (say for LIKE)
    since this will interfere with psycopg2 interpreting parameters.

    Printing the query will not print AWS credentials IF the string used matches "CREDENTIALS '[^']*'"
    So be careful or you'll end up sending your credentials to the logfile.
    """
    logger = logging.getLogger(__name__)
    with cx.cursor() as cursor:
        stmt = textwrap.dedent(stmt).strip('\n')  # clean-up whitespace from queries embedded in code
        if len(args):
            printable_stmt = cursor.mogrify(stmt, args)
        else:
            printable_stmt = cursor.mogrify(stmt)
        logger.debug("QUERY: %s\n;", remove_credentials(printable_stmt.decode()))
        # stmt += '\n'
        with Timer() as timer:
            if len(args):
                cursor.execute(stmt, args)
            else:
                cursor.execute(stmt)
        logger.debug("QUERY STATUS: %s (%s)", cursor.statusmessage, timer)
        if cx.notices and logger.isEnabledFor(logging.DEBUG):
            for msg in cx.notices:
                logger.debug("QUERY " + msg.rstrip('\n'))
            del cx.notices[:]
        if return_result:
            return cursor.fetchall()


def ping(cx):
    """
    Give me a ping to the database, Vasili. One ping only, please.
    Return true if connection appears to be alive.
    """
    is_alive = False
    try:
        result = query(cx, "SELECT 1 AS connection_test")
        if len(result) == 1 and "connection_test" in result[0]:
            is_alive = cx.closed == 0
    except psycopg2.OperationalError:
        return False
    else:
        return is_alive


def drop_and_create_database(cx, database):
    exists = query(cx, """SELECT 1 FROM pg_database WHERE datname = '{}'""".format(database))
    if exists:
        execute(cx, """DROP DATABASE {}""".format(database))
    execute(cx, """CREATE DATABASE {}""".format(database))


def create_group(cx, group):
    execute(cx, """CREATE GROUP "{}" """.format(group))


def create_user(cx, user, group):
    dsn_complete = dict(kv.split('=') for kv in cx.dsn.split(" "))
    dsn_partial = {key: dsn_complete[key] for key in ["host", "port", "dbname"]}
    password = pgpasslib.getpass(user=user, **dsn_partial)
    if password is None:
        raise RuntimeError("Password missing from PGPASSFILE for {}".format(user))
    execute(cx, """CREATE USER {} IN GROUP "{}" PASSWORD %s""".format(user, group), (password,))


def alter_group_add_user(cx, group, user):
    execute(cx, """ALTER GROUP {} ADD USER "{}" """.format(group, user))


def schema_exists(cx, name):
    return bool(query(cx, """SELECT 1 FROM pg_namespace WHERE nspname = %s""", (name,)))


def alter_schema_rename(cx, old_name, new_name):
    """
    Renames old_name to new_name if schema old_name exists and new_name doesn't
    """
    if schema_exists(cx, new_name):
        raise RuntimeError("There is already a schema at the target name {}".format(new_name))

    if schema_exists(cx, old_name):
        execute(cx, """ALTER SCHEMA {} RENAME TO "{}" """.format(old_name, new_name))


def create_schema(cx, schema, owner=None):
    if owner:
        execute(cx, """CREATE SCHEMA IF NOT EXISTS "{}" AUTHORIZATION "{}" """.format(schema, owner))
    else:
        execute(cx, """CREATE SCHEMA IF NOT EXISTS "{}" """.format(schema))


def grant_usage(cx, schema, group):
    execute(cx, """GRANT USAGE ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group))


def grant_all_on_schema_to_user(cx, schema, user):
    execute(cx, """GRANT ALL PRIVILEGES ON SCHEMA "{}" TO "{}" """.format(schema, user))


def revoke_usage(cx, schema, group):
    execute(cx, """REVOKE USAGE ON SCHEMA "{}" FROM GROUP "{}" """.format(schema, group))


def grant_select(cx, schema, table, group):
    execute(cx, """GRANT SELECT ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def grant_select_and_write(cx, schema, table, group):
    execute(cx, """GRANT SELECT, INSERT, UPDATE, DELETE ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def grant_all_to_user(cx, schema, table, user):
    execute(cx, """GRANT ALL PRIVILEGES ON "{}"."{}" TO "{}" """.format(schema, table, user))


def revoke_select(cx, schema, table, group):
    execute(cx, """REVOKE SELECT ON "{}"."{}" FROM GROUP "{}" """.format(schema, table, group))


def alter_table_owner(cx, schema, table, owner):
    execute(cx, """ALTER TABLE "{}"."{}" OWNER TO {} """.format(schema, table, owner))


def alter_search_path(cx, user, schemas):
    execute(cx, """ALTER USER {} SET SEARCH_PATH TO {}""".format(user, ', '.join(schemas)))


def set_search_path(cx, schemas):
    execute(cx, """SET SEARCH_PATH = {}""".format(', '.join(schemas)))


def log_sql_error(exc):
    """
    Send information from psycopg2.Error instance to logfile.

    See PostgreSQL documentation at
    http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
    and psycopg2 documentation at http://initd.org/psycopg/docs/extensions.html
    """
    logger = logging.getLogger(__name__)
    if exc.pgcode is not None:
        logger.error('SQL ERROR "%s" %s', exc.pgcode, str(exc.pgerror).strip())
    for name in ('severity',
                 'sqlstate',
                 'message_primary',
                 'message_detail',
                 'message_hint',
                 'statement_position',
                 'internal_position',
                 'internal_query',
                 'context',
                 'schema_name',
                 'table_name',
                 'column_name',
                 'datatype_name',
                 'constraint_name',
                 # 'source_file',
                 # 'source_function',
                 # 'source_line',
                 ):
        value = getattr(exc.diag, name)
        if value:
            logger.debug("DIAG %s: %s", name.upper(), value)


@contextmanager
def log_error():
    """Log any psycopg2 errors using the pretty log_sql_error function before re-raising the exception"""
    try:
        yield
    except psycopg2.Error as exc:
        log_sql_error(exc)
        raise


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: {} dsn_string".format(sys.argv[0]))
        print('Hint: Try "postgresql://${USER}@localhost:5432/${USER}" as dsn_string')
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG)
    with log_error():
        with connection(sys.argv[1], readonly=True) as conn:
            if ping(conn):
                print("Connection is valid")
            for row in query(conn, "SELECT now() AS local_server_time"):
                for k, v in row.items():
                    print("{} = {}".format(k, v))
