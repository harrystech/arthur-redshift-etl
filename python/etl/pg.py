#! /usr/bin/env python3

"""
Thin wrapper around PostgreSQL API, adding niceties like logging,
transaction handling, simple DSN strings, as well as simple commands
(for new users, schema creation, etc.)

For a description of the connection string, take inspiration from:
https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
"""

import logging
import os
import re
import textwrap
from contextlib import closing, contextmanager
from typing import Dict

import psycopg2
import psycopg2.extras
import pgpasslib

from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def parse_connection_string(dsn: str) -> Dict[str, str]:
    """
    Extract connection value from JDBC-style connection string.

    The fields ""host" and "database" will be set.
    The fields "user", "password", "port" and "sslmode" may be set.

    >>> dsn_min = parse_connection_string("postgres://example.com/xyzzy")
    >>> unparse_connection(dsn_min)
    'host=example.com port=<default> dbname=xyzzy user=<default> password=***'
    >>> dsn_max = parse_connection_string("postgresql://john.doe:secret@pg.example.com:5432/xyzzy")
    >>> unparse_connection(dsn_max)
    'host=pg.example.com port=5432 dbname=xyzzy user=john.doe password=***'
    """
    # Some people, when confronted with a problem, think "I know, I'll use regular expressions."
    # Now they have two problems.
    dsn_re = re.compile(r"""(?:jdbc:)?(redshift|postgresql|postgres)://  # be nice and accept either connection type
                            (?:(?P<user>\w[.\w]*)(?::(?P<password>[-\w]+))?@)?  # optional user with password
                            (?P<host>\w[-.\w]*)(:?:(?P<port>\d+))?/  # host and optional port information
                            (?P<database>\w+)  # database (and not dbname)
                            (?:\?sslmode=(?P<sslmode>\w+))?$""",  # sslmode is the only option currently supported
                        re.VERBOSE)
    dsn_after_expansion = os.path.expandvars(dsn)  # Supports stuff like $USER
    match = dsn_re.match(dsn_after_expansion)
    if match is None:
        raise ValueError("value of connection string does not conform to expected format.")
    values = match.groupdict()
    return {key: values[key] for key in values if values[key] is not None}


def unparse_connection(dsn: Dict[str, str]) -> str:
    """
    Return connection string for pretty printing or copying when starting psql
    """
    values = dict(dsn)
    for key in ("user", "port"):
        if key not in values:
            values[key] = "<default>"
    return "host={host} port={port} dbname={database} user={user} password=***".format(**values)


def connection(dsn_dict: Dict[str, str], application_name=psycopg2.__name__, autocommit=False, readonly=False):
    """
    Open a connection to the database described by dsn_string which looks something like
    "postgresql://user:password@host:port/database" (see parse_connection_string).

    Caveat Emptor: By default, this turns off autocommit on the connection. This means that you
    have to explicitly commit on the connection object or run your SQL within a transaction context!
    """
    dsn_values = dict(dsn_dict)  # so as to not mutate the argument
    dsn_values["application_name"] = application_name
    logger.info("Connecting to: %s", unparse_connection(dsn_values))
    cx = psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **dsn_values)
    cx.set_session(autocommit=autocommit, readonly=readonly)
    logger.debug("Connected successfully (backend pid: %d, server version: %s, is_superuser: %s)",
                 cx.get_backend_pid(), cx.server_version, cx.get_parameter_status("is_superuser"))
    return cx


def extract_dsn(dsn_dict: Dict[str, str]):
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
    with cx.cursor() as cursor:
        stmt = textwrap.dedent(stmt).strip('\n')  # clean-up whitespace from queries embedded in code
        if len(args):
            printable_stmt = cursor.mogrify(stmt, args)
        else:
            printable_stmt = cursor.mogrify(stmt)
        logger.debug("QUERY:\n%s\n;", remove_credentials(printable_stmt.decode()))
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


def explain(cx, stmt, args=()):
    """
    Return explain plan for the query as a list of steps.

    We sometimes use this just to test out a query syntax so we are heavy on the logging.
    """
    rows = execute(cx, "EXPLAIN\n" + stmt, args, return_result=True)
    lines = [row[0] for row in rows]
    logger.debug("Query plan:\n | " + "\n | ".join(lines))
    return lines


def test_connection(cx):
    """
    Send a test query to our connection
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


def ping(dsn):
    """
    Give me a ping to the database, Vasili. One ping only, please.
    """
    with closing(connection(dsn, readonly=True)) as cx:
        if test_connection(cx):
            print("{} is alive".format(dbname(cx)))


def log_sql_error(exc):
    """
    Send information from psycopg2.Error instance to logfile.

    See PostgreSQL documentation at
    http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
    and psycopg2 documentation at http://initd.org/psycopg/docs/extensions.html
    """
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
        value = getattr(exc.diag, name, None)
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


# ---- DATABASE ----

def drop_and_create_database(cx, database, owner):
    exists = query(cx, """SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}'""".format(database))
    if exists:
        execute(cx, """DROP DATABASE {}""".format(database))
    execute(cx, """CREATE DATABASE {} WITH OWNER {}""".format(database, owner))


# ---- USERS and GROUPS ----

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


def alter_search_path(cx, user, schemas):
    execute(cx, """ALTER USER {} SET SEARCH_PATH TO {}""".format(user, ', '.join(schemas)))


def set_search_path(cx, schemas):
    execute(cx, """SET SEARCH_PATH = {}""".format(', '.join(schemas)))


def list_connections(cx):
    return query(cx, """SELECT datname, procpid, usesysid, usename FROM pg_stat_activity""")


def list_transactions(cx):
    return query(cx, """SELECT  t.*,  c.relname FROM svv_transactions t
                        JOIN pg_catalog.pg_class c    ON t.relation = c.OID""")


# ---- SCHEMAS ----


def select_schemas(cx, names):
    rows = query(cx, """
        SELECT nspname AS name
          FROM pg_catalog.pg_namespace
         WHERE nspname IN %s
        """, (tuple(names),))
    return frozenset(row[0] for row in rows)


def drop_schema(cx, name):
    execute(cx, """DROP SCHEMA IF EXISTS "{}" CASCADE""".format(name))


def alter_schema_rename(cx, old_name, new_name):
    execute(cx, """ALTER SCHEMA {} RENAME TO "{}" """.format(old_name, new_name))


def create_schema(cx, schema, owner=None):
    execute(cx, """CREATE SCHEMA IF NOT EXISTS "{}" """.format(schema))
    if owner:
        # Because of the "IF NOT EXISTS" we need to expressly set owner in case there's a change in ownership.
        execute(cx, """ALTER SCHEMA "{}" OWNER TO "{}" """.format(schema, owner))


def grant_usage(cx, schema, group):
    execute(cx, """GRANT USAGE ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group))


def grant_all_on_schema_to_user(cx, schema, user):
    execute(cx, """GRANT ALL PRIVILEGES ON SCHEMA "{}" TO "{}" """.format(schema, user))


def revoke_usage(cx, schema, group):
    execute(cx, """REVOKE USAGE ON SCHEMA "{}" FROM GROUP "{}" """.format(schema, group))


def grant_select_in_schema(cx, schema, group):
    execute(cx, """GRANT SELECT ON ALL TABLES IN SCHEMA "{}" TO GROUP "{}" """.format(schema, group))


def revoke_select_on_all_tables_in_schema(cx, schema, group):
    execute(cx, """REVOKE SELECT ON ALL TABLES IN SCHEMA "{}" FROM GROUP "{}" """.format(schema, group))


# ---- TABLES ----

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


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: {} dsn_string".format(sys.argv[0]))
        print()
        print('Hint: Try your local machine: {} "postgres://${{USER}}@localhost:5432/${{USER}}"'.format(sys.argv[0]))
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG)
    dsn_dict = parse_connection_string(sys.argv[1])
    with log_error():
        ping(dsn_dict)
