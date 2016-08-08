#! /usr/bin/env python3

"""
Thin wrapper around PostgreSQL API, adding niceties like logging,
transaction handling, simple DSN strings, as well as simple commands
(for new users, schema creation, etc.)
"""

from contextlib import contextmanager
import logging
import os
import re
import textwrap

import psycopg2
import psycopg2.extras

from etl.monitor import Timer


def connection(dsn_string, application_name=psycopg2.__name__, autocommit=False, readonly=False):
    """
    Open a connection to the database described by dsn_string which needs to
    be of the format "postgres://user:password@host:port/database"

    By default, this turns on autocommit on the connection.
    """
    logger = logging.getLogger(__name__)
    dsn_values = parse_connection_string(dsn_string)
    dsn_values["application_name"] = application_name

    logger.info("Connecting to: host={host} port={port} database={database} "
                "user={user} password=***".format(**dsn_values))
    cx = psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **dsn_values)
    cx.set_session(autocommit=autocommit, readonly=readonly)
    logger.debug("Connected successfully (backend pid: %d, server version: %s, is_superuser: %s)",
                 cx.get_backend_pid(), cx.server_version, cx.get_parameter_status("is_superuser"))
    return cx


def parse_connection_string(dsn: str) -> dict:
    """
    Extract connection value from JDBC-style connection string.
    """
    # Some people, when confronted with a problem, think "I know, I'll use regular expressions."
    # Now they have two problems.
    dsn_re = re.compile(r"""(?:jdbc:)?(redshift|postgresql|postgres)://  # be nice and accept either connection type
                            (?P<user>\w+)(?::(?P<password>[-\w]+))?@  # user information with optional password
                            (?P<host>[-\w.]+)(:?:(?P<port>\d+))?/  # host and optional port information
                            (?P<database>\w+)  # database (and not dbname)
                            (?:\?sslmode=(?P<sslmode>\w+))?$""",  # sslmode is the only option
                        re.VERBOSE)
    match = dsn_re.match(os.path.expandvars(dsn))
    if match is None:
        raise ValueError("Value of connection string does not conform to expected format.")
    values = match.groupdict()
    # Remove optional key/value pairs
    if values['sslmode'] is None:
        del values['sslmode']
    return values


def dbname(cx):
    """
    Return name of database that connection points to
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
        logger.debug("QUERY STATUS: %s (%.1fs)", cursor.statusmessage, timer.elapsed)
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


def create_group(cx, group):
    execute(cx, """CREATE GROUP "{}" """.format(group))


def create_user(cx, user, password, group):
    execute(cx, """CREATE USER {} IN GROUP "{}" PASSWORD %s""".format(user, group), (password,))


def alter_group_add_user(cx, group, user):
    execute(cx, """ALTER GROUP {} ADD USER "{}" """.format(group, user))


def create_schema(cx, schema, owner):
    execute(cx, """CREATE SCHEMA IF NOT EXISTS "{}" AUTHORIZATION "{}" """.format(schema, owner))


def grant_usage(cx, schema, group):
    execute(cx, """GRANT USAGE ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group))


def grant_all_on_schema(cx, schema, group):
    execute(cx, """GRANT ALL PRIVILEGES ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group))


def grant_select(cx, schema, table, group):
    execute(cx, """GRANT SELECT ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def grant_all(cx, schema, table, group):
    execute(cx, """GRANT ALL PRIVILEGES ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def alter_table_owner(cx, schema, table, owner):
    execute(cx, """ALTER TABLE "{}"."{}" OWNER TO {} """.format(schema, table, owner))


def alter_search_path(cx, user, schemas):
    execute(cx, """ALTER USER {} SET SEARCH_PATH = {}""".format(user, ', '.join(schemas)))


def set_search_path(cx, schemas):
    execute(cx, """SET SEARCH_PATH = {}""".format(', '.join(schemas)))


def log_sql_error(exc):
    """
    Send information from psycopg2.Error instance to logfile.

    See PostgreSQL documentation at http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
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
        print('Hint: Try "postgres://${USER}@localhost:5432/${USER}" as dsn_string')
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG)
    with log_error():
        with connection(sys.argv[1], readonly=True) as conn:
            if ping(conn):
                print("Connection is valid")
            for row in query(conn, "SELECT now() AS local_server_time"):
                for k, v in row.items():
                    print("{} = {}".format(k, v))
