#! /usr/bin/env python3

"""
Thin wrapper around PostgreSQL API, adding niceties like logging,
transaction handling, simple DSN strings, as well simple commands
like for schema creation.

Example:
    >>> from etl import pg
    >>> cx = pg.connection("postgres://${USER}@localhost:5432/${USER}")
    >>> rows = pg.query(cx, "select now()")
    >>> len(rows)
    1
"""

from contextlib import contextmanager
from datetime import datetime
import logging
import os
import re
import textwrap

import psycopg2
import psycopg2.extras


def connection(dsn_string, application_name=psycopg2.__name__, readonly=False):
    """
    Open a connection to the database described by dsn_string which needs to
    be of the format "postgres://user:password@host:port/database"
    """
    # Extract connection value from jdbc-style connection string. (Some people, when confronted with a problem,
    # think "I know, I'll use regular expressions." Now they have two problems.)
    dsn_re = re.compile(r"""postgres(?:ql)?://  # be nice and accept either connection type
                            (?P<user>\w+)(?::(?P<password>[-\w]+))?@  # user information with optional password
                            (?P<host>[-\w.]+)(:?:(?P<port>\d+))?/  # host and optional port information
                            (?P<database>\w+)  # database (and not dbname)
                            (?:\?sslmode=(?P<sslmode>\w+))?""",  # sslmode is the only option
                        re.VERBOSE)

    dsn_match = dsn_re.match(os.path.expandvars(dsn_string))
    if dsn_match is None:
        raise ValueError("Value of connection string does not conform to expected format.")
    dsn_values = dsn_match.groupdict()
    dsn_values["application_name"] = application_name

    logging.getLogger(__name__).info("Connecting to: host={host} port={port} database={database} "
                                     "user={user} password=***".format(**dsn_values))
    cx = psycopg2.connect(cursor_factory=psycopg2.extras.DictCursor, **dsn_values)
    # Avoid having a new transaction with every command by setting autocommit to True.
    cx.set_session(readonly=readonly, autocommit=True)
    return cx


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
    >>> s = '''COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS 'aws_access_key_id=...';'''
    >>> remove_credentials(s)
    "COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS '';"
    >>> s = '''CREATE USER dw_user IN GROUP etl PASSWORD 'horse_staple_battery';'''
    """
    match = re.search("(CREDENTIALS|PASSWORD)\s*'([^']*)'", s, re.IGNORECASE)
    if match:
        start, end = match.span()
        creds = match.groups()[0]
        s = s[:start] + creds + " ''" + s[end:]
    return s


def _log_stmt(cursor, stmt, args, debug=False):
    stmt = textwrap.dedent(stmt)  # clean-up whitespace from queries embedded in code
    if debug:
        if len(args):
            printable_stmt = cursor.mogrify(stmt, args)
        else:
            printable_stmt = cursor.mogrify(stmt)
        logging.getLogger(__name__).debug("QUERY: %s;", remove_credentials(printable_stmt.decode()))
    return stmt


def _seconds_since(start_time):
    return (datetime.now() - start_time).total_seconds()


def query(cx, stmt, args=(), debug=False):
    """
    Send query stmt to connection (with parameters) and return rows.

    If debug is True, then the statement is sent to the log as well.
    """
    return execute(cx, stmt, args, debug, return_result=True)


def execute(cx, stmt, args=(), debug=False, return_result=False):
    """
    Execute query in 'stmt' over connection 'cx' (with parameters in 'args').

    Be careful with query statements that have a '%' in them (say for LIKE)
    since this will interfere with psycopg2 interpreting parameters.

    Printing the query will not print AWS credentials IF the string used matches "CREDENTIALS '[^']*'"
    So be careful or you'll end up sending your credentials to the logfile.

    If debug is True, then the statement is sent to the log as well.
    """
    with cx.cursor() as cursor:
        stmt = _log_stmt(cursor, stmt, args, debug)
        start_time = datetime.now()
        if len(args):
            cursor.execute(stmt, args)
        else:
            cursor.execute(stmt)
        logging.getLogger(__name__).debug("STATUS: %s (%.1fs)", cursor.statusmessage, _seconds_since(start_time))
        if return_result:
            return cursor.fetchall()


def create_group(cx, group, debug=False):
    execute(cx, """CREATE GROUP "{}" """.format(group), debug=debug)


def create_user(cx, user, password, group, debug=False):
    execute(cx, """CREATE USER {} IN GROUP "{}" PASSWORD %s""".format(user, group), (password,), debug=debug)


def create_schema(cx, schema, owner, debug=False):
    execute(cx, """CREATE SCHEMA "{}" AUTHORIZATION "{}" """.format(schema, owner), debug=debug)


def grant_usage(cx, schema, group, debug=False):
    execute(cx, """GRANT USAGE ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group), debug=debug)


def grant_all(cx, schema, group, debug=False):
    execute(cx, """GRANT ALL PRIVILEGES ON SCHEMA "{}" TO GROUP "{}" """.format(schema, group), debug=debug)


def grant_select(cx, schema, table, group, debug=False):
    execute(cx, """GRANT SELECT ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group), debug=debug)


def alter_search_path(cx, user, schemas, debug=False):
    execute(cx, """ALTER USER {} SET SEARCH_PATH = {}""".format(user, ', '.join(schemas)), debug=debug)


def set_search_path(cx, schemas, debug=False):
    execute(cx, """SET SEARCH_PATH = {}""".format(', '.join(schemas)), debug=debug)


def log_sql_error(exc, as_warning=False):
    """
    Send information from psycopg2.Error instance to logfile.

    See PostgreSQL documentation at http://www.postgresql.org/docs/current/static/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
    and psycopg2 documentation at http://initd.org/psycopg/docs/extensions.html
    """
    logger = logging.getLogger(__name__)
    if as_warning:
        logger.warning('SQL CODE "%s" ERROR "%s"', exc.pgcode, exc.pgerror)
    else:
        logger.exception('SQL CODE "%s" ERROR "%s"', exc.pgcode, exc.pgerror)
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
def measure_elapsed_time():
    """
    Measure time it takes to execute code and report on success.

    Exceptions are being caught here, reported on but then swallowed.

    Example:
        >>> with measure_elapsed_time():
        ...     pass
    """
    logger = logging.getLogger(__name__)
    start_time = datetime.now()
    try:
        yield
        logger.info("Ran for %.2fs and finished successfully!", _seconds_since(start_time))
    except psycopg2.Error as exc:
        log_sql_error(exc)
        logger.info("Ran for %.2fs before tripping over this error!", _seconds_since(start_time))
    except Exception:
        logger.exception("Something terrible happened")
        logger.info("Ran for %.2fs before encountering disaster!", _seconds_since(start_time))
    except BaseException:
        logger.exception("Something really terrible happened")
        logger.info("Ran for %.2fs before an exceptional termination!", _seconds_since(start_time))


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: {} dsn_string".format(sys.argv[0]))
        print("Hint: Try postgres://${USER}@localhost:5432/${USER} as dsn_string")
        sys.exit(1)

    logging.basicConfig(level=logging.DEBUG)
    with measure_elapsed_time():
        with connection(sys.argv[1], readonly=True) as conn:
            for row in query(conn,
                             # "select application_name from pg_stat_activity where pid = pg_backend_pid();",
                             "SELECT DISTINCT datname, usename FROM pg_stat_activity WHERE procpid = pg_backend_pid()",
                             debug=True):
                for k, v in row.items():
                    print("*** {} = {} ***".format(k, v))
