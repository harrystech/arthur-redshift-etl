"""
Low-level database routines.

This is a thin wrapper around Psycopg2 for database access, adding niceties like logging,
transaction handling, simple DSN strings, as well as simple commands
(for new users, schema creation, etc.)

For a description of the connection string, take inspiration from:
https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING
"""

import hashlib
import inspect
import logging
import os
import os.path
import re
from contextlib import closing, contextmanager
from typing import Dict, Iterable, List, Optional
from urllib.parse import unquote

import pgpasslib
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.pool
from psycopg2.extensions import connection as Connection  # only used for typing

import etl.text
from etl.errors import ETLRuntimeError
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Allow Ctrl-C to interrupt a query. See https://www.psycopg.org/docs/faq.html#faq-interrupt-query
psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)


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
    dsn_re = re.compile(
        r"""(?:jdbc:)?(?P<subprotocol>redshift|postgresql|postgres)://  # accept either type
            (?:(?P<user>[-\w.%]+)(?::(?P<password>[-\w.%]+))?@)?  # optional user with password
            (?P<host>[-\w.%]+)(:?:(?P<port>\d+))?/  # host and optional port information
            (?P<database>[-\w.%]+)  # database (and not dbname)
            (?:\?sslmode=(?P<sslmode>\w+))?$""",  # sslmode is the only option currently supported
        re.ASCII | re.VERBOSE,
    )
    dsn_after_expansion = os.path.expandvars(dsn)  # Supports stuff like $USER
    match = dsn_re.match(dsn_after_expansion)
    if match is None:
        raise ValueError("value of connection string does not conform to expected format.")
    values = match.groupdict()
    return {key: unquote(values[key], errors="strict") for key in values if values[key] is not None}


def unparse_connection(dsn: Dict[str, str]) -> str:
    """Return connection string for pretty printing or copying when starting psql."""
    values = dict(dsn)
    for key in ("user", "port"):
        if key not in values:
            values[key] = "<default>"
    return "host={host} port={port} dbname={database} user={user} password=***".format_map(values)


def _dsn_connection_values(dsn_dict: Dict[str, str], application_name: str) -> dict:
    """
    Return a dictionary of parameters that can be used to open a db connection with psycopg2.

    This includes popping "subprotocol" from our dictionary of parameters extracted
    from the connection string, which is not expected by psycopg2.connect().

    We also set some basic connection parameters here, including connection timeout and
    keepalives settings.
    """
    # See https://www.postgresql.org/docs/10/libpq-connect.html for the keepalive* args.
    # and https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-firewall-guidance.html
    dsn_values = {
        "application_name": application_name,
        "connect_timeout": 30,
        "cursor_factory": psycopg2.extras.DictCursor,
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 60,
    }
    dsn_values.update(dsn_dict)
    dsn_values.pop("subprotocol", None)
    return dsn_values


def connection(dsn_dict: Dict[str, str], application_name=psycopg2.__name__, autocommit=False, readonly=False):
    """
    Open a connection to the database described by dsn_string.

    The dsn_string is expected to look something like
    "postgresql://user:password@host:port/database" (see parse_connection_string).

    Caveat Emptor: By default, this turns off autocommit on the connection. This means that you
    have to explicitly commit on the connection object or run your SQL within a transaction context!
    """
    dsn_values = _dsn_connection_values(dsn_dict, application_name)
    logger.info("Connecting to: %s", unparse_connection(dsn_values))
    cx = psycopg2.connect(**dsn_values)
    cx.set_session(autocommit=autocommit, readonly=readonly)
    logger.debug(
        "Connected successfully (backend pid: %d, server version: %s, is_superuser: %s)",
        cx.get_backend_pid(),
        cx.server_version,
        cx.get_parameter_status("is_superuser"),
    )
    return cx


def connection_pool(max_conn, dsn_dict: Dict[str, str], application_name=psycopg2.__name__):
    """
    Create a connection pool with up to max_conn connections.

    All connections will use the given connection string.
    """
    dsn_values = _dsn_connection_values(dsn_dict, application_name)
    return psycopg2.pool.ThreadedConnectionPool(1, max_conn, **dsn_values)


def extract_dsn(dsn_dict: Dict[str, str], read_only=False):
    """
    Break the connection string into a JDBC URL and connection properties.

    This is necessary since a JDBC URL may not contain all the properties needed
    to successfully connect, e.g. username, password.  These properties must
    be passed in separately.

    Use the postgresql subprotocol and driver regardless of whether the connection
    string's protocol was postgres or redshift.
    """
    dsn_properties = dict(dsn_dict)  # so as to not mutate the argument
    dsn_properties.update(
        {"ApplicationName": __name__, "readOnly": "true" if read_only else "false", "driver": "org.postgresql.Driver"}
    )
    if "port" in dsn_properties:
        jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format_map(dsn_properties)
    else:
        jdbc_url = "jdbc:postgresql://{host}/{database}".format_map(dsn_properties)
    return jdbc_url, dsn_properties


def remove_password(line):
    """
    Remove any password or credentials information from a query string.

    >>> s = '''CREATE USER dw_user IN GROUP etl PASSWORD 'horse_staple_battery';'''
    >>> remove_password(s)
    "CREATE USER dw_user IN GROUP etl PASSWORD '';"
    >>> s = '''copy listing from 's3://mybucket/data/tbl/' credentials 'aws_access_key_id=...';'''
    >>> remove_password(s)
    "copy listing from 's3://mybucket/data/tbl/' credentials '';"
    >>> s = '''COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS 'aws_iam_role=...';'''
    >>> remove_password(s)
    "COPY LISTING FROM 's3://mybucket/data/listing/' CREDENTIALS '';"
    """
    match = re.search(r"(CREDENTIALS|PASSWORD)\s*'([^']*)'", line, re.IGNORECASE)
    if match:
        start, end = match.span()
        creds = match.groups()[0]
        line = line[:start] + creds + " ''" + line[end:]
    return line


def mogrify(cursor, stmt, args=()):
    """Build the statement by filling in the arguments and cleaning up whitespace along the way."""
    stripped = etl.text.whitespace_cleanup(stmt)
    if len(args):
        actual_stmt = cursor.mogrify(stripped, args)
    else:
        actual_stmt = cursor.mogrify(stripped)
    return actual_stmt


def query(cx, stmt, args=()):
    """Send query stmt to connection (with parameters) and return rows."""
    return execute(cx, stmt, args, return_result=True)


def execute(cx, stmt, args=(), return_result=False):
    """
    Execute query in 'stmt' over connection 'cx' (with parameters in 'args').

    Be careful with query statements that have a '%' in them (say for LIKE)
    since this will interfere with psycopg2 interpreting parameters.

    Printing the query will not print AWS credentials IF the string used
    matches "CREDENTIALS '[^']*'".
    So be careful or you'll end up sending your credentials to the logfile.
    """
    with cx.cursor() as cursor:
        executable_statement = mogrify(cursor, stmt, args)
        printable_stmt = remove_password(executable_statement.decode())
        logger.debug("QUERY:\n%s\n;", printable_stmt)
        with Timer() as timer:
            cursor.execute(executable_statement)
        if cursor.rowcount is not None and cursor.rowcount > 0:
            logger.debug("QUERY STATUS: %s [rowcount=%d] (%s)", cursor.statusmessage, cursor.rowcount, timer)
        else:
            logger.debug("QUERY STATUS: %s (%s)", cursor.statusmessage, timer)
        if cx.notices and logger.isEnabledFor(logging.DEBUG):
            for msg in cx.notices:
                logger.debug("QUERY " + msg.rstrip("\n"))
            del cx.notices[:]
        if return_result:
            return cursor.fetchall()


def skip_query(cx, stmt, args=()):
    """For logging side-effect only ... show which query would have been executed."""
    with cx.cursor() as cursor:
        executable_statement = mogrify(cursor, stmt, args)
        printable_stmt = remove_password(executable_statement.decode())
        logger.debug("Skipped QUERY:\n%s\n;", printable_stmt)


def run(cx, message, stmt, args=(), return_result=False, dry_run=False):
    """
    Execute the query and log the message around it.

    Or just show what would have been run in dry-run mode.
    This will try to use the caller's logger.
    """
    # Figure out caller for better logging
    current_frame = inspect.currentframe()
    caller_globals = current_frame.f_back.f_globals
    caller_logger = caller_globals.get("logger", logger)
    assert isinstance(caller_logger, logging.Logger)

    if dry_run:
        caller_logger.info("Dry-run: Skipping {}{}".format(message[:1].lower(), message[1:]))
        skip_query(cx, stmt, args=args)
    else:
        caller_logger.info("{}".format(message))
        return execute(cx, stmt, args=args, return_result=return_result)


def print_result(title, dict_rows) -> None:
    """Print query result."""
    print(title)
    if dict_rows:
        keys = list(dict_rows[0].keys())
    else:
        keys = []
    print(etl.text.format_lines([[row[key] for key in keys] for row in dict_rows], header_row=keys))


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
    """Send a test query to our connection."""
    is_alive = False
    try:
        result = run(cx, f"Ping '{cx.info.dbname}'!", "SELECT 1 AS connection_test", return_result=True)
        if len(result) == 1 and "connection_test" in result[0]:
            is_alive = cx.closed == 0
    except psycopg2.OperationalError:
        return False
    else:
        return is_alive


def ping(dsn):
    """Give me a ping to the database, Vasili; one ping only, please."""
    with closing(connection(dsn, readonly=True)) as cx:
        if test_connection(cx):
            print(f"{cx.info.dbname} is alive")


def run_statement_with_args(dsn, stmt: str, args: Optional[dict] = None, readonly=True):
    """Run a single statement (possibly with args) and return query results."""
    # TODO(tom): Switch to named args for all queries.
    with closing(connection(dsn, autocommit=True, readonly=readonly)) as cx:
        with log_error():
            return query(cx, stmt, args)


def log_sql_error(exc):
    """
    Send information from psycopg2.Error instance to logfile.

    See PostgreSQL documentation at
    https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
    and psycopg2 documentation at https://www.psycopg.org/docs/extensions.html
    """
    if exc.pgcode is not None:
        logger.error('SQL ERROR "%s" %s', exc.pgcode, str(exc.pgerror).strip())
    for name in (
        "severity",
        "sqlstate",
        "message_primary",
        "message_detail",
        "message_hint",
        "statement_position",
        "internal_position",
        "internal_query",
        "context",
        "schema_name",
        "table_name",
        "column_name",
        "datatype_name",
        "constraint_name",
        # 'source_file',
        # 'source_function',
        # 'source_line',
    ):
        value = getattr(exc.diag, name, None)
        if value:
            logger.debug("DIAG %s: %s", name.upper(), value)


@contextmanager
def log_error():
    """Log any psycopg2 errors using the log_sql_error function before re-raising the exception."""
    try:
        yield
    except psycopg2.Error as exc:
        log_sql_error(exc)
        raise


# ---- DATABASE ----


def drop_and_create_database(cx: Connection, database: str, owner: str) -> None:
    exists = query(cx, f"SELECT 1 AS check_database FROM pg_catalog.pg_database WHERE datname = '{database}'")
    if exists:
        execute(cx, """DROP DATABASE {}""".format(database))
    execute(cx, """CREATE DATABASE {} WITH OWNER {}""".format(database, owner))


# ---- USERS and GROUPS ----


def create_group(cx: Connection, group: str) -> None:
    execute(cx, """CREATE GROUP "{}" """.format(group))


def group_exists(cx: Connection, group: str) -> bool:
    rows = query(
        cx,
        """
        SELECT groname
          FROM pg_catalog.pg_group
         WHERE groname = %s
        """,
        (group,),
    )
    return len(rows) > 0


def _get_encrypted_password(cx, user) -> Optional[str]:
    """Return MD5-hashed password if entry is found in PGPASSLIB or None otherwise."""
    dsn_complete = dict(kv.split("=") for kv in cx.dsn.split(" "))
    dsn_partial = {key: dsn_complete[key] for key in ["host", "port", "dbname"]}
    dsn_user = dict(dsn_partial, user=user)
    try:
        password = pgpasslib.getpass(**dsn_user)
    except pgpasslib.FileNotFound as exc:
        logger.info("Create the file using 'touch ~/.pgpass && chmod go= ~/.pgpass'")
        raise ETLRuntimeError("PGPASSFILE file is missing") from exc
    except pgpasslib.InvalidPermissions as exc:
        logger.info("Update the permissions using: 'chmod go= ~/.pgpass'")
        raise ETLRuntimeError("PGPASSFILE file has invalid permissions") from exc

    if password is None:
        return None
    md5 = hashlib.md5()
    md5.update((password + user).encode())
    return "md5" + md5.hexdigest()


def create_user(cx, user, group):
    password = _get_encrypted_password(cx, user)
    if password is None:
        logger.warning("Missing entry in PGPASSFILE file for '%s'", user)
        raise ETLRuntimeError("password missing from PGPASSFILE for user '{}'".format(user))
    execute(cx, """CREATE USER "{}" IN GROUP "{}" PASSWORD %s""".format(user, group), (password,))


def alter_password(cx, user, ignore_missing_password=False):
    password = _get_encrypted_password(cx, user)
    if password is None:
        logger.warning("Failed to find password in PGPASSFILE for '%s'", user)
        if not ignore_missing_password:
            raise ETLRuntimeError("password missing from PGPASSFILE for user '{}'".format(user))
        return
    execute(cx, """ALTER USER "{}" PASSWORD %s""".format(user), (password,))


def alter_group_add_user(cx, group, user):
    execute(cx, """ALTER GROUP "{}" ADD USER "{}" """.format(group, user))


def alter_search_path(cx, user, schemas):
    execute(cx, """ALTER USER "{}" SET SEARCH_PATH TO {}""".format(user, ", ".join(schemas)))


def user_exists(cx, user) -> bool:
    rows = query(
        cx,
        """
        SELECT usename
          FROM pg_catalog.pg_user
         WHERE usename = %s
        """,
        (user,),
    )
    return len(rows) > 0


# ---- SCHEMAS ----


def select_schemas(cx: Connection, names: Iterable[str]) -> List[str]:
    """Return the subset of the schema names that actually exist in the database."""
    # Make sure to evaluate the names arg only once.
    names = tuple(names)
    rows = query(
        cx,
        """
        SELECT nspname AS name
          FROM pg_catalog.pg_namespace
         WHERE nspname IN %s
        """,
        (names,),
    )
    found = frozenset(row[0] for row in rows)
    # Instead of an ORDER BY clause, keep original order.
    return [name for name in names if name in found]


def drop_schema(cx: Connection, name: str) -> None:
    execute(cx, f'DROP SCHEMA IF EXISTS "{name}" CASCADE')


def alter_schema_rename(cx: Connection, old_name: str, new_name: str) -> None:
    execute(cx, f'ALTER SCHEMA {old_name} RENAME TO "{new_name}"')


def create_schema(cx: Connection, schema: str, owner: Optional[str] = None) -> None:
    execute(cx, f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    if owner:
        # Because of the "IF NOT EXISTS" we need to expressly set owner in case there's a change
        # in ownership.
        execute(cx, f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"')


def grant_usage(cx: Connection, schema: str, groups: Iterable[str]) -> None:
    quoted_group_list = etl.text.join_with_double_quotes(groups, sep=", GROUP ", prefix="GROUP ")
    execute(cx, f'GRANT USAGE ON SCHEMA "{schema}" TO {quoted_group_list}')


def grant_all_on_schema_to_user(cx: Connection, schema: str, user: str) -> None:
    execute(cx, f'GRANT ALL PRIVILEGES ON SCHEMA "{schema}" TO "{user}"')


def revoke_usage(cx: Connection, schema: str, groups: Iterable[str]) -> None:
    quoted_group_list = etl.text.join_with_double_quotes(groups, sep=", GROUP ", prefix="GROUP ")
    execute(cx, f'REVOKE USAGE ON SCHEMA "{schema}" FROM {quoted_group_list}')


def grant_select_on_all_tables_in_schema(cx: Connection, schema: str, groups: Iterable[str]) -> None:
    quoted_group_list = etl.text.join_with_double_quotes(groups, sep=", GROUP ", prefix="GROUP ")
    execute(cx, f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO {quoted_group_list}')


def grant_select_and_write_on_all_tables_in_schema(cx: Connection, schema: str, groups: Iterable[str]) -> None:
    quoted_group_list = etl.text.join_with_double_quotes(groups, sep=", GROUP ", prefix="GROUP ")
    execute(
        cx,
        f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "{schema}" TO {quoted_group_list}',
    )


def revoke_all_on_all_tables_in_schema(cx: Connection, schema: str, groups: Iterable[str]) -> None:
    quoted_group_list = etl.text.join_with_double_quotes(groups, sep=", GROUP ", prefix="GROUP ")
    execute(cx, f'REVOKE ALL ON ALL TABLES IN SCHEMA "{schema}" FROM {quoted_group_list}')


# ---- TABLES ----


def relation_kind(cx, schema, table) -> Optional[str]:
    """
    Return "kind" of relation, either 'TABLE' or 'VIEW' for relations that actually exist.

    If the relation doesn't exist, None is returned.
    """
    rows = query(
        cx,
        """
        SELECT CASE cls.relkind
                 WHEN 'r' THEN 'TABLE'
                 WHEN 'v' THEN 'VIEW'
               END AS relation_kind
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid
         WHERE nsp.nspname = %s
           AND cls.relname = %s
           AND cls.relkind IN ('r', 'v')
        """,
        (schema, table),
    )
    if rows:
        return rows[0][0]
    else:
        return None


def grant_select(cx, schema, table, group):
    execute(cx, """GRANT SELECT ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def grant_select_and_write(cx, schema, table, group):
    execute(cx, """GRANT SELECT, INSERT, UPDATE, DELETE ON "{}"."{}" TO GROUP "{}" """.format(schema, table, group))


def grant_all_to_user(cx, schema, table, user):
    execute(cx, """GRANT ALL PRIVILEGES ON "{}"."{}" TO "{}" """.format(schema, table, user))


def revoke_select(cx, schema, table, group):
    execute(cx, """REVOKE SELECT ON "{}"."{}" FROM GROUP "{}" """.format(schema, table, group))
