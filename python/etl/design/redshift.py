"""
This module contains functions to maintain relations in Redshift.

Note that overall we try to have this minimally depend on other data structures -- actually
we use only TableName which is handy to get a qualified and quoted name as needed.
"""

import logging
from contextlib import contextmanager
from itertools import chain
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extensions
from psycopg2.extensions import connection  # only for type annotation

import etl.config
import etl.db
from etl.errors import ETLSystemError, TransientETLError
from etl.names import TableName
from etl.text import join_column_list


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def build_column_description(column: Dict[str, str], skip_identity=False, skip_references=False) -> str:
    """
    Return the description of a table column suitable for a table creation.
    See build_columns.

    >>> build_column_description({"name": "key", "sql_type": "int", "not_null": True, "encoding": "raw"})
    '"key" int ENCODE raw NOT NULL'
    >>> build_column_description({"name": "my_key", "sql_type": "int", "references": ["sch.ble", ["key"]]})
    '"my_key" int REFERENCES "sch"."ble" ( "key" )'
    """
    column_ddl = '"{name}" {sql_type}'.format(**column)
    if column.get("identity", False) and not skip_identity:
        column_ddl += " IDENTITY(1, 1)"
    if "encoding" in column:
        column_ddl += " ENCODE {}".format(column["encoding"])
    if column.get("not_null", False):
        column_ddl += " NOT NULL"
    if "references" in column and not skip_references:
        [table_identifier, [foreign_column]] = column["references"]
        foreign_name = TableName.from_identifier(table_identifier)
        column_ddl += ' REFERENCES {} ( "{}" )'.format(foreign_name, foreign_column)
    return column_ddl


def build_columns(columns: List[dict], is_temp=False) -> List[str]:
    """
    Build up partial DDL for all non-skipped columns.

    For our final table, we skip the IDENTITY (which would mess with our row at key==0) but do add
    references to other tables.
    For temp tables, we use IDENTITY so that the key column is filled in but skip the references.

    >>> build_columns([{"name": "key", "sql_type": "int"}, {"name": "?", "skipped": True}])
    ['"key" int']
    """
    ddl_for_columns = [
        build_column_description(column, skip_identity=not is_temp, skip_references=is_temp)
        for column in columns
        if not column.get("skipped", False)
    ]
    return ddl_for_columns


def build_table_constraints(table_design: dict) -> List[str]:
    """
    Return the constraints from the table design so that they can be inserted into a SQL DDL statement.

    >>> build_table_constraints({})  # no-op
    []
    >>> build_table_constraints({"constraints": [{"primary_key": ["id"]}, {"unique": ["name", "email"]}]})
    ['PRIMARY KEY ( "id" )', 'UNIQUE ( "name", "email" )']
    """
    table_constraints = table_design.get("constraints", [])
    type_lookup = dict(
        [
            ("primary_key", "PRIMARY KEY"),
            ("surrogate_key", "PRIMARY KEY"),
            ("unique", "UNIQUE"),
            ("natural_key", "UNIQUE"),
        ]
    )
    ddl_for_constraints = []
    for constraint in table_constraints:
        [[constraint_type, column_list]] = constraint.items()
        ddl_for_constraints.append("{} ( {} )".format(type_lookup[constraint_type], join_column_list(column_list)))
    return ddl_for_constraints


def build_table_attributes(table_design: dict) -> List[str]:
    """
    Return the attributes from the table design so that they can be inserted into a SQL DDL statement.

    >>> build_table_attributes({})  # no-op
    []
    >>> build_table_attributes({"attributes": {"distribution": "even"}})
    ['DISTSTYLE EVEN']
    >>> build_table_attributes({"attributes": {"distribution": ["key"], "compound_sort": ["name"]}})
    ['DISTSTYLE KEY', 'DISTKEY ( "key" )', 'COMPOUND SORTKEY ( "name" )']
    """
    table_attributes = table_design.get("attributes", {})
    distribution = table_attributes.get("distribution", [])
    compound_sort = table_attributes.get("compound_sort", [])
    interleaved_sort = table_attributes.get("interleaved_sort", [])

    ddl_attributes = []
    # TODO Use for staging tables: ddl_attributes.append("BACKUP NO")
    if distribution:
        if isinstance(distribution, list):
            ddl_attributes.append("DISTSTYLE KEY")
            ddl_attributes.append("DISTKEY ( {} )".format(join_column_list(distribution)))
        else:
            ddl_attributes.append("DISTSTYLE {}".format(distribution.upper()))
    if compound_sort:
        ddl_attributes.append("COMPOUND SORTKEY ( {} )".format(join_column_list(compound_sort)))
    elif interleaved_sort:
        ddl_attributes.append("INTERLEAVED SORTKEY ( {} )".format(join_column_list(interleaved_sort)))
    return ddl_attributes


def build_table_ddl(table_design: dict, table_name: TableName, is_temp=False) -> str:
    """
    Assemble the DDL of a table in a Redshift data warehouse.
    """
    columns = build_columns(table_design["columns"], is_temp=is_temp)
    constraints = build_table_constraints(table_design)
    attributes = build_table_attributes(table_design)

    ddl = """
        CREATE TABLE {table_name} (
            {columns_and_constraints}
        )
        {attributes}
        """.format(
        table_name=table_name,
        columns_and_constraints=",\n            ".join(chain(columns, constraints)),
        attributes="\n        ".join(attributes),
    )
    return ddl


@contextmanager
def log_load_error(cx):
    """Log any Redshift LOAD errors during a COPY command"""
    try:
        yield
    except psycopg2.Error as exc:
        if cx.get_transaction_status() != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
            logger.warning("Cannot retrieve error information from stl_load_errors within failed transaction")
        else:
            rows = etl.db.query(
                cx,
                """
                        SELECT session
                             , query
                             , starttime
                             , colname
                             , type
                             , trim(filename) AS filename
                             , line_number
                             , trim(err_reason) AS err_reason
                          FROM stl_load_errors
                         WHERE session = pg_backend_pid()
                         ORDER BY starttime DESC
                         LIMIT 1""",
            )
            if rows:
                row0 = rows.pop()
                max_len = max(len(k) for k in row0.keys())
                info = ["{key:{width}s} | {value}".format(key=k, value=row0[k], width=max_len) for k in row0.keys()]
                logger.warning("Load error information from stl_load_errors:\n  %s", "\n  ".join(info))
            else:
                etl.db.log_sql_error(exc)
        raise


def copy_using_manifest(
    conn: connection,
    table_name: TableName,
    column_list: List[str],
    s3_uri: str,
    aws_iam_role: str,
    data_format: Optional[str] = None,
    format_option: Optional[str] = None,
    file_compression: Optional[str] = None,
    need_compupdate=False,
    dry_run=False,
) -> None:

    credentials = "aws_iam_role={}".format(aws_iam_role)

    if data_format is None:
        # This is our original data format (which mirrors settings in unload).
        data_format_parameters = "DELIMITER ',' ESCAPE REMOVEQUOTES GZIP"
    else:
        if data_format == "CSV":
            if format_option is None:
                data_format_parameters = "CSV"
            else:
                data_format_parameters = "CSV QUOTE AS '{}'".format(format_option)
        elif data_format in ["AVRO", "JSON"]:
            if format_option is None:
                format_option = "auto"
            data_format_parameters = "{} AS '{}'".format(data_format, format_option)
        else:
            raise ETLSystemError("found unexpected data format: {}".format(data_format))
        if file_compression is not None:
            data_format_parameters += " {}".format(file_compression)

    copy_stmt = """
        COPY {table} (
            {columns}
        )
        FROM %s
        CREDENTIALS %s MANIFEST
        {data_format_parameters}
        TIMEFORMAT AS 'auto'
        DATEFORMAT AS 'auto'
        TRUNCATECOLUMNS
        STATUPDATE OFF
        COMPUPDATE {compupdate}
        """.format(
        table=table_name,
        columns=join_column_list(column_list),
        data_format_parameters=data_format_parameters,
        compupdate="ON" if need_compupdate else "OFF",
    )
    if dry_run:
        logger.info("Dry-run: Skipping copying data into '%s' using '%s'", table_name.identifier, s3_uri)
        etl.db.skip_query(conn, copy_stmt, (s3_uri, credentials))
    else:
        logger.info("Copying data into '%s' using '%s'", table_name.identifier, s3_uri)
        try:
            with log_load_error(conn):
                etl.db.execute(conn, copy_stmt, (s3_uri, credentials))
        except psycopg2.InternalError as exc:
            raise TransientETLError(exc) from exc


def query_load_commits(conn: connection, table_name: TableName, s3_uri: str, dry_run=False) -> None:
    stmt = """
        SELECT TRIM(filename) AS filename
             , lines_scanned
          FROM stl_load_commits
         WHERE query = pg_last_copy_id()
         ORDER BY TRIM(filename)
        """
    if dry_run:
        etl.db.skip_query(conn, stmt)
    else:
        rows = etl.db.query(conn, stmt)
        summary = "    " + "\n    ".join("'{filename}' ({lines_scanned} line(s))".format(**row) for row in rows)
        logger.debug(
            "Copied %d file(s) into '%s' using manifest '%s':\n%s", len(rows), table_name.identifier, s3_uri, summary
        )


def query_load_summary(conn: connection, table_name: TableName, dry_run=False) -> None:
    # This query is not guarded by "dry_run" so that we have a copy_id for the other query.
    [[copy_count, copy_id]] = etl.db.query(conn, "SELECT pg_last_copy_count(), pg_last_copy_id()")

    stmt = """
        SELECT COUNT(s3.key) AS file_count
             , COUNT(DISTINCT s.slice) AS slice_count
             , COUNT(DISTINCT s.node) AS node_count
             , MAX(wq.slot_count) AS slot_count
             , ROUND(MAX(wq.total_queue_time/1000000.0), 2) AS elapsed_queued
             , ROUND(MAX(wq.total_exec_time/1000000.0), 2) AS elapsed
             , ROUND(SUM(s3.transfer_size)/(1024.0*1024.0), 2) AS total_mb
          FROM stl_wlm_query wq
          JOIN stl_s3client s3 USING (query)
          JOIN stv_slices s USING (slice)
         WHERE wq.query = %s
        """
    if dry_run:
        etl.db.skip_query(conn, stmt, (copy_id,))
    else:
        [row] = etl.db.query(conn, stmt, (copy_id,))
        logger.info(
            (
                "Copied {copy_count:d} row(s) into {table_name:x} "
                "(files: {file_count:d}, slices: {slice_count:d}, nodes: {node_count:d}, slots: {slot_count:d}, "
                "elapsed: {elapsed}s ({elapsed_queued}s queued), size: {total_mb}MB)"
            ).format(copy_count=copy_count, table_name=table_name, **row)
        )


def copy_from_uri(
    conn: connection,
    table_name: TableName,
    column_list: List[str],
    s3_uri: str,
    aws_iam_role: str,
    data_format: Optional[str] = None,
    format_option: Optional[str] = None,
    file_compression: Optional[str] = None,
    need_compupdate=False,
    dry_run=False,
) -> None:
    """
    Load data into table in the data warehouse using the COPY command.
    """
    copy_using_manifest(
        conn,
        table_name,
        column_list,
        s3_uri,
        aws_iam_role,
        data_format,
        format_option,
        file_compression,
        need_compupdate,
        dry_run,
    )
    query_load_commits(conn, table_name, s3_uri, dry_run)
    query_load_summary(conn, table_name, dry_run)


def insert_from_query(
    conn: connection, table_name: TableName, column_list: List[str], query_stmt: str, dry_run=False
) -> None:
    """
    Load data into table in the data warehouse using the INSERT INTO command.
    """
    retriable_error_codes = etl.config.get_config_list("arthur_settings.retriable_error_codes")
    stmt = """
        INSERT INTO {table} (
            {columns}
        )
        {query}
        """.format(
        table=table_name, columns=join_column_list(column_list), query=query_stmt
    )

    if dry_run:
        logger.info("Dry-run: Skipping inserting data into '%s' from query", table_name.identifier)
        etl.db.skip_query(conn, stmt)
    else:
        logger.info("Inserting data into '%s' from query", table_name.identifier)
        try:
            etl.db.execute(conn, stmt)
        except psycopg2.InternalError as exc:
            if exc.pgcode in retriable_error_codes:
                raise TransientETLError(exc) from exc
            else:
                logger.warning("Unretriable SQL Error: pgcode=%d, pgerror=%s", exc.pgcode, exc.pgerror)
                raise
