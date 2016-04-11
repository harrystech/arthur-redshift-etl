import configparser
import io
from itertools import chain
import logging
import os.path
import simplejson as json

import jsonschema
import psycopg2
import yaml

from etl import TableName
import etl.config
import etl.dump
import etl.s3
import etl.pg


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file). The table design is
    validated before being returned.
    """
    table_design = yaml.safe_load(stream)
    validate_table_design(table_design, table_name)
    return table_design


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not right.
    """
    try:
        table_design_schema = etl.config.load_json("table_design.schema")
    except (jsonschema.exceptions.SchemaError, json.scanner.JSONDecodeError):
        logging.getLogger(__name__).critical("Internal Error: Schema in 'table_design.schema' is not valid")
        raise
    jsonschema.validate(table_design, table_design_schema)
    # TODO Need more rules?
    if table_design["name"] != table_name.identifier:
        raise ValueError("Name of table (%s) must match target (%s)" % (table_design["name"], table_name.identifier))
    for column in table_design["columns"]:
        if column.get("not_null", False):
            # NOT NULL columns -- may not have "null" as type
            if isinstance(column["type"], list) or column["type"] == "null":
                raise ValueError('"not null" column may not have null type')
        else:
            # NULL columns -- must have "null" as type and may not be primary key (identity)
            if not (isinstance(column["type"], list) and "null" in column["type"]):
                raise ValueError('"null" missing as type for null-able column')
            if column.get("identity", False):
                raise ValueError("identity column must be set to not null")


def format_column_list(columns):
    """
    Return string with comma-separated, delimited column names
    """
    return ", ".join('"{}"'.format(column) for column in columns)


def _build_constraints(table_design):
    constraints = table_design.get("constraints", {})
    ddl_constraints = []
    # TODO add surrogate key as an additional constraint name?
    for pk in ("primary_key",):
        if pk in constraints:
            ddl_constraints.append('PRIMARY KEY ( {} )'.format(format_column_list(constraints[pk])))
    for nk in ("unique", "natural_key"):
        if nk in constraints:
            ddl_constraints.append('UNIQUE ( {} )'.format(format_column_list(constraints[nk])))
    if "foreign_key" in constraints:
        local_columns, reference, reference_columns = constraints["foreign_key"]
        reference_table = TableName(*reference.split('.', 1))
        ddl_constraints.append('FOREIGN KEY ( {} ) REFERENCES {} ( {} )'.format(format_column_list(local_columns),
                                                                                reference_table,
                                                                                format_column_list(reference_columns)))
    return ddl_constraints


def _build_attributes(table_design):
    attributes = table_design.get("attributes", {})
    ddl_attributes = []
    if "distribution" in attributes:
        dist = attributes["distribution"]
        if isinstance(dist, list):
            ddl_attributes.append('DISTSTYLE KEY')
            ddl_attributes.append('DISTKEY ( {} )'.format(format_column_list(dist)))
        elif dist in ("all", "even"):
            ddl_attributes.append('DISTSTYLE {}'.format(dist.upper()))
    if "compound_sort" in attributes:
        ddl_attributes.append('COMPOUND SORTKEY ( {} )'.format(format_column_list(attributes["compound_sort"])))
    elif "interleaved_sort" in attributes:
        ddl_attributes.append('INTERLEAVED SORTKEY ( {} )'.format(format_column_list(attributes["interleaved_sort"])))
    return ddl_attributes


def assemble_table_ddl(table_design, table_name, use_identity=False):
    """
    Columns must have a name and a SQL type (compatible with Redshift).
    They may have an attribute of the compression encoding and the nullable
    constraint.
    Other column attributes and constraints should be resolved as table
    attributes (e.g. distkey) and table constraints (e.g. primary key).
    Tables may have attributes such as a distribution style and sort key.
    Depending on the distribution style, they may also have a distribution key.
    Supported table constraints include primary key (most likely "id"),
    unique constraint, and foreign keys.
    """
    s_columns = []
    for column in table_design["columns"]:
        f_column = '"{name}" {sql_type}'
        if column.get('identity', False) and use_identity:
            f_column += " IDENTITY(1, 1)"
        if "encoding" in column:
            f_column += " ENCODE {encoding}"
        if column.get('not_null', False):
            f_column += " NOT NULL"
        s_columns.append(f_column.format(**column))
    s_constraints = _build_constraints(table_design)
    s_attributes = _build_attributes(table_design)

    return "CREATE TABLE IF NOT EXISTS {} (\n{})\n{}".format(table_name,
                                                             ",\n".join(chain(s_columns, s_constraints)),
                                                             "\n".join(s_attributes)).replace('\n', "\n    ")


def create_table(conn, table_name, table_owner, bucket_name, ddl_file, drop_table=False, dry_run=False):
    """
    Run the CREATE TABLE statement before trying to copy data into table.
    Also assign ownership to make sure all tables are owned by same user.
    Table may be dropped before (re-)creation but only the table owner is
    allowed to do so.
    """
    logger = logging.getLogger(__name__)
    content = etl.s3.get_file_content(bucket_name, ddl_file)
    table_design = load_table_design(io.StringIO(content), table_name)
    ddl_stmt = assemble_table_ddl(table_design, table_name)

    if dry_run:
        logger.info("Dry-run: Skipping creation of %s from 's3://%s/%s'", table_name.identifier, bucket_name, ddl_file)
        logger.debug("Skipped DDL:\n%s", ddl_stmt)
    else:
        if drop_table:
            logger.info("Dropping table first: %s", table_name.identifier)
            etl.pg.execute(conn, "DROP TABLE IF EXISTS {} CASCADE".format(table_name))

        logger.info("Creating table (if not exists): %s", table_name.identifier)
        etl.pg.execute(conn, ddl_stmt)

        logger.info("Making user %s owner of %s", table_owner, table_name.identifier)
        etl.pg.alter_table_owner(conn, table_name.schema, table_name.table, table_owner)


def read_aws_credentials(from_file="~/.aws/credentials"):
    """
    Read access key and secret key from file (by default, ~/.aws/credentials)
    or from environment variables, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
    """
    if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
        access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        logging.getLogger(__name__).info("Read credentials from environment")
    else:
        parser = configparser.ConfigParser()
        found = parser.read(os.path.expanduser(from_file))
        if len(found) != 1:
            raise RuntimeError("Unable to read your '%s' file" % from_file)
        access_key_id = parser.get('default', 'aws_access_key_id')
        secret_access_key = parser.get('default', 'aws_secret_access_key')
        logging.getLogger(__name__).info("Read credentials for COPY command from '%s'", found[0])
    return {'access_key_id': access_key_id, 'secret_access_key': secret_access_key}


def copy_data(conn, table_name, bucket_name, csv_file, credentials, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.

    Tables can only be truncated by their owners, so this will delete all rows
    instead of truncating the tables.
    """
    access = "aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}".format(**credentials)
    filename = "s3://{}/{}".format(bucket_name, csv_file)
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping copy for {} from '{}'".format(table_name.identifier, filename))
    else:
        logger.info("Copying data into {} from '{}'".format(table_name.identifier, filename))
        try:
            # The connection should not be open with autocommit at this point or we may have empty random tables.
            etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
            etl.pg.execute(conn, """COPY {}
                                    FROM %s
                                    CREDENTIALS %s
                                    FORMAT AS CSV GZIP IGNOREHEADER {:d}
                                    NULL AS '\\\\N'
                                    TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
                                    TRUNCATECOLUMNS
                                 """.format(table_name, etl.dump.N_HEADER_LINES), (filename, access))
            conn.commit()
        except psycopg2.Error as exc:
            if "stl_load_errors" in exc.pgerror:
                info = etl.pg.query(conn, """SELECT query, starttime, filename, colname, type, col_length,
                                                    line_number, position, err_code, err_reason
                                               FROM stl_load_errors
                                              WHERE session = pg_backend_pid()
                                              ORDER BY starttime DESC
                                              LIMIT 1""")
                values = '\n  '.join(["{}: {}".format(k, row[k]) for row in info for k in row.keys()])
                logger.info("Information from stl_load_errors:\n  %s", values)
            raise


def assemble_ctas_ddl(table_design, temp_name, query_stmt):
    """
    Return statement to create table based on a query, something like:
    CREATE TEMP TABLE table_name ( column_name [, ... ] ) table_attributes AS query
    """
    s_columns = format_column_list(column["name"]
                                   for column in table_design["columns"]
                                   if not column.get("identity", False))
    # TODO Measure whether adding attributes helps or hurts performance.
    s_attributes = _build_attributes(table_design)
    return "CREATE TEMP TABLE {} (\n{})\n{}\nAS\n".format(temp_name, s_columns,
                                                          "\n".join(s_attributes)).replace('\n', "\n     ") + query_stmt


def assemble_insert_into_dml(table_design, table_name, temp_name, add_row_for_key_0=False):
    """
    Create an INSERT statement to copy data from temp table to new table.

    If there is an identity column involved, also add the n/a row with key=0.
    """
    if add_row_for_key_0:
        s_columns = format_column_list(column["name"] for column in table_design["columns"])
        values = []
        for column in table_design["columns"]:
            if column.get("identity", False):
                values.append(0)
            else:
                if not column.get("not_null", False):
                    values.append("NULL")
                elif "string" in column["type"]:
                    values.append("'N/A'")
                elif "boolean" in column["type"]:
                    values.append("FALSE")
                else:
                    values.append("0")
        s_values = ", ".join(str(value) for value in values)
        return """INSERT INTO {}
                    (SELECT
                         {}
                       FROM {}
                      UNION ALL
                     SELECT
                         {})""".format(table_name, s_columns, temp_name, s_values).replace('\n', "\n    ")
    else:
        return """INSERT INTO {} (SELECT * FROM {})""".format(table_name, temp_name)


def create_temp_table_as_and_copy(conn, table_name, bucket_name, design_file, sql_file,
                                  add_explain_plan=False, dry_run=False):
    """
    Run the CREATE TABLE AS statement to load data into temp table,
    then copy into final table.

    Actual implementation:
    (1) If there is a column marked with identity=True, then create a temporary
    table, insert into it (to build key values).  Finally insert the temp table
    into the destination table while adding a row that has key=0 and n/a values.
    (2) Otherwise, create temp table with CTAS then copy into destination table.

    Note that CTAS doesn't allow to specify column types (or encoding or column
    constraints) so we need to have a temp table separate from destination
    table in order to have full flexibility how we define the destination table.
    """
    logger = logging.getLogger(__name__)
    content = etl.s3.get_file_content(bucket_name, design_file)
    table_design = load_table_design(io.StringIO(content), table_name)
    query_stmt = etl.s3.get_file_content(bucket_name, sql_file)

    temp_name = '"{}${}"'.format("staging", table_name.table)
    has_any_identity = any([column.get("identity", False) for column in table_design["columns"]])

    if has_any_identity:
        ddl_temp_stmt = assemble_table_ddl(table_design, temp_name, use_identity=True)
        s_columns = format_column_list(column["name"]
                                       for column in table_design["columns"]
                                       if not column.get("identity", False))
        dml_temp_stmt = "INSERT INTO {} (\n{}) (\n{})".format(temp_name, s_columns, query_stmt)
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name, add_row_for_key_0=True)
    else:
        ddl_temp_stmt = assemble_ctas_ddl(table_design, temp_name, query_stmt)
        dml_temp_stmt = None
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name)

    if add_explain_plan:
        plan = etl.pg.query(conn, "EXPLAIN\n" + query_stmt)
        logger.info("Explain plan for query:\n | %s", "\n | ".join(row[0] for row in plan))
    if dry_run:
        logger.info("Dry-run: Skipping loading of %s using %s", table_name.identifier, temp_name)
        logger.debug("Skipped DDL for %s: %s", temp_name, ddl_temp_stmt)
        logger.debug("Skipped DML for %s: %s", temp_name, dml_temp_stmt)
        logger.debug("Skipped DML for %s: %s", table_name.identifier, dml_stmt)
    else:
        logger.info("Creating temp table %s", temp_name)
        etl.pg.execute(conn, ddl_temp_stmt)
        if dml_temp_stmt:
            logger.info("Filling temp table %s", temp_name)
            etl.pg.execute(conn, dml_temp_stmt)
        logger.info("Loading table %s from temp table %s", table_name.identifier, temp_name)
        etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
        etl.pg.execute(conn, dml_stmt)
        etl.pg.execute(conn, """DROP TABLE {}""".format(temp_name))


def grant_access(conn, table_name, etl_group, user_group, dry_run=False):
    """
    Grant select permission to users and all privileges to etl group.
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping permissions grant on %s", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Granting all privileges on %s to %s", table_name.identifier, etl_group)
        etl.pg.grant_all(conn, table_name.schema, table_name.table, etl_group)
        logging.getLogger(__name__).info("Granting select access on %s to %s", table_name.identifier, user_group)
        etl.pg.grant_select(conn, table_name.schema, table_name.table, user_group)


def analyze(conn, table_name, dry_run=False):
    """
    Final step ... tidy up the warehouse before guests come over
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping analyze %s", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Running analyze step on %s", table_name.identifier)
        etl.pg.execute(conn, "ANALYZE {}".format(table_name))
