import configparser
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
import etl.pg


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file). The table design is
    validated before being returned.
    """
    table_design = yaml.safe_load(stream)
    logging.getLogger(__name__).debug("Trying to validate table design for '%s' from stream", table_name.identifier)
    return validate_table_design(table_design, table_name)


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not
    right.

    Phase 1 of validation is based on a schema and json-schema validation.
    Phase 2 is built on specific rules that I couldn't figure out how
    to run inside json-schema.
    """
    logger = logging.getLogger(__name__)
    try:
        table_design_schema = etl.config.load_json("table_design.schema")
    except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError, json.scanner.JSONDecodeError):
        logger.critical("Internal Error: Schema in 'table_design.schema' is not valid")
        raise
    try:
        jsonschema.validate(table_design, table_design_schema)
    except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError, json.scanner.JSONDecodeError):
        logger.error("Failed to validate table design for '%s'!", table_name.identifier)
        raise
    # TODO Need more rules?
    if table_design["name"] != table_name.identifier:
        raise ValueError("Name of table (%s) must match target (%s)" % (table_design["name"], table_name.identifier))
    if table_design["source_name"] == "VIEW":
        for column in table_design["columns"]:
            if column.get("skipped", False):
                raise ValueError("columns may not be skipped in views")
    else:
        for column in table_design["columns"]:
            if column.get("skipped", False):
                continue
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
        identity_columns = [column["name"] for column in table_design["columns"] if column.get("identity", False)]
        if len(identity_columns) > 1:
            raise ValueError("only one column should have identity")
        surrogate_keys = table_design.get("constraints", {}).get("surrogate_key", [])
        if len(surrogate_keys) and not surrogate_keys == identity_columns:
            raise ValueError("surrogate key must be identity")
    return table_design


def compare_columns(live_design, file_design):
    logger = logging.getLogger(__name__)
    live_columns = {column["name"] for column in live_design["columns"]}
    file_columns = {column["name"] for column in file_design["columns"]}
    # TODO define policy to declare columns "ETL-only"
    etl_columns = {name for name in file_columns if name.startswith("etl__")}
    logger.debug("Number of columns of '%s' in database: %d vs. in design: %d (ETL: %d)",
                 file_design["name"], len(live_columns), len(file_columns), len(etl_columns))
    not_accounted_for_on_file = live_columns.difference(file_columns)
    described_but_not_live = file_columns.difference(live_columns).difference(etl_columns)
    if not_accounted_for_on_file:
        logger.warning("New columns in '%s' that are not in existing table design: %s",
                       file_design["name"], sorted(not_accounted_for_on_file))
        indices = dict((name, i) for i, name in enumerate(column["name"] for column in live_design["columns"]))
        for name in not_accounted_for_on_file:
            logger.debug("New column %s.%s: %s", live_design["name"], name,
                         json.dumps(live_design["columns"][indices[name]], indent="    ", sort_keys=True))
    if described_but_not_live:
        logger.warning("Columns that have disappeared in '%s': %s", file_design["name"], sorted(described_but_not_live))


def format_column_list(columns):
    """
    Return string with comma-separated, delimited column names
    """
    return ", ".join('"{}"'.format(column) for column in columns)


def _build_constraints(table_design, exclude_foreign_keys=False):
    constraints = table_design.get("constraints", {})
    ddl_constraints = []
    for pk in ("primary_key", "surrogate_key"):
        if pk in constraints:
            ddl_constraints.append('PRIMARY KEY ( {} )'.format(format_column_list(constraints[pk])))
    for nk in ("unique", "natural_key"):
        if nk in constraints:
            ddl_constraints.append('UNIQUE ( {} )'.format(format_column_list(constraints[nk])))
    if "foreign_key" in constraints and not exclude_foreign_keys:
        local_columns, reference, reference_columns = constraints["foreign_key"]
        reference_table = TableName(*reference.split('.', 1))
        ddl_constraints.append('FOREIGN KEY ( {} ) REFERENCES {} ( {} )'.format(format_column_list(local_columns),
                                                                                reference_table,
                                                                                format_column_list(reference_columns)))
    return ddl_constraints


def _build_attributes(table_design, exclude_distribution=False):
    attributes = table_design.get("attributes", {})
    ddl_attributes = []
    if "distribution" in attributes and not exclude_distribution:
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


def assemble_table_ddl(table_design, table_name, use_identity=False, is_temp=False):
    """
    Assemble the DDL to create the table for this design.

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
        if column.get("skipped", False):
            continue
        f_column = '"{name}" {sql_type}'
        if column.get("identity", False) and use_identity:
            f_column += " IDENTITY(1, 1)"
        if "encoding" in column:
            f_column += " ENCODE {encoding}"
        if column.get("not_null", False):
            f_column += " NOT NULL"
        if column.get("references") and not is_temp:
            # Split column constraint into the table and columns that are referenced
            foreign_table, foreign_columns = column["references"]
            column.update({"foreign_table": foreign_table,
                           "foreign_column": format_column_list(foreign_columns)})
            f_column += " REFERENCES {foreign_table} ( {foreign_column} )"
        s_columns.append(f_column.format(**column))
    s_constraints = _build_constraints(table_design, exclude_foreign_keys=is_temp)
    s_attributes = _build_attributes(table_design, exclude_distribution=is_temp)
    table_type = "TEMP TABLE" if is_temp else "TABLE"

    return "CREATE {} IF NOT EXISTS {} (\n{})\n{}".format(table_type, table_name,
                                                          ",\n".join(chain(s_columns, s_constraints)),
                                                          "\n".join(s_attributes)).replace('\n', "\n    ")


def create_table(conn, table_design, table_name, table_owner, drop_table=False, dry_run=False):
    """
    Run the CREATE TABLE statement before trying to copy data into table.
    Also assign ownership to make sure all tables are owned by same user.
    Table may be dropped before (re-)creation but only the table owner is
    allowed to do so.
    """
    logger = logging.getLogger(__name__)
    ddl_stmt = assemble_table_ddl(table_design, table_name)

    if dry_run:
        logger.info("Dry-run: Skipping creation of table '%s'", table_name.identifier)
        logger.debug("Skipped DDL:\n%s", ddl_stmt)
    else:
        if drop_table:
            logger.info("Dropping table '%s'", table_name.identifier)
            etl.pg.execute(conn, "DROP TABLE IF EXISTS {} CASCADE".format(table_name))

        logger.info("Creating table '%s' (if not exists)", table_name.identifier)
        etl.pg.execute(conn, ddl_stmt)

        logger.info("Making user '%s' owner of table '%s'", table_owner, table_name.identifier)
        etl.pg.alter_table_owner(conn, table_name.schema, table_name.table, table_owner)


def create_view(conn, table_design, view_name, table_owner, query_stmt, drop_view=False, dry_run=False):
    """
    Run the CREATE VIEW statement.

    Optionally drop the view first.  This is necessary if the name or type
    of columns changes.
    """
    logger = logging.getLogger(__name__)
    s_columns = format_column_list(column["name"] for column in table_design["columns"])
    ddl_stmt = """CREATE OR REPLACE VIEW {} (\n{}\n) AS\n{}""".format(view_name, s_columns, query_stmt)
    if dry_run:
        logger.info("Dry-run: Skipping creation of view '%s'", view_name.identifier)
        logger.debug("Skipped DDL:\n%s", ddl_stmt)
    else:
        if drop_view:
            logger.info("Dropping view '%s'", view_name.identifier)
            etl.pg.execute(conn, "DROP VIEW IF EXISTS {} CASCADE".format(view_name))
        # TODO Make sure ownership is ETL owner!
        logger.info("Creating view '%s'", view_name.identifier)
        etl.pg.execute(conn, ddl_stmt)


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


def copy_data(conn, credentials, table_name, filename, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.

    If filename ends in ".manifest" then it is assumed to be a manifest.

    Tables can only be truncated by their owners, so this will delete all rows
    instead of truncating the tables.
    """
    access = "aws_access_key_id={access_key_id};aws_secret_access_key={secret_access_key}".format(**credentials)
    logger = logging.getLogger(__name__)
    with_manifest = " MANIFEST" if filename.endswith(".manifest") else ""
    if dry_run:
        logger.info("Dry-run: Skipping copy for '%s' from%s '%s'", table_name.identifier, with_manifest, filename)
    else:
        logger.info("Copying data into '%s' from%s '%s'", table_name.identifier, with_manifest, filename)
        try:
            # The connection should not be open with autocommit at this point or we may have empty random tables.
            etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
            etl.pg.execute(conn, """COPY {}
                                    FROM %s
                                    CREDENTIALS %s{}
                                    FORMAT AS CSV GZIP IGNOREHEADER {:d}
                                    NULL AS '\\\\N'
                                    TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
                                    TRUNCATECOLUMNS
                                 """.format(table_name, with_manifest, etl.dump.N_HEADER_LINES), (filename, access))
            conn.commit()
        except psycopg2.Error as exc:
            conn.rollback()
            if "stl_load_errors" in exc.pgerror:
                logger.debug("Trying to get error message from stl_log_errors table")
                info = etl.pg.query(conn, """SELECT query, starttime, filename, colname, type, col_length,
                                                    line_number, position, err_code, err_reason
                                               FROM stl_load_errors
                                              WHERE session = pg_backend_pid()
                                              ORDER BY starttime DESC
                                              LIMIT 1""")
                values = "  \n".join(["{}: {}".format(k, row[k]) for row in info for k in row.keys()])
                logger.info("Information from stl_load_errors:\n  %s", values)
            raise


def assemble_ctas_ddl(table_design, temp_name, query_stmt):
    """
    Return statement to create table based on a query, something like:
    CREATE TEMP TABLE table_name ( column_name [, ... ] ) table_attributes AS query
    """
    s_columns = format_column_list(column["name"]
                                   for column in table_design["columns"]
                                   if not (column.get("identity", False) or column.get("skipped", False)))
    # TODO Measure whether adding attributes helps or hurts performance.
    s_attributes = _build_attributes(table_design, exclude_distribution=True)
    return "CREATE TEMP TABLE {} (\n{})\n{}\nAS\n".format(temp_name, s_columns,
                                                          "\n".join(s_attributes)).replace('\n', "\n     ") + query_stmt


def assemble_insert_into_dml(table_design, table_name, temp_name, add_row_for_key_0=False):
    """
    Create an INSERT statement to copy data from temp table to new table.

    If there is an identity column involved, also add the n/a row with key=0.
    """
    s_columns = format_column_list(column["name"]
                                   for column in table_design["columns"]
                                   if not column.get("skipped", False))
    if add_row_for_key_0:
        na_values_row = []
        for column in table_design["columns"]:
            if column.get("skipped", False):
                continue
            elif column.get("identity", False):
                na_values_row.append(0)
            else:
                if not column.get("not_null", False):
                    na_values_row.append("NULL::{}".format(column["sql_type"]))
                elif "string" in column["type"]:
                    na_values_row.append("'N/A'")
                elif "boolean" in column["type"]:
                    na_values_row.append("FALSE")
                else:
                    na_values_row.append("0")
        s_values = ", ".join(str(value) for value in na_values_row)
        return """INSERT INTO {}
                    (SELECT
                         {}
                       FROM {}
                      UNION ALL
                     SELECT
                         {})""".format(table_name, s_columns, temp_name, s_values).replace('\n', "\n    ")
    else:
        return """INSERT INTO {}
                    (SELECT {}
                       FROM {})""".format(table_name, s_columns, temp_name)


def create_temp_table_as_and_copy(conn, table_name, table_design, query_stmt, add_explain_plan=False, dry_run=False):
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
    temp_identifier = "{}${}".format("staging", table_name.table)
    temp_name = '"{}"'.format(temp_identifier)
    has_any_identity = any([column.get("identity", False) for column in table_design["columns"]])

    if has_any_identity:
        ddl_temp_stmt = assemble_table_ddl(table_design, temp_name, use_identity=True, is_temp=True)
        s_columns = format_column_list(column["name"]
                                       for column in table_design["columns"]
                                       if not (column.get("identity", False) or column.get("skipped", False)))
        dml_temp_stmt = "INSERT INTO {} (\n{}) (\n{})".format(temp_name, s_columns, query_stmt)
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name, add_row_for_key_0=True)
    else:
        ddl_temp_stmt = assemble_ctas_ddl(table_design, temp_name, query_stmt)
        dml_temp_stmt = None
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name)

    if add_explain_plan:
        plan = etl.pg.query(conn, "EXPLAIN\n" + query_stmt, debug=False)
        logger.info("Explain plan for query:\n | %s", "\n | ".join(row[0] for row in plan))
    if dry_run:
        logger.info("Dry-run: Skipping loading of table '%s' using '%s'", table_name.identifier, temp_identifier)
        logger.debug("Skipped DDL for '%s': %s", temp_identifier, ddl_temp_stmt)
        logger.debug("Skipped DML for '%s': %s", temp_identifier, dml_temp_stmt)
        logger.debug("Skipped DML for '%s': %s", table_name.identifier, dml_stmt)
    else:
        logger.info("Creating temp table '%s'", temp_identifier)
        etl.pg.execute(conn, ddl_temp_stmt)
        if dml_temp_stmt:
            logger.info("Filling temp table '%s'", temp_identifier)
            etl.pg.execute(conn, dml_temp_stmt)
        logger.info("Loading table '%s' from temp table '%s'", table_name.identifier, temp_identifier)
        etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
        etl.pg.execute(conn, dml_stmt)
        etl.pg.execute(conn, """DROP TABLE {}""".format(temp_name))


def grant_access(conn, table_name, etl_group, user_group, dry_run=False):
    """
    Grant select permission to users and all privileges to etl group.
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping permissions grant on '%s'", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Granting all privileges on '%s' to '%s'", table_name.identifier, etl_group)
        etl.pg.grant_all(conn, table_name.schema, table_name.table, etl_group)
        logging.getLogger(__name__).info("Granting select access on '%s' to '%s'", table_name.identifier, user_group)
        etl.pg.grant_select(conn, table_name.schema, table_name.table, user_group)


def analyze(conn, table_name, dry_run=False):
    """
    Update table statistics.
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping analysis of '%s'", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Running analyze step on table '%s'", table_name.identifier)
        etl.pg.execute(conn, "ANALYZE {}".format(table_name))


def vacuum(conn, table_name, dry_run=False):
    """
    Final step ... tidy up the warehouse before guests come over.
    """
    if dry_run:
        logging.getLogger(__name__).info("Dry-run: Skipping vacuum of '%s'", table_name.identifier)
    else:
        logging.getLogger(__name__).info("Running vacuum step on table '%s'", table_name.identifier)
        etl.pg.execute(conn, "VACUUM {}".format(table_name))
