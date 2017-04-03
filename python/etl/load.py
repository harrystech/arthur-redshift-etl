"""
Load (or update) data from upstream or execute CTAS or add views to Redshift.

A "load" refers to the wholesale replacement of any schema or table involved.

An "update" refers to the gentle replacement of tables.


There are three possibilities:

(1) "Tables" that have upstream sources must have CSV files and a manifest file.

(2) "CTAS" tables are derived from queries so must have a SQL file.  (Think of them
as materialized views.)

(3) "VIEWS" are views and so must have a SQL file.


Details for (1):

CSV files must have fields delimited by commas, quotes around fields if they
contain a comma, and have doubled-up quotes if there's a quote within the field.

Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP

TODO What is the date format and timestamp format of Sqoop and Spark?

Details for (2):

Expects for every derived table (CTAS) a SQL file in the S3 bucket with a valid
expression to create the content of the table (meaning: just the select without
closing ';'). The actual DDL statement (CREATE TABLE AS ...) and the table
attributes / constraints are added from the matching table design file.

Note that the table is actually created empty, then CTAS is used for a temporary
table which is then inserted into the table.  This is needed to attach
constraints, attributes, and encodings.
"""

from contextlib import closing
from itertools import chain
import logging

import psycopg2

import etl
from etl import join_column_list, TableName
import etl.dw
from etl.errors import MissingManifestError, RequiredRelationFailed, FailedConstraintError
import etl.monitor
import etl.pg
import etl.relation


def _build_constraints(table_design, exclude_foreign_keys=False):
    constraints = table_design.get("constraints", {})
    ddl_constraints = []
    for pk in ("primary_key", "surrogate_key"):
        if pk in constraints:
            ddl_constraints.append('PRIMARY KEY ( {} )'.format(join_column_list(constraints[pk])))
    for nk in ("unique", "natural_key"):
        if nk in constraints:
            ddl_constraints.append('UNIQUE ( {} )'.format(join_column_list(constraints[nk])))
    if "foreign_key" in constraints and not exclude_foreign_keys:
        local_columns, reference, reference_columns = constraints["foreign_key"]
        reference_table = TableName(*reference.split('.', 1))
        ddl_constraints.append('FOREIGN KEY ( {} ) REFERENCES {} ( {} )'.format(join_column_list(local_columns),
                                                                                reference_table,
                                                                                join_column_list(reference_columns)))
    return ddl_constraints


def _build_attributes(table_design, exclude_distribution=False):
    attributes = table_design.get("attributes", {})
    ddl_attributes = []
    if "distribution" in attributes and not exclude_distribution:
        dist = attributes["distribution"]
        if isinstance(dist, list):
            ddl_attributes.append('DISTSTYLE KEY')
            ddl_attributes.append('DISTKEY ( {} )'.format(join_column_list(dist)))
        elif dist in ("all", "even"):
            ddl_attributes.append('DISTSTYLE {}'.format(dist.upper()))
    if "compound_sort" in attributes:
        ddl_attributes.append('COMPOUND SORTKEY ( {} )'.format(join_column_list(attributes["compound_sort"])))
    elif "interleaved_sort" in attributes:
        ddl_attributes.append('INTERLEAVED SORTKEY ( {} )'.format(join_column_list(attributes["interleaved_sort"])))
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
                           "foreign_column": join_column_list(foreign_columns)})
            f_column += " REFERENCES {foreign_table} ( {foreign_column} )"
        s_columns.append(f_column.format(**column))
    s_constraints = _build_constraints(table_design, exclude_foreign_keys=is_temp)
    s_attributes = _build_attributes(table_design, exclude_distribution=is_temp)
    table_type = "TEMP TABLE" if is_temp else "TABLE"

    return "CREATE {} IF NOT EXISTS {} (\n{})\n{}".format(table_type, table_name,
                                                          ",\n".join(chain(s_columns, s_constraints)),
                                                          "\n".join(s_attributes)).replace('\n', "\n    ")


def create_table(conn, description, drop_table=False, dry_run=False):
    """
    Run the CREATE TABLE statement before trying to copy data into table.
    Also assign ownership to make sure all tables are owned by same user.
    Table may be dropped before (re-)creation but only the table owner is
    allowed to do so.
    """
    logger = logging.getLogger(__name__)
    table_name = description.target_table_name
    table_design = description.table_design
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


def create_view(conn, description, drop_view=False, dry_run=False):
    """
    Run the CREATE VIEW statement after dropping (potentially) an existing one.
    NOTE that this a no-op if drop_view is False.
    """
    logger = logging.getLogger(__name__)
    view_name = description.target_table_name
    s_columns = join_column_list(column["name"] for column in description.table_design["columns"])
    ddl_stmt = """CREATE VIEW {} (\n{}\n) AS\n{}""".format(view_name, s_columns, description.query_stmt)
    if drop_view:
        if dry_run:
            logger.info("Dry-run: Skipping (re-)creation of view '%s'", view_name.identifier)
            logger.debug("Skipped DDL:\n%s", ddl_stmt)
        else:
            logger.info("Dropping view (if exists) '%s'", view_name.identifier)
            etl.pg.execute(conn, "DROP VIEW IF EXISTS {} CASCADE".format(view_name))
            logger.info("Creating view '%s'", view_name.identifier)
            etl.pg.execute(conn, ddl_stmt)
    else:
        logger.info("Skipping update of view '%s'", view_name.identifier)
        logger.debug("Skipped DDL:\n%s", ddl_stmt)


def copy_data(conn, description, aws_iam_role, skip_copy=False, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.
    A manifest for the CSV files must be provided -- it is an error if the manifest is missing.

    Tables can only be truncated by their owners (and outside of a transaction), so this will delete
    all rows instead of truncating the tables.
    """
    logger = logging.getLogger(__name__)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    s3_path = "s3://{}/{}".format(description.bucket_name, description.manifest_file_name)
    table_name = description.target_table_name

    if dry_run:
        if not description.has_manifest:
            logger.warning("Missing manifest file for '%s'", description.identifier)
        logger.info("Dry-run: Skipping copy for '%s' from '%s'", table_name.identifier, s3_path)
    elif skip_copy:
        logger.info("Skipping copy for '%s' from '%s'", table_name.identifier, s3_path)
    else:
        if not description.has_manifest:
            raise MissingManifestError("Missing manifest file for '{}'".format(description.identifier))

        logger.info("Copying data into '%s' from '%s'", table_name.identifier, s3_path)
        try:
            # FIXME Given that we're always running as the owner now, could we truncate now?
            # The connection should not be open with autocommit at this point or we may have empty random tables.
            etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
            # N.B. If you change the COPY options, make sure to change the documentation at the top of the file.
            etl.pg.execute(conn, """COPY {}
                                    FROM %s
                                    CREDENTIALS %s MANIFEST
                                    DELIMITER ',' ESCAPE REMOVEQUOTES GZIP
                                    TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
                                    TRUNCATECOLUMNS
                                 """.format(table_name), (s3_path, credentials))
            # TODO Retrieve list of files that were actually loaded
            row_count = etl.pg.query(conn, "SELECT pg_last_copy_count()")
            logger.info("Copied %d rows into '%s'", row_count[0][0], table_name.identifier)
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
    s_columns = join_column_list(column["name"]
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
    Note that for timestamps, an arbitrary point in the past is used if the column
    isn't nullable.
    """
    s_columns = join_column_list(column["name"]
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
                # Use NULL for all null-able columns:
                if not column.get("not_null", False):
                    # Use NULL for any nullable column and use type cast (for UNION ALL to succeed)
                    na_values_row.append("NULL::{}".format(column["sql_type"]))
                elif "timestamp" in column["sql_type"]:
                    # TODO Is this a good value or should timestamps be null?
                    na_values_row.append("'0000-01-01 00:00:00'")
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


def create_temp_table_as_and_copy(conn, description, skip_copy=False, dry_run=False):
    """
    Run the CREATE TABLE AS statement to load data into temp table, then copy into final table.

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
    table_name = description.target_table_name
    table_design = description.table_design
    query_stmt = description.query_stmt

    temp_identifier = '$'.join(("arthur_temp", table_name.table))
    temp_name = '"{}"'.format(temp_identifier)
    has_any_identity = any([column.get("identity", False) for column in table_design["columns"]])

    if has_any_identity:
        ddl_temp_stmt = assemble_table_ddl(table_design, temp_name, use_identity=True, is_temp=True)
        s_columns = join_column_list(column["name"]
                                     for column in table_design["columns"]
                                     if not (column.get("identity", False) or column.get("skipped", False)))
        dml_temp_stmt = "INSERT INTO {} (\n{}\n) (\n{}\n)".format(temp_name, s_columns, query_stmt)
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name, add_row_for_key_0=True)
    else:
        ddl_temp_stmt = assemble_ctas_ddl(table_design, temp_name, query_stmt)
        dml_temp_stmt = None
        dml_stmt = assemble_insert_into_dml(table_design, table_name, temp_name)

    if dry_run:
        logger.info("Dry-run: Skipping loading of table '%s' using '%s'", table_name.identifier, temp_identifier)
        logger.debug("Skipped DDL for '%s': %s", temp_identifier, ddl_temp_stmt)
        logger.debug("Skipped DML for '%s': %s", temp_identifier, dml_temp_stmt)
        logger.debug("Skipped DML for '%s': %s", table_name.identifier, dml_stmt)
    elif skip_copy:
        logger.info("Skipping copy for '%s' from query", table_name.identifier)
        # Run explain plan to test the query and ensure upstream tables and views exist
        etl.pg.execute(conn, "EXPLAIN\n" + query_stmt)
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


def grant_access(conn, table_name, owner, reader_groups, writer_groups, dry_run=False):
    """
    Grant privileges on (new) relation based on configuration.

    We always grant all privileges to the ETL user.  We may grant read-only access
    or read-write access based on configuration.  Note that the is always based on groups, not users.
    """
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping grant of all privileges on '%s' to '%s'", table_name.identifier, owner)
    else:
        logger.info("Granting all privileges on '%s' to '%s'", table_name.identifier, owner)
        etl.pg.grant_all_to_user(conn, table_name.schema, table_name.table, owner)

    if reader_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of select access on '%s' to %s",
                        table_name.identifier, etl.join_with_quotes(reader_groups))
        else:
            logger.info("Granting select access on '%s' to %s",
                        table_name.identifier, etl.join_with_quotes(reader_groups))
            for reader in reader_groups:
                etl.pg.grant_select(conn, table_name.schema, table_name.table, reader)

    if writer_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of write access on '%s' to %s",
                        table_name.identifier, etl.join_with_quotes(writer_groups))
        else:
            logger.info("Granting write access on '%s' to %s",
                        table_name.identifier, etl.join_with_quotes(writer_groups))
            for writer in writer_groups:
                etl.pg.grant_select_and_write(conn, table_name.schema, table_name.table, writer)


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


def load_or_update_redshift_relation(conn, description, credentials, schema,
                                     drop=False, skip_copy=False, add_explain_plan=False, dry_run=False):
    """
    Load single table from CSV or using a SQL query or create new view.
    """
    table_name = description.target_table_name
    owner, reader_groups, writer_groups = schema.owner, schema.reader_groups, schema.writer_groups
    if description.is_ctas_relation or description.is_view_relation:
        object_key = description.sql_file_name
    else:
        object_key = description.manifest_file_name

    # TODO The monitor should contain the number of rows that were loaded.
    modified = False
    with etl.monitor.Monitor(table_name.identifier, 'load', dry_run=dry_run,
                             options=["skip_copy"] if skip_copy else [],
                             source={'bucket_name': description.bucket_name,
                                     'object_key': object_key},
                             destination={'name': etl.pg.dbname(conn),
                                          'schema': table_name.schema,
                                          'table': table_name.table}):
        if description.is_view_relation:
            create_view(conn, description, drop_view=drop, dry_run=dry_run)
            grant_access(conn, table_name, owner, reader_groups, writer_groups, dry_run=dry_run)
        elif description.is_ctas_relation:
            create_table(conn, description, drop_table=drop, dry_run=dry_run)
            create_temp_table_as_and_copy(conn, description, skip_copy=skip_copy, dry_run=dry_run)
            analyze(conn, table_name, dry_run=dry_run)
            # TODO What should we do with table data if a constraint violation is detected? Delete it?
            verify_constraints(conn, description, dry_run=dry_run)
            grant_access(conn, table_name, owner, reader_groups, writer_groups, dry_run=dry_run)
            modified = True
        else:
            create_table(conn, description, drop_table=drop, dry_run=dry_run)
            # Grant access to data source regardless of loading errors (writers may fix the problem outside of ETL)
            grant_access(conn, table_name, owner, reader_groups, writer_groups, dry_run=dry_run)
            copy_data(conn, description, credentials, skip_copy=skip_copy, dry_run=dry_run)
            analyze(conn, table_name, dry_run=dry_run)
            verify_constraints(conn, description, dry_run=dry_run)
            modified = True
        return modified


def verify_constraints(conn, description, dry_run=False) -> None:
    """
    Raises a FailedConstraintError if :description's target table doesn't obey its declared unique constraints.
    """
    logger = logging.getLogger(__name__)
    constraints = description.table_design.get("constraints")
    if constraints is None:
        logger.info("No constraints to verify for '%s'", description.identifier)
        return

    # To make this work in DataGrip, define '\{(\w+)\}' under Tools -> Database -> User Parameters.
    # Then execute the SQL using command-enter, enter the values for `cols` and `table`, et voila!
    statement_template = """
        SELECT {columns}
          FROM {table}
      GROUP BY {columns}
        HAVING COUNT(*) > 1
         LIMIT 5
    """

    for constraint_type in ["primary_key", "natural_key", "surrogate_key", "unique"]:
        if constraint_type in constraints:
            columns = constraints[constraint_type]
            quoted_columns = join_column_list(columns)
            statement = statement_template.format(columns=quoted_columns, table=description.target_table_name)
            if dry_run:
                logger.info("Dry-run: Skipping checking %s constraint on '%s'", constraint_type, description.identifier)
                logger.debug("Skipped query:\n%s", statement)
            else:
                logger.info("Checking %s constraint on '%s'", constraint_type, description.identifier)
                results = etl.pg.query(conn, statement)
                if results:
                    raise FailedConstraintError(description, constraint_type, columns, results)


def evaluate_execution_order(descriptions, selector, only_first=False, whole_schemas=False):
    """
    Returns a tuple like ( list of relations to executed, set of schemas they're in )

    Relation descriptions are ordered such that loading them in that order will succeed as
    predicted by the `depends_on` fields.

    If you select to use only the first, then the dependency fan-out is NOT followed.
    It is an error if this option is attempted to be used with possibly more than one
    table selected.

    If you select to widen the update to entire schemas, then, well, entire schemas
    are updated instead of surgically picking up tables.
    """
    logger = logging.getLogger(__name__)

    complete_sequence = etl.relation.order_by_dependencies(descriptions)

    dirty = set()
    for description in complete_sequence:
        if selector.match(description.target_table_name):
            dirty.add(description.identifier)

    if only_first:
        if len(dirty) != 1:
            raise ValueError("Bad selector, should result in single table being selected")
        if whole_schemas:
            raise ValueError("Cannot elect to pick both, entire schemas and only first relation")
    else:
        for description in complete_sequence:
            if any(table in dirty for table in description.dependencies):
                dirty.add(description.identifier)

    dirty_schemas = {description.target_table_name.schema
                     for description in complete_sequence if description.identifier in dirty}
    if whole_schemas:
        for description in complete_sequence:
            if description.target_table_name.schema in dirty_schemas:
                dirty.add(description.identifier)

    # FIXME move this into load/upgrade/update to have verb correct?
    if len(dirty) == len(complete_sequence):
        logger.info("Decided on updating ALL tables")
    elif len(dirty) == 1:
        logger.info("Decided on updating a SINGLE table: %s", list(dirty)[0])
    else:
        logger.info("Decided on updating %d of %d table(s)", len(dirty), len(complete_sequence))
    return [description for description in complete_sequence if description.identifier in dirty], dirty_schemas


def show_dependents(descriptions, selector):
    """
    List the execution order of loads or updates.

    Relations are marked based on whether they were directly selected or selected as
    part of the fan-out of an update.
    They are also marked whether they'd lead to a fatal error since they're required for full load.
    """
    logger = logging.getLogger(__name__)

    execution_order, involved_schema_names = evaluate_execution_order(descriptions, selector)
    if len(execution_order) == 0:
        logger.warning("Found no matching relations for: %s", selector)
        return
    logger.info("Involved schemas: %s", etl.join_with_quotes(involved_schema_names))

    selected = frozenset(description.identifier for description in execution_order
                         if selector.match(description.target_table_name))

    immediate = set(selected)
    for description in execution_order:
        if description.is_view_relation and any(name in immediate for name in description.dependencies):
            immediate.add(description.identifier)
    immediate = frozenset(immediate - selected)

    logger.info("Execution order includes %d selected, %d immediate, and %d fan-out relation(s)",
                len(selected), len(immediate), len(execution_order) - len(selected) - len(immediate))

    required = [description for description in execution_order if description.is_required]
    logger.info("Execution order includes %d required relation(s)", len(required))

    max_len = max(len(description.identifier) for description in execution_order)
    for i, description in enumerate(execution_order):
        if description.is_ctas_relation:
            reltype = "CTAS"
        elif description.is_view_relation:
            reltype = "VIEW"
        else:
            reltype = "DATA"
        if description.identifier in selected:
            flag = "selected"
        elif description.identifier in immediate:
            flag = "immediate"
        else:
            flag = "fan-out"
        if description.is_required:
            flag += ", required"
        print("{index:4d} {identifier:{width}s} ({reltype}) ({flag})".format(
                index=i + 1, identifier=description.identifier, width=max_len, reltype=reltype, flag=flag))


def load_or_update_redshift(data_warehouse, descriptions, selector, drop=False, stop_after_first=False,
                            no_rollback=False, skip_copy=False, dry_run=False):
    """
    Load table from CSV file or based on SQL query or install new view.

    Tables are matched based on the selector but note that anything downstream is also refreshed
    as long as the dependency is known.

    This is forceful if drop is True ... and replaces anything that might already exist.

    You can skip the COPY command to bring up the database schemas with all tables quickly although without
    any content.  So a load with drop=True, skip_copy=True followed by a load with drop=False, skip_copy=False
    should be a quick way to load data that is "under development" and may not have all dependencies or
    names / types correct.
    """
    logger = logging.getLogger(__name__)
    whole_schemas = drop and not stop_after_first
    execution_order, involved_schema_names = evaluate_execution_order(
        descriptions, selector, only_first=stop_after_first, whole_schemas=whole_schemas)

    required_selector = data_warehouse.required_in_full_load_selector
    schema_config_lookup = {schema.name: schema for schema in data_warehouse.schemas}
    involved_schemas = [schema_config_lookup[s] for s in involved_schema_names]

    if whole_schemas:
        with closing(etl.pg.connection(data_warehouse.dsn_etl, autocommit=whole_schemas)) as conn:
            if dry_run:
                logger.info("Dry-run: Skipping backup of schemas and creation")
            else:
                etl.dw.backup_schemas(conn, involved_schemas)
                etl.dw.create_schemas(conn, involved_schemas)

    vacuumable = []
    failed = set()

    # TODO Add retry here in case we're doing a full reload.
    conn = etl.pg.connection(data_warehouse.dsn_etl, autocommit=whole_schemas)
    with closing(conn) as conn, conn as conn:
        try:
            for index, description in enumerate(execution_order):
                logger.info("Starting to work on '%s' (%d/%d)", description.identifier, index+1, len(execution_order))
                if description.identifier in failed:
                    logger.info("Skipping load for relation '%s' due to failed dependencies", description.identifier)
                    continue
                target_schema = schema_config_lookup[description.target_table_name.schema]
                try:
                    modified = load_or_update_redshift_relation(
                        conn, description, data_warehouse.iam_role, target_schema,
                        drop=drop, skip_copy=skip_copy, add_explain_plan=add_explain_plan, dry_run=dry_run)
                    if modified:
                        vacuumable.append(description.target_table_name)
                except Exception as e:
                    if whole_schemas:
                        subtree = _get_failed_subtree(description, execution_order, required_selector)
                        failed.update(subtree)
                        # FIXME This is difficult to read in the log, especially when the subtree is empty.
                        logger.warning("Load failure for '%s' does not harm any relations required by selector '%s';"
                                       " continuing load omitting these dependent relations: %s"
                                       ". Failure error was: %s",
                                       description.identifier, required_selector,
                                       subtree.difference({description.identifier}), e)
                    else:
                        raise

        except Exception:
            if whole_schemas:
                if dry_run:
                    logger.info("Dry-run: Skipping restoration of backup in exception handling")
                elif not no_rollback:
                    # Defensively create a new connection to rollback
                    etl.dw.restore_schemas(etl.pg.connection(data_warehouse.dsn_etl, autocommit=whole_schemas),
                                           involved_schemas)
            raise

    # Reconnect to run vacuum outside transaction block
    if vacuumable and not drop:
        with closing(etl.pg.connection(data_warehouse.dsn_etl, autocommit=True)) as conn:
            for table_name in vacuumable:
                vacuum(conn, table_name, dry_run=dry_run)


def _get_failed_subtree(failed_description, execution_order, required_selector):
    """
    After an exception in loading :description, add its dependency subtree using :execution_order,
    and if any element matches :required_selector, raise a RequiredRelationFailed exception
    """
    subtree = {failed_description.identifier}
    for description in execution_order:
        if any(table in subtree for table in description.dependencies):
            subtree.add(description.identifier)

    illegal_failures = [d.identifier for d in execution_order
                        if d.identifier in subtree and required_selector.match(d.target_table_name)]

    if illegal_failures:
        raise RequiredRelationFailed(failed_description, illegal_failures, required_selector)
    else:
        return subtree
