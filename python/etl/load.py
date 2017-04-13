"""
This is the "load and transform" part of our ELT.

There are three options for accomplishing this:

(1) a "load" command will start building the data warehouse from scratch (usually, after backing up).
(2) an "upgrade" command will try to load new data and changed relations (without any back up).
(3) an "update" command will attempt to bring in new data and new relations, then let it percolate.

The "load" command is used in the rebuild pipeline. It is important to notice that
it always operates on all tables in any schemas impacted by the load.

The "update" command is used in the refresh pipeline. It is safe to run (and safe to
re-run) to bring in new data.  It cannot be used to make a structural change. Basically,
"update" will respect the boundaries set by "publishing" the data warehouse state in S3 as "current".

The "upgrade" command should probably be used only during development. Think of an "upgrade" as a
"load" without the safety net. Unlike "load", it will upgrade more surgically and not expand
to modify entire schemas, but unlike "update" it will not go gently about it.
(It is a bit expensive given the cautious build up and permission changes.)

It is possible to bring up an "empty" data warehouse where all the structure exists
(meaning all the tables and views are in place) but no data was actually loaded.
This is used during the validation pipeline. See the "skip copy" options.

These are the general pre-requisites:

* "Tables" that have upstream sources must have CSV files and a manifest file from the "extract".

    * CSV files must have fields delimited by commas, quotes around fields if they
      contain a comma, and have doubled-up quotes if there's a quote within the field.

    * Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP

* "CTAS" tables are derived from queries so must have a SQL file. (Think of them as materialized views.)

    * For every derived table (CTAS) a SQL file must exist in S3 with a valid
      expression to create the content of the table (meaning: just the select without
      closing ';'). The actual DDL statement (CREATE TABLE AS ...) and the table
      attributes / constraints are added from the matching table design file.

    * Note that the target table is actually created empty, then CTAS is used for a temporary
      table which is then inserted into the table. This is needed to attach
      constraints, attributes, and column encodings.

* "VIEWS" are views and so must have a SQL file in S3.
"""

import logging
from contextlib import closing
from itertools import chain
from typing import Dict, List, Set

import psycopg2
from psycopg2.extensions import connection  # only for type annotation

import etl
import etl.dw
import etl.monitor
import etl.pg
import etl.relation
from etl.config.dw import DataWarehouseSchema
from etl.errors import (RelationModificationError, ETLRuntimeError, FailedConstraintError, RequiredRelationLoadError,
                        UpdateTableError)
from etl.names import join_column_list, join_with_quotes, TableName, TableSelector
from etl.relation import RelationDescription

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
    # Build up formatting string, then make just one call to format.
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
    type_lookup = dict([("primary_key", "PRIMARY KEY"),
                        ("surrogate_key", "PRIMARY KEY"),
                        ("unique", "UNIQUE"),
                        ("natural_key", "UNIQUE")])
    ddl_for_constraints = []
    for constraint in table_constraints:
        [[constraint_type, column_list]] = constraint.items()
        ddl_for_constraints.append("{} ( {} )".format(type_lookup[constraint_type], join_column_list(column_list)))
    return ddl_for_constraints


def build_table_attributes(table_design: dict, is_temp=False) -> List[str]:
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
    if distribution:
        if isinstance(distribution, list):
            ddl_attributes.append("DISTSTYLE KEY")
            ddl_attributes.append("DISTKEY ( {} )".format(join_column_list(distribution)))
        elif distribution == "all":
            ddl_attributes.append("DISTSTYLE ALL")
        elif distribution == "even":
            ddl_attributes.append("DISTSTYLE EVEN")
    if compound_sort:
        ddl_attributes.append("COMPOUND SORTKEY ( {} )".format(join_column_list(compound_sort)))
    elif interleaved_sort:
        ddl_attributes.append("INTERLEAVED SORTKEY ( {} )".format(join_column_list(interleaved_sort)))
    if is_temp:
        ddl_attributes.append("BACKUP NO")
    return ddl_attributes


def build_missing_dimension_row(columns: List[dict]) -> List[str]:
    """
    Return row that represents missing dimension values.
    """
    na_values_row = []
    for column in columns:
        if column.get("skipped", False):
            continue
        elif column.get("identity", False):
            na_values_row.append("0")
        else:
            # Use NULL for all null-able columns:
            if not column.get("not_null", False):
                # Use NULL for any nullable column and use type cast (for UNION ALL to succeed)
                na_values_row.append("NULL::{}".format(column["sql_type"]))
            elif "timestamp" in column["sql_type"]:
                na_values_row.append("'0000-01-01 00:00:00'")
            elif "boolean" in column["type"]:
                na_values_row.append("false")
            elif "string" in column["type"]:
                na_values_row.append("'N/A'")
            else:
                na_values_row.append("0")
    return na_values_row


def create_table(conn: connection, relation: RelationDescription, is_temp=False, dry_run=False) -> TableName:
    """
    Create a table matching this design (but possibly under another name, e.g. for temp tables).
    Return name of table that was created.

    Columns must have a name and a SQL type (compatible with Redshift).
    They may have an attribute of the compression encoding and the nullable constraint.

    Other column attributes and constraints should be resolved as table
    attributes (e.g. distkey) and table constraints (e.g. primary key).

    Tables may have attributes such as a distribution style and sort key.
    Depending on the distribution style, they may also have a distribution key.
    """
    columns = build_columns(relation.table_design["columns"], is_temp=is_temp)
    constraints = build_table_constraints(relation.table_design)
    attributes = build_table_attributes(relation.table_design, is_temp=is_temp)

    if is_temp:
        # TODO Start using an actual temp table (name starts with # and is session specific).
        table_name = TableName.from_identifier("{0.schema}.arthur_temp${0.table}".format(relation.target_table_name))
    else:
        table_name = relation.target_table_name

    ddl = """
        CREATE TABLE {table_name} (
            {columns_and_constraints}
        )
        {attributes}
        """.format(table_name=table_name,
                   columns_and_constraints=",\n".join(chain(columns, constraints)),
                   attributes="\n".join(attributes))
    if dry_run:
        logger.info("Dry-run: Skipping creating table '%s'", table_name.identifier)
        logger.debug("Skipped query: %s", ddl)
    else:
        logger.info("Creating table '%s'", table_name.identifier)
        etl.pg.execute(conn, ddl.replace('\n', "\n    "))

    return table_name


def create_view(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Create VIEW using the relation's query.
    """
    view_name = relation.target_table_name
    columns = join_column_list(relation.unquoted_columns)
    stmt = """CREATE VIEW {} (\n{}\n) AS\n{}""".format(view_name, columns, relation.query_stmt)
    if dry_run:
        logger.info("Dry-run: Skipping creating view '%s'", relation.identifier)
        logger.debug("Skipped statement: %s", stmt)
    else:
        logger.info("Creating view '%s'", relation.identifier)
        etl.pg.execute(conn, stmt)


def create_or_replace_relation(conn: connection, relation: RelationDescription, schema: DataWarehouseSchema,
                               dry_run=False) -> None:
    """
    Create fresh VIEW or TABLE and grant groups access permissions.

    Note that we cannot use CREATE OR REPLACE statements since we want to allow going back and forth
    between VIEW and TABLE (or in table design terms: VIEW and CTAS).
    """
    try:
        drop_relation_if_exists(conn, relation, dry_run=dry_run)
        if relation.is_view_relation:
            create_view(conn, relation, dry_run=dry_run)
        else:
            create_table(conn, relation, dry_run=dry_run)
        grant_access(conn, relation, schema, dry_run=dry_run)
    except Exception as exc:
        raise RelationModificationError(exc) from exc


def drop_relation_if_exists(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run either DROP VIEW IF EXISTS or DROP TABLE IF EXISTS depending on type of existing relation.
    (It's ok if the relation doesn't already exist.)
    """
    try:
        kind = etl.pg.relation_kind(conn, relation.target_table_name.schema, relation.target_table_name.table)
        if kind is not None:
            stmt = """DROP {} {} CASCADE""".format(kind, relation)
            if dry_run:
                logger.info("Dry-run: Skipping dropping %s '%s'", kind.lower(), relation.identifier)
                logger.debug("Skipped statement: %s", stmt)
            else:
                logger.info("Dropping %s '%s'", kind.lower(), relation.identifier)
                etl.pg.execute(conn, stmt)
    except Exception as exc:
        raise RelationModificationError(exc) from exc


def grant_access(conn: connection, relation: RelationDescription, schema: DataWarehouseSchema, dry_run=False):
    """
    Grant privileges on (new) relation based on configuration.

    We always grant all privileges to the ETL user. We may grant read-only access
    or read-write access based on configuration. Note that the access is always based on groups, not users.
    """
    target = relation.target_table_name
    owner, reader_groups, writer_groups = schema.owner, schema.reader_groups, schema.writer_groups

    if dry_run:
        logger.info("Dry-run: Skipping grant of all privileges on '%s' to '%s'", relation.identifier, owner)
    else:
        logger.info("Granting all privileges on '%s' to '%s'", relation.identifier, owner)
        etl.pg.grant_all_to_user(conn, target.schema, target.table, owner)

    if reader_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of select access on '%s' to %s",
                        relation.identifier, join_with_quotes(reader_groups))
        else:
            logger.info("Granting select access on '%s' to %s", relation.identifier, join_with_quotes(reader_groups))
            for reader in reader_groups:
                etl.pg.grant_select(conn, target.schema, target.table, reader)

    if writer_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of write access on '%s' to %s",
                        relation.identifier, join_with_quotes(writer_groups))
        else:
            logger.info("Granting write access on '%s' to %s", relation.identifier, join_with_quotes(writer_groups))
            for writer in writer_groups:
                etl.pg.grant_select_and_write(conn, target.schema, target.table, writer)


def delete_whole_table(conn: connection, table: RelationDescription, dry_run=False) -> None:
    if dry_run:
        logger.info("Dry-run: Skipping deletion of '%s'", table.identifier)
    else:
        logger.info("Deleting all rows in table '%s'", table.identifier)
        etl.pg.execute(conn, "DELETE FROM {}".format(table))


def copy_data(conn: connection, relation: RelationDescription, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.
    A manifest for the CSV files must be provided -- it is an error if the manifest is missing.
    """
    aws_iam_role = etl.config.get_data_lake_config("iam_role")
    credentials = "aws_iam_role={}".format(aws_iam_role)
    s3_uri = "s3://{}/{}".format(relation.bucket_name, relation.manifest_file_name)

    # N.B. If you change the COPY options, make sure to change the documentation at the top of the file.
    stmt = """
        COPY {}
        FROM %s
        CREDENTIALS %s MANIFEST
        DELIMITER ',' ESCAPE REMOVEQUOTES GZIP
        TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
        TRUNCATECOLUMNS
        STATUPDATE OFF
        """
    # TODO Measure COMPUPDATE OFF
    # TODO Enable using "NOLOAD" to test whether CSV files are valid.
    if dry_run:
        # stmt += " NOLOAD"
        logger.info("Dry-run: Skipping copying data into '%s' from '%s'", relation.identifier, s3_uri)
        return

    logger.info("Copying data into '%s' from '%s'", relation.identifier, s3_uri)
    try:
        etl.pg.execute(conn, stmt.format(relation), (s3_uri, credentials))
        row_count = etl.pg.query(conn, "SELECT pg_last_copy_count()")
        logger.info("Copied %d rows into '%s'", row_count[0][0], relation.identifier)
    except psycopg2.Error as exc:
        if "stl_load_errors" in exc.pgerror:
            logger.debug("Trying to get error message from stl_log_errors table")
            info = etl.pg.query(conn, """
                SELECT query, starttime, filename, colname, type, col_length,
                       line_number, position, err_code, err_reason
                  FROM stl_load_errors
                 WHERE session = pg_backend_pid()
                 ORDER BY starttime DESC
                 LIMIT 1""")
            values = "  \n".join(["{}: {}".format(k, row[k]) for row in info for k in row.keys()])
            logger.info("Information from stl_load_errors:\n  %s", values)
        raise


def insert_from_query(conn: connection, table_name: TableName, columns: List[str], query: str, dry_run=False) -> None:
    """
    Load data into table from its query (aka materializing a view). The table name must be specified since
    the load goes either into the target table or a temporary one.
    """
    stmt_template = """
        INSERT INTO {table} (
          {columns}
        ) (
          {query}
        )
    """
    stmt = stmt_template.format(table=table_name, columns=join_column_list(columns), query=query)

    if dry_run:
        logger.info("Dry-run: Skipping query to load data into '%s'", table_name.identifier)
        logger.debug("Skipped query: %s", stmt)
    else:
        logger.info("Loading data into %s from query", table_name.identifier)
        etl.pg.execute(conn, stmt)


def load_ctas_directly(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run query to fill CTAS relation. (Not to be used for dimensions etc.)
    """
    insert_from_query(conn, relation.target_table_name, relation.unquoted_columns, relation.query_stmt, dry_run=dry_run)


def load_ctas_using_temp_table(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run query to fill temp table, then copy data (possibly along with missing dimension) into CTAS relation.
    """
    temp_name = create_table(conn, relation, is_temp=True, dry_run=dry_run)
    temp_columns = [column["name"] for column in relation.table_design["columns"]
                    if not (column.get("skipped") or column.get("identity"))]
    insert_from_query(conn, temp_name, temp_columns, relation.query_stmt, dry_run=dry_run)

    inner_stmt = "SELECT {} FROM {}".format(join_column_list(relation.unquoted_columns), temp_name)
    if relation.target_table_name.table.startswith("dim_"):
        missing_dimension = build_missing_dimension_row(relation.table_design["columns"])
        inner_stmt += " UNION ALL SELECT {}".format(", ".join(missing_dimension))
    insert_from_query(conn, relation.target_table_name, relation.unquoted_columns, inner_stmt, dry_run=dry_run)

    # Until we make it actually temporary:
    if dry_run:
        logger.info("Dry-run: Skipping dropping of temporary table '%s'", temp_name.identifier)
    else:
        logger.info("Dropping temporary table '%s'", temp_name.identifier)
        etl.pg.execute(conn, "DROP TABLE {}".format(temp_name))


def analyze(conn: connection, table: RelationDescription, dry_run=False) -> None:
    """
    Update table statistics.
    """
    if dry_run:
        logger.info("Dry-run: Skipping analysis of '%s'", table.identifier)
    else:
        logger.info("Running analyze step on table '%s'", table.identifier)
        etl.pg.execute(conn, "ANALYZE {}".format(table))


def update_table(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Update table contents either from CSV files from upstream sources or by running some SQL
    for CTAS relations. This assumes that the table was previously created.

    For tables backed by upstream sources, data is copied in.

    If the CTAS doesn't have a key (no identity column), then values are inserted straight from a view.

    If a column is marked as being a key (identity is true), then a temporary table is built from
    the query and then copied into the "CTAS" relation. If the name of the relation starts with "dim_",
    then it's assumed to be a dimension and a row with missing values (mostly 0, false, etc.) is added as well.
    """
    try:
        if relation.is_ctas_relation:
            if relation.has_identity_column:
                load_ctas_using_temp_table(conn, relation, dry_run=dry_run)
            else:
                load_ctas_directly(conn, relation, dry_run=dry_run)
        else:
            copy_data(conn, relation, dry_run=dry_run)
        analyze(conn, relation, dry_run=dry_run)
    except Exception as exc:
        raise UpdateTableError(exc) from exc


def evaluate_execution_order(relations: List[RelationDescription], selector: TableSelector,
                             include_dependents=False) -> List[RelationDescription]:
    """
    Filter the list of relation descriptions by the selector. Optionally, expand the list to the dependents
    of the selected relations.
    """
    execution_order = etl.relation.order_by_dependencies(relations)
    selected = etl.relation.find_matches(execution_order, selector)
    if not include_dependents:
        return selected

    dependents = etl.relation.find_dependents(execution_order, selected)
    combined = frozenset(relation.identifier for relation in chain(selected, dependents))
    return [relation for relation in execution_order if relation.identifier in combined]


def find_traversed_schemas(relations: List[RelationDescription]) -> List[DataWarehouseSchema]:
    """
    Return schemas traversed when refreshing relations (in order that they are needed).
    """
    got_it = set()  # type: Set[str]
    traversed_in_order = []
    for relation in relations:
        this_schema = relation.dw_schema
        if this_schema.name not in got_it:
            got_it.add(this_schema.name)
            traversed_in_order.append(this_schema)
    return traversed_in_order


def build_monitor_info(relations: List[RelationDescription], step: str, dbname: str) -> dict:
    monitor_info = {}
    base_index = {"name": dbname, "current": 0, "final": len(relations)}
    for i, relation in enumerate(relations):
        source = dict(bucket_name=relation.bucket_name)
        if relation.is_view_relation or relation.is_ctas_relation:
            source['object_key'] = relation.sql_file_name
        else:
            source['object_key'] = relation.manifest_file_name
        destination = relation.target_table_name.to_dict()
        destination['name'] = dbname
        monitor_info[relation.identifier] = {
            "target": relation.identifier,
            "step": step,
            "source": source,
            "destination": destination,
            "index": dict(base_index, current=i + 1)
        }
    return monitor_info


def build_single_relation(conn, relation, skip_copy=False, dry_run=False, **kwargs):
    """
    Make a single relation appear in its schema. Any keyword args are passed to the monitor.
    """
    with etl.monitor.Monitor(dry_run=dry_run, **kwargs):
        create_or_replace_relation(conn, relation, relation.dw_schema, dry_run=dry_run)
        if not (skip_copy or relation.is_view_relation):
            update_table(conn, relation, dry_run=dry_run)
            verify_constraints(conn, relation, dry_run=dry_run)


def build_relations(dsn_etl: Dict[str, str], relations: List[RelationDescription], command: str,
                    skip_copy=False, dry_run=False) -> None:
    """
    "Building" relations refers to creating them, granting access, and if they should hold data, load them.

    The process stops if there is an error on a required relation.
    The process continues if the error happened on a non-required relation but note that all dependencies
    of the non-required relation will then be left empty.
    """
    monitor_info = build_monitor_info(relations, command, dsn_etl["database"])
    skip_after_prior_fail = set()  # type: Set[str]
    with closing(etl.pg.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for relation in relations:
            if relation.identifier in skip_after_prior_fail:
                logger.warning("Skipping %s for relation '%s' due to failed dependencies", command, relation.identifier)
                continue
            try:
                build_single_relation(conn, relation, skip_copy=skip_copy, dry_run=dry_run,
                                      **monitor_info[relation.identifier])
            except ETLRuntimeError as exc:
                dependent_relations = etl.relation.find_dependents(relations, [relation])
                dependent_required_relations = [relation for relation in dependent_relations if relation.is_required]
                if relation.is_required or dependent_required_relations:
                    raise RequiredRelationLoadError(relation, dependent_required_relations) from exc
                logger.warning("This failure for '%s' does not harm any required relations:", relation.identifier,
                               exc_info=True)
                # Make sure we don't try to load any of the dependents
                if dependent_relations:
                    dependent_identifiers = [relation.identifier for relation in dependent_relations]
                    skip_after_prior_fail.update(dependent_identifiers)
                    logger.warning("Continuing %s omitting these dependent relations: %s", command,
                                   join_with_quotes(dependent_identifiers))


def load_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                        skip_copy=False, no_rollback=False, dry_run=False):
    """
    Fully "load" the data warehouse after creating a blank slate by moving existing schemas out of the way.

    Check that only complete schemas are selected. Error out if not.

    1 Determine schemas that house any of the selected or dependent relations.
    2 Move old schemas in the data warehouse out of the way (for "backup").
    3 Create new schemas (and give access)
    4 Loop over all relations in selected schemas:
      4.1 Create relation (and give access)
      4.2 Load data into tables or CTAS (no further action for views)
          If it's a source table, use COPY to load data.
          If it's a CTAS with an identity column, create temp table, then move data into final table.
          If it's a CTAS without an identity column, insert values straight into final table.
    On error: restore the backup (unless asked not to rollback)

    N.B. If arthur gets interrupted (eg. because the instance is inadvertently shut down),
    then there will be an incomplete state.
    """
    relations = evaluate_execution_order(all_relations, selector, include_dependents=True)
    if not relations:
        logger.warning("Found no relations matching: %s", selector)
        return
    traversed_schemas = find_traversed_schemas(relations)
    logger.info("Starting to load %s relation(s) in %d schema(s)", len(relations), len(traversed_schemas))

    dsn_etl = etl.config.get_dw_config().dsn_etl
    etl.dw.backup_schemas(dsn_etl, traversed_schemas, dry_run=dry_run)
    try:
        etl.dw.create_schemas(dsn_etl, traversed_schemas, dry_run=dry_run)
        build_relations(dsn_etl, relations, "load", skip_copy=skip_copy, dry_run=dry_run)
    except ETLRuntimeError:
        if not no_rollback:
            etl.dw.restore_schemas(dsn_etl, traversed_schemas, dry_run=dry_run)
        raise


def upgrade_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                           only_selected=False, skip_copy=False, dry_run=False):
    """
    Push new (structural) changes and fresh data through data warehouse.

    This will create schemas as needed to house the relations being created or replaced.
    The set of relations is usually expanded to include all those in (transitively) depending
    on the selected ones. But this can be kept to just the selected ones for faster testing.

    For all relations:
        1 Drop relation
        2.Create relation and grant access to the relation
        3 Unless skip_copy is true (else leave tables empty):
            3.1 Load data into tables
            3.2 Verify constraints
    """
    relations = evaluate_execution_order(all_relations, selector, include_dependents=not only_selected)
    if not relations:
        logger.warning("Found no relations matching: %s", selector)
        return
    traversed_schemas = find_traversed_schemas(relations)
    logger.info("Starting to upgrade %s relation(s) in %d schema(s)", len(relations), len(traversed_schemas))

    dsn_etl = etl.config.get_dw_config().dsn_etl
    etl.dw.create_missing_schemas(dsn_etl, traversed_schemas, dry_run=dry_run)
    build_relations(dsn_etl, relations, "upgrade", skip_copy=skip_copy, dry_run=dry_run)


def update_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                          only_selected=False, run_vacuum=False, dry_run=False):
    """
    Let new data percolate through the data warehouse.

    Within a transaction:
        Iterate over relations (selected or (selected and transitively dependent)):
            1 Delete rows
            2.Load data from upstream sources using COPY command, load data into CTAS using views for queries
            3 Verify constraints

    Note that a failure will rollback the transaction -- there is no distinction between required or not-required.
    Finally, if elected, run vacuum (in new connection) for all tables that were modified.
    """
    relations = evaluate_execution_order(all_relations, selector, include_dependents=not only_selected)
    tables = [relation for relation in relations if not relation.is_view_relation]
    if not tables:
        logger.warning("Found no tables matching: %s", selector)
        return
    logger.info("Starting to update %s tables(s)", len(tables))

    dsn_etl = etl.config.get_dw_config().dsn_etl
    monitor_info = build_monitor_info(tables, "update", dsn_etl["database"])

    # Run update within a transaction:
    with closing(etl.pg.connection(dsn_etl, readonly=dry_run)) as conn, conn as tx:
        for relation in tables:
            monitor_info[relation.identifier]["dry_run"] = dry_run  # make type checker happy
            with etl.monitor.Monitor(**monitor_info[relation.identifier]):
                delete_whole_table(tx, relation, dry_run=dry_run)
                update_table(tx, relation, dry_run=dry_run)
                verify_constraints(tx, relation, dry_run=dry_run)

    if run_vacuum:
        vacuum(dsn_etl, tables, dry_run=dry_run)


def vacuum(dsn_etl: Dict[str, str], relations: List[RelationDescription], dry_run=False) -> None:
    """
    Final step ... tidy up the warehouse before guests come over.

    This needs to open a new connection since it needs to happen outside a transaction.
    """
    with closing(etl.pg.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for relation in relations:
            if dry_run:
                logger.info("Dry-run: Skipping vacuum of '%s'", relation.identifier)
            else:
                logger.info("Running vacuum step on table '%s'", relation.identifier)
                etl.pg.execute(conn, "VACUUM {}".format(relation))


def verify_constraints(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Raises a FailedConstraintError if :relation's target table doesn't obey its declared constraints.

    Note that NULL in SQL is never equal to another value. This means for unique constraints that
    rows where (at least) one column is null are not equal even if they have the same values in the
    not-null columns.  See description of unique index in the PostgreSQL documentation:
    https://www.postgresql.org/docs/8.1/static/indexes-unique.html

    For constraints that check "key" values (like 'primary_key'), this warning does not apply since
    the columns must be not null anyways.

    > "Note that a unique constraint does not, by itself, provide a unique identifier because it
    > does not exclude null values."
    https://www.postgresql.org/docs/8.1/static/ddl-constraints.html
    """
    constraints = relation.table_design.get("constraints")
    if constraints is None:
        logger.info("No constraints to verify for '%s'", relation.identifier)
        return

    # To make this work in DataGrip, define '\{(\w+)\}' under Tools -> Database -> User Parameters.
    # Then execute the SQL using command-enter, enter the values for `cols` and `table`, et voila!
    statement_template = """
        SELECT DISTINCT
               {columns}
          FROM {table}
         WHERE {condition}
      GROUP BY {columns}
        HAVING COUNT(*) > 1
         LIMIT 5
    """

    for constraint in constraints:
        [[constraint_type, columns]] = constraint.items()  # There will always be exactly one item.
        quoted_columns = join_column_list(columns)
        if constraint_type == "unique":
            condition = " AND ".join('"{}" IS NOT NULL'.format(name) for name in columns)
        else:
            condition = "TRUE"
        statement = statement_template.format(columns=quoted_columns, table=relation, condition=condition)
        if dry_run:
            logger.info("Dry-run: Skipping check of %s constraint in '%s' on [%s]",
                        constraint_type, relation.identifier, join_with_quotes(columns))
            logger.debug("Skipped query:\n%s", statement)
        else:
            logger.info("Checking %s constraint in '%s' on [%s]",
                        constraint_type, relation.identifier, join_with_quotes(columns))
            results = etl.pg.query(conn, statement)
            if results:
                raise FailedConstraintError(relation, constraint_type, columns, results)


def show_dependents(relations: List[RelationDescription], selector: TableSelector):
    """
    List the execution order of loads or updates.

    Relations are marked based on whether they were directly selected or selected as
    part of the propagation of an update or upgrade.
    They are also marked whether they'd lead to a fatal error since they're required for full load.
    """
    complete_sequence = evaluate_execution_order(relations, selector, include_dependents=True)
    selected_relations = etl.relation.find_matches(complete_sequence, selector)

    if len(selected_relations) == 0:
        logger.warning("Found no matching relations for: %s", selector)
        return

    selected = frozenset(relation.identifier for relation in selected_relations)
    immediate = set(selected)
    for relation in complete_sequence:
        if relation.is_view_relation and any(name in immediate for name in relation.dependencies):
            immediate.add(relation.identifier)
    immediate -= selected
    logger.info("Execution order includes %d selected, %d immediate, and %d other downstream relation(s)",
                len(selected), len(immediate), len(complete_sequence) - len(selected) - len(immediate))

    max_len = max(len(relation.identifier) for relation in complete_sequence)
    for i, relation in enumerate(complete_sequence):
        if relation.identifier in selected:
            flag = "selected"
        elif relation.identifier in immediate:
            flag = "immediate"
        else:
            flag = "downstream"
        if relation.is_required:
            flag += ", required"
        print("{index:4d} {identifier:{width}s} ({relation_kind}) ({flag})".format(
            index=i + 1, identifier=relation.identifier, width=max_len, relation_kind=relation.kind, flag=flag))
