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
(It is a bit expensive still given the cautious build up and permission changes.)

It is possible to bring up an "empty" data warehouse where all the structure exists
(meaning all the tables and views are in place) but no data was actually loaded.
This is used during the validation pipeline. See the "skip copy" options.

These are the general pre-requisites:

    * "Tables" that have upstream sources must have CSV files and a manifest file from the "extract".

    * "CTAS" tables are derived from queries so must have a SQL file. (Think of them as materialized views.)

        * For every derived table (CTAS) a SQL file must exist in S3 with a valid
          expression to create the content of the table (meaning: just the select without
          closing ';'). The actual DDL statement (CREATE TABLE AS ...) and the table
          attributes / constraints are added from the matching table design file.

    * "VIEWS" are views and so must have a SQL file in S3.
"""

import concurrent.futures
import logging
import textwrap
from contextlib import closing
from itertools import chain
from typing import Dict, Iterable, List, Set

from psycopg2.extensions import connection  # only for type annotation

import etl
import etl.data_warehouse
import etl.monitor
import etl.db
import etl.design.redshift
import etl.relation
from etl.config.dw import DataWarehouseSchema
from etl.errors import (ETLRuntimeError, FailedConstraintError, MissingManifestError, RelationDataError,
                        RelationConstructionError, RequiredRelationLoadError, UpdateTableError)
from etl.names import join_column_list, join_with_quotes, TableName, TableSelector
from etl.relation import RelationDescription
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ---- Section 1: Functions that work on relations (creating them, filling them, permissioning them) ----

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
    if is_temp:
        # TODO Start using an actual temp table (name starts with # and is session specific).
        table_name = TableName.from_identifier("{0.schema}.arthur_temp${0.table}".format(relation.target_table_name))
    else:
        table_name = relation.target_table_name

    ddl_stmt = etl.design.redshift.build_table_ddl(relation.table_design, table_name, is_temp=is_temp)
    etl.db.run(conn, "Creating table '{:x}'".format(table_name), ddl_stmt, dry_run=dry_run)

    return table_name


def create_view(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Create VIEW using the relation's query.
    """
    view_name = relation.target_table_name
    columns = join_column_list(relation.unquoted_columns)
    stmt = """CREATE VIEW {} (\n{}\n) AS\n{}""".format(view_name, columns, relation.query_stmt)
    etl.db.run(conn, "Creating view '{:x}'".format(relation), stmt, dry_run=dry_run)


def drop_relation_if_exists(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run either DROP VIEW or DROP TABLE depending on type of existing relation. It's ok if the relation
    doesn't already exist.
    """
    try:
        kind = etl.db.relation_kind(conn, relation.target_table_name.schema, relation.target_table_name.table)
        if kind is not None:
            stmt = """DROP {} {} CASCADE""".format(kind, relation)
            etl.db.run(conn, "Dropping {} '{:x}'".format(kind.lower(), relation), stmt, dry_run=dry_run)
    except Exception as exc:
        raise RelationConstructionError(exc) from exc


def create_or_replace_relation(conn: connection, relation: RelationDescription, dry_run=False) -> None:
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
        grant_access(conn, relation, dry_run=dry_run)
    except Exception as exc:
        raise RelationConstructionError(exc) from exc


def grant_access(conn: connection, relation: RelationDescription, dry_run=False):
    """
    Grant privileges on (new) relation based on configuration.

    We always grant all privileges to the ETL user. We may grant read-only access
    or read-write access based on configuration. Note that the access is always based on groups, not users.
    """
    target = relation.target_table_name
    schema = relation.dw_schema
    owner, reader_groups, writer_groups = schema.owner, schema.reader_groups, schema.writer_groups

    if dry_run:
        logger.info("Dry-run: Skipping grant of all privileges on '%s' to '%s'", relation.identifier, owner)
    else:
        logger.info("Granting all privileges on '%s' to '%s'", relation.identifier, owner)
        etl.db.grant_all_to_user(conn, target.schema, target.table, owner)

    if reader_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of select access on '%s' to %s",
                        relation.identifier, join_with_quotes(reader_groups))
        else:
            logger.info("Granting select access on '%s' to %s", relation.identifier, join_with_quotes(reader_groups))
            for reader in reader_groups:
                etl.db.grant_select(conn, target.schema, target.table, reader)

    if writer_groups:
        if dry_run:
            logger.info("Dry-run: Skipping granting of write access on '%s' to %s",
                        relation.identifier, join_with_quotes(writer_groups))
        else:
            logger.info("Granting write access on '%s' to %s", relation.identifier, join_with_quotes(writer_groups))
            for writer in writer_groups:
                etl.db.grant_select_and_write(conn, target.schema, target.table, writer)


def delete_whole_table(conn: connection, table: RelationDescription, dry_run=False) -> None:
    """
    Delete all rows from this table.
    """
    stmt = """DELETE FROM {}""".format(table)
    etl.db.run(conn, "Deleting all rows in table '{:x}'".format(table), stmt, dry_run=dry_run)


def copy_data(conn: connection, relation: RelationDescription, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.
    A manifest for the CSV files must be provided -- it is an error if the manifest is missing.
    """
    aws_iam_role = etl.config.get_data_lake_config("iam_role")
    s3_uri = "s3://{}/{}".format(relation.bucket_name, relation.manifest_file_name)

    if not relation.has_manifest:
        if dry_run:
            logger.info("Dry-run: Ignoring missing manifest file '%s'", s3_uri)
        else:
            raise MissingManifestError("relation '{}' is missing its manifest file".format(relation.identifier))

    etl.design.redshift.copy_from_uri(conn, relation.target_table_name, s3_uri, aws_iam_role, dry_run=dry_run)


def insert_from_query(conn: connection, table_name: TableName, columns: List[str], query: str, dry_run=False) -> None:
    """
    Load data into table from its query (aka materializing a view). The table name must be specified since
    the load goes either into the target table or a temporary one.
    """
    stmt = """INSERT INTO {table} ( {columns} )""".format(table=table_name, columns=join_column_list(columns))
    stmt += "\n" + query
    etl.db.run(conn, "Loading data into '{:x}' from query".format(table_name), stmt, dry_run=dry_run)


def load_ctas_directly(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run query to fill CTAS relation. (Not to be used for dimensions etc.)
    """
    insert_from_query(conn, relation.target_table_name, relation.unquoted_columns, relation.query_stmt, dry_run=dry_run)


def create_missing_dimension_row(columns: List[dict]) -> List[str]:
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


def load_ctas_using_temp_table(conn: connection, relation: RelationDescription, dry_run=False) -> None:
    """
    Run query to fill temp table, then copy data (possibly along with missing dimension) into CTAS relation.
    """
    temp_name = create_table(conn, relation, is_temp=True, dry_run=dry_run)
    temp_columns = [column["name"] for column in relation.table_design["columns"]
                    if not (column.get("skipped") or column.get("identity"))]

    try:
        insert_from_query(conn, temp_name, temp_columns, relation.query_stmt, dry_run=dry_run)

        inner_stmt = "SELECT {} FROM {}".format(join_column_list(relation.unquoted_columns), temp_name)
        if relation.target_table_name.table.startswith("dim_"):
            missing_dimension = create_missing_dimension_row(relation.table_design["columns"])
            inner_stmt += "\nUNION ALL SELECT {}".format(", ".join(missing_dimension))

        insert_from_query(conn, relation.target_table_name, relation.unquoted_columns, inner_stmt, dry_run=dry_run)
    finally:
        # Until we make it actually temporary:
        stmt = "DROP TABLE {}".format(temp_name)
        etl.db.run(conn, "Dropping temporary table '{:x}'".format(temp_name), stmt, dry_run=dry_run)


def analyze(conn: connection, table: RelationDescription, dry_run=False) -> None:
    """
    Update table statistics.
    """
    etl.db.run(conn, "Running analyze step on table '{:x}'".format(table), "ANALYZE {}".format(table), dry_run=dry_run)


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
         LIMIT {limit}
        """
    limit = 5  # arbitrarily chosen limit of examples to show

    for constraint in constraints:
        [[constraint_type, columns]] = constraint.items()  # There will always be exactly one item.
        quoted_columns = join_column_list(columns)
        if constraint_type == "unique":
            condition = " AND ".join('"{}" IS NOT NULL'.format(name) for name in columns)
        else:
            condition = "TRUE"
        statement = statement_template.format(columns=quoted_columns, table=relation, condition=condition, limit=limit)
        if dry_run:
            logger.info("Dry-run: Skipping check of %s constraint in '%s' on column(s): %s",
                        constraint_type, relation.identifier, join_with_quotes(columns))
            etl.db.skip_query(conn, statement)
        else:
            logger.info("Checking %s constraint in '%s' on column(s): %s",
                        constraint_type, relation.identifier, join_with_quotes(columns))
            results = etl.db.query(conn, statement)
            if results:
                if len(results) == limit:
                    logger.error("Constraint check failed on at least %d row(s)", len(results))
                else:
                    logger.error("Constraint check failed on %d row(s)", len(results))
                raise FailedConstraintError(relation, constraint_type, columns, results)


# ---- Section 2: Functions that work on schemas ----

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


# ---- Section 3: Functions that tie table operations together ----

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


def build_one_relation(conn, relation, use_delete=False, skip_copy=False, dry_run=False, **kwargs) -> None:
    """
    Empty out tables (either with delete or by create-or-replacing them) and fill 'em up.
    Unless in delete mode, this always makes sure tables and views are created.

    Within transaction? Only applies to tables which get emptied and then potentially filled again.
    Not in transaction? Drop and create all relations, for tables, also potentially fill 'em up again.
    """
    monitor_info = dict(**kwargs)
    monitor_info["dry_run"] = dry_run
    with etl.monitor.Monitor(**monitor_info):
        # Step 1 -- clear out existing data (by deletion or by re-creation)
        if use_delete:
            if not relation.is_view_relation:
                delete_whole_table(conn, relation, dry_run=dry_run)
        else:
            create_or_replace_relation(conn, relation, dry_run=dry_run)
        # Step 2 -- load data (and verify)
        if not relation.is_view_relation:
            if skip_copy:
                logger.info("Skipping loading data into '%s'", relation.identifier)
            else:
                # TODO Should we delete_whole_table if constraints are violated? Or Update & verify in tx?
                update_table(conn, relation, dry_run=dry_run)
                verify_constraints(conn, relation, dry_run=dry_run)


def build_one_relation_using_pool(pool, relation, skip_copy=False, dry_run=False, **kwargs) -> None:
    conn = pool.getconn()
    try:
        build_one_relation(conn, relation, skip_copy=skip_copy, dry_run=dry_run, **kwargs)
    finally:
        pool.putconn(conn)


def vacuum(relations: List[RelationDescription], dry_run=False) -> None:
    """
    Final step ... tidy up the warehouse before guests come over.

    This needs to open a new connection since it needs to happen outside a transaction.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with Timer() as timer, closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for relation in relations:
            etl.db.run(conn, "Running vacuum on '{:x}'".format(relation), "VACUUM {}".format(relation), dry_run=dry_run)
        if not dry_run:
            logger.info("Ran vacuum for %d table(s) (%s)", len(relations), timer)


# ---- Section 4: Functions related to control flow ----

def evaluate_execution_order(relations: List[RelationDescription], selector: TableSelector,
                             include_dependents=False) -> List[RelationDescription]:
    """
    Filter the list of relation descriptions by the selector. Optionally, expand the list to the dependents
    of the selected relations.
    """
    logger.info("Pondering execution order of %d relation(s)", len(relations))
    execution_order = etl.relation.order_by_dependencies(relations)
    selected = etl.relation.find_matches(execution_order, selector)
    if not include_dependents:
        return selected

    dependents = etl.relation.find_dependents(execution_order, selected)
    combined = frozenset(relation.identifier for relation in chain(selected, dependents))
    return [relation for relation in execution_order if relation.identifier in combined]


def build_monitor_info(relations: List[RelationDescription], step: str, dbname: str) -> dict:
    # FIXME Move into new class
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


def create_source_tables_with_data(relations: List[RelationDescription], monitor_info: dict,
                                   max_concurrency=1, skip_copy=False, dry_run=False) -> List[str]:
    """
    Pick out all tables in source schemas from the list of relations and build up just those.
    Return list of identifiers for relations that are now empty (failed to load).

    Since we know that these relations never have dependencies, we don't have to track any
    failures while loading them.
    """
    source_relations = [relation for relation in relations if not relation.is_transformation]
    if not source_relations:
        logger.info("None of the relations are in source schemas")

    timer = Timer()
    dsn_etl = etl.config.get_dw_config().dsn_etl
    pool = etl.db.connection_pool(max_concurrency, dsn_etl)
    futures = {}  # type: Dict[str, concurrent.futures.Future]
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            for relation in source_relations:
                future = executor.submit(build_one_relation_using_pool, pool, relation, skip_copy=skip_copy,
                                         dry_run=dry_run, **monitor_info[relation.identifier])
                futures[relation.identifier] = future
            # TODO for fail fast, switch to FIRST_EXCEPTION
            done, not_done = concurrent.futures.wait(futures.values(), return_when=concurrent.futures.ALL_COMPLETED)
            cancelled = [future for future in not_done if future.cancel()]
            logger.info("Wrapping up work in %d worker(s): %d done, %d not done (%d cancelled) (%s)",
                        max_concurrency, len(done), len(not_done), len(cancelled), timer)
    finally:
        pool.closeall()

    failed = []  # type: List[str]
    failed_and_required = []  # type: List[str]
    for relation in source_relations:
        try:
            futures[relation.identifier].result()
        except concurrent.futures.CancelledError:
            pass
        except (RelationConstructionError, RelationDataError) as exc:
            failed.append(relation.identifier)
            if relation.is_required:
                logger.error("Exception information for required relation '%s': %s", relation.identifier, exc)
                failed_and_required.append(relation.identifier)
            else:
                logger.warning("Exception information for relation '%s': %s", relation.identifier, exc)

    if failed_and_required:
        raise RequiredRelationLoadError(failed_and_required)
    if failed:
        logger.warning("These %d relation(s) failed to build: %s", len(failed), failed)
    logger.info("Finished with %d relation(s) in source schemas (%s)", len(source_relations), timer)
    return failed


def create_transformations_with_data(relations: List[RelationDescription], monitor_info: dict,
                                     failed_before: Iterable[str], skip_copy=False, dry_run=False) -> None:
    """
    Pick out all tables in transformation schemas from the list of relations and build up just those.

    These relations may be dependent on relations that may have failed before we reached them.
    If dependencies were left empty, we'll fall back to skip_copy mode.
    Unless the relation is "required" in which case we abort here.
    """
    transformations = [relation for relation in relations if relation.is_transformation]
    if not transformations:
        logger.info("None of the relations are in transformation schemas")

    dsn_etl = etl.config.get_dw_config().dsn_etl
    failed_relations = []  # type: List[RelationDescription]
    # TODO move state of "failed"ness into relation description
    skip_copy_after_prior_fail = set(failed_before)  # type: Set[str]

    timer = Timer()
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for relation in transformations:
            this_skip_copy = skip_copy or relation.identifier in skip_copy_after_prior_fail
            try:
                build_one_relation(conn, relation, skip_copy=this_skip_copy, dry_run=dry_run,
                                   **monitor_info[relation.identifier])
            except (RelationConstructionError, RelationDataError) as exc:
                if relation.is_required:
                    raise RequiredRelationLoadError([relation.identifier]) from exc
                dependent_relations = etl.relation.find_dependents(transformations, [relation])
                dependent_required = [relation.identifier for relation in dependent_relations if relation.is_required]
                if dependent_required:
                    raise RequiredRelationLoadError(dependent_required, relation.identifier) from exc
                logger.warning("Failed relation '%s' is not required, ignoring this exception:",
                               relation.identifier, exc_info=True)
                failed_relations.append(relation)
                if dependent_relations:
                    dependents = [relation.identifier for relation in dependent_relations]
                    skip_copy_after_prior_fail.update(dependents)
                    logger.warning("Continuing while omitting dependent relation(s): %s", join_with_quotes(dependents))

    if failed_relations:
        logger.warning("These %d relation(s) failed to build: %s", len(failed_relations),
                       join_with_quotes(relation.identifier for relation in failed_relations))
    if skip_copy_after_prior_fail:
        logger.warning("These %d relation(s) were left empty: %s", len(skip_copy_after_prior_fail),
                       join_with_quotes(skip_copy_after_prior_fail))
    logger.info("Finished with %d relation(s) in transformation schemas (%s)", len(transformations), timer)


def create_relations_with_data(relations: List[RelationDescription], command: str,
                               max_concurrency=1, skip_copy=False, dry_run=False) -> None:
    """
    "Building" relations refers to creating them, granting access, and if they should hold data, loading them.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    monitor_info = build_monitor_info(relations, command, dsn_etl["database"])

    failed = create_source_tables_with_data(relations, monitor_info,
                                            max_concurrency, skip_copy=skip_copy, dry_run=dry_run)
    create_transformations_with_data(relations, monitor_info, failed_before=failed,
                                     skip_copy=skip_copy, dry_run=dry_run)


# ---- Section 5: "Callbacks" (functions that implement commands) ----

def load_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                        max_concurrency=1, skip_copy=False, no_rollback=False, dry_run=False):
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

    etl.data_warehouse.backup_schemas(traversed_schemas, dry_run=dry_run)
    try:
        etl.data_warehouse.create_schemas(traversed_schemas, dry_run=dry_run)
        create_relations_with_data(relations, "load", max_concurrency, skip_copy=skip_copy, dry_run=dry_run)
    except ETLRuntimeError:
        if not no_rollback:
            etl.data_warehouse.restore_schemas(traversed_schemas, dry_run=dry_run)
        raise


def upgrade_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                           max_concurrency=1, only_selected=False, skip_copy=False, dry_run=False):
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
    logger.info("Starting to upgrade %d relation(s) in %d schema(s)", len(relations), len(traversed_schemas))

    etl.data_warehouse.create_missing_schemas(traversed_schemas, dry_run=dry_run)
    create_relations_with_data(relations, "upgrade", max_concurrency, skip_copy=skip_copy, dry_run=dry_run)


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
    with closing(etl.db.connection(dsn_etl, readonly=dry_run)) as tx_conn, tx_conn as conn:
        for relation in tables:
            build_one_relation(conn, relation, use_delete=True, dry_run=dry_run, **monitor_info[relation.identifier])

    if run_vacuum:
        vacuum(tables, dry_run=dry_run)


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
    line_template = ("{index:4d} {relation.identifier:{width}s}"
                     " # {flag:9s} | kind={relation.kind} is_required={relation.is_required}")
    for i, relation in enumerate(complete_sequence):
        if relation.identifier in selected:
            flag = "selected"
        elif relation.identifier in immediate:
            flag = "immediate"
        else:
            flag = ""
        print(line_template.format(index=i + 1, relation=relation, width=max_len, flag=flag))


def show_dependency_chain(relations: List[RelationDescription], selector: TableSelector):
    """
    List the relations upstream (towards sources) from the selected ones, report in execution order.
    """
    execution_order = etl.relation.order_by_dependencies(relations)
    selected_relations = etl.relation.find_matches(execution_order, selector)
    if len(selected_relations) == 0:
        logger.warning("Found no matching relations for: %s", selector)
        return

    dependencies = set(relation.identifier for relation in selected_relations)
    for relation in execution_order[::-1]:
        if relation.identifier in dependencies:
            dependencies.update(relation.dependencies)

    max_len = max(len(identifier) for identifier in dependencies)
    line_template = ("{index:4d} {relation.identifier:{width}s}"
                     " # kind={relation.kind} is_required={relation.is_required}")
    for i, relation in enumerate(execution_order):
        if relation.identifier in dependencies:
            print(line_template.format(index=i + 1, relation=relation, width=max_len))
