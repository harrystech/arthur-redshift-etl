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
import re
from contextlib import closing
from itertools import chain
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Set

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
from etl.names import join_column_list, join_with_quotes, TableName, TableSelector, TempTableName
from etl.relation import RelationDescription
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ---- Section 0: Foundation: LoadableRelation ----

class LoadableRelation:
    """
    Wrapper for RelationDescription that adds state machinery useful for loading.
    (Load here refers to the step in the ETL, so includes load, upgrade, and update commands).

    We use composition here to avoid copying from the RelationDescription.
    Also, inheritance would work less well given how lazy loading is used in RelationDescription.
    You're welcome.

    Being 'Loadable' means that load-relevant RelationDescription properties may get new values here.
    In particular:
        - target_table_name is 'use_staging' aware
        - query_stmt is 'use_staging' aware

    However, dependency graph properties of RelationDescription should _not_ differ.
    In particular:
        - identifier should be consistent
        - dependencies should be consistent

    Q: Why 'loadable'?
    A: Because 'LoadOrUpgradeOrUpdateInProgress' sounded less supercalifragilisticexpialidocious.
    """

    def __getattr__(self, name):
        """
        Grab everything from the contained relation. Fail if it's actually not available.
        """
        if hasattr(self._relation_description, name):
            return getattr(self._relation_description, name)
        raise AttributeError("'%s' object has no attribute '%s'" % (self.__class__.__name__, name))

    __str__ = RelationDescription.__str__

    __format__ = RelationDescription.__format__

    def __init__(self, relation: RelationDescription, info: dict, use_staging=False, skip_copy=False) -> None:
        self._relation_description = relation
        self.info = info
        self.skip_copy = skip_copy or relation.is_view_relation
        self.failed = False
        self.use_staging = use_staging

    def delete_to_reset(self) -> bool:
        return False

    def create_to_reset(self) -> bool:
        return True

    def monitor(self):
        return etl.monitor.Monitor(**self.info)

    @property
    def identifier(self) -> str:
        # Load context does not change the identifier
        return self._relation_description.identifier

    @property
    def target_table_name(self):
        # Load context changes our target table
        if self.use_staging:
            return self._relation_description.target_table_name.as_staging_table_name()
        else:
            return self._relation_description.target_table_name

    def find_dependents(self, relations: List["LoadableRelation"]) -> List["LoadableRelation"]:
        unpacked = [r._relation_description for r in relations]  # do DAG operations in terms of RelationDescriptions
        dependent_relations = etl.relation.find_dependents(unpacked, [self._relation_description])
        dependent_relation_identifiers = set([r.identifier for r in dependent_relations])
        return [loadable for loadable in relations if loadable.identifier in dependent_relation_identifiers]

    @property
    def query_stmt(self) -> str:
        stmt = self._relation_description.query_stmt
        if self.use_staging:
            # Rewrite the query to use staging schemas:
            for dependency in self.dependencies:
                staging_dependency = TableName.from_identifier(dependency).as_staging_table_name()
                stmt = re.sub(r'\b' + dependency + r'\b', staging_dependency.identifier, stmt)
        return stmt

    @property
    def table_design(self) -> Dict[str, Any]:
        design = self._relation_description.table_design
        if self.use_staging:
            for column in design['columns']:
                if 'references' in column:
                    [foreign_table, [foreign_column]] = column['references']
                    column['references'] = [
                        TableName.from_identifier(foreign_table).as_staging_table_name().identifier,
                        [foreign_column]
                    ]
        return design

    @classmethod
    def from_descriptions(cls, relations: List[RelationDescription], command: str,
                          use_staging=False, skip_copy=False) -> List["LoadableRelation"]:
        """
        Build a list of "loadable" relations
        """
        dsn_etl = etl.config.get_dw_config().dsn_etl
        dbname = dsn_etl["database"]
        base_index = {"name": dbname, "current": 0, "final": len(relations)}
        base_destination = {"name": dbname}

        loadable = []
        for i, relation in enumerate(relations):
            target = relation.target_table_name
            source = dict(bucket_name=relation.bucket_name)
            if relation.is_view_relation or relation.is_ctas_relation:
                source['object_key'] = relation.sql_file_name
            else:
                source['object_key'] = relation.manifest_file_name
            destination = dict(base_destination, schema=target.schema, table=target.table)
            monitor_info = {
                "target": target.identifier,
                "step": command,
                "source": source,
                "destination": destination,
                "index": dict(base_index, current=i + 1)
            }
            loadable.append(cls(relation, monitor_info, use_staging, skip_copy=skip_copy))

        return loadable


class LoadableInTransactionRelation(LoadableRelation):

    def delete_to_reset(self) -> bool:
        return not self.is_view_relation

    def create_to_reset(self) -> bool:
        return False


# ---- Section 1: Functions that work on relations (creating them, filling them, adding permissions) ----

def create_table(conn: connection, table_design: dict, table_name: TableName, is_temp=False, dry_run=False) -> None:
    """
    Create a table matching this design (but possibly under another name, e.g. for temp tables).

    Columns must have a name and a SQL type (compatible with Redshift).
    They may have an attribute of the compression encoding and the nullable constraint.

    Other column attributes and constraints should be resolved as table
    attributes (e.g. distkey) and table constraints (e.g. primary key).

    Tables may have attributes such as a distribution style and sort key.
    Depending on the distribution style, they may also have a distribution key.
    """
    ddl_stmt = etl.design.redshift.build_table_ddl(table_design, table_name, is_temp=is_temp)
    etl.db.run(conn, "Creating table '{:x}'".format(table_name), ddl_stmt, dry_run=dry_run)


def create_view(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Create VIEW using the relation's query.
    """
    view_name = relation.target_table_name
    columns = join_column_list(relation.unquoted_columns)
    stmt = """CREATE VIEW {} (\n{}\n) AS\n{}""".format(view_name, columns, relation.query_stmt)
    etl.db.run(conn, "Creating view '{:x}'".format(relation), stmt, dry_run=dry_run)


def drop_relation_if_exists(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
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


def create_schemas_for_rebuild(schemas: List[DataWarehouseSchema],
                               use_staging: bool, dry_run=False) -> None:
    """
    Create schemas necessary for a full rebuild of data warehouse
    If `use_staging`, create new staging schemas.
    Otherwise, move standard position schemas out of the way by renaming them. Then create new ones.
    """
    if use_staging:
        etl.data_warehouse.create_schemas(schemas, use_staging=use_staging, dry_run=dry_run)
    else:
        etl.data_warehouse.backup_schemas(schemas, dry_run=dry_run)
        etl.data_warehouse.create_schemas(schemas, dry_run=dry_run)


def create_or_replace_relation(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
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
            create_table(conn, relation.table_design, relation.target_table_name, dry_run=dry_run)
        grant_access(conn, relation, dry_run=dry_run)
    except Exception as exc:
        raise RelationConstructionError(exc) from exc


def grant_access(conn: connection, relation: LoadableRelation, dry_run=False):
    """
    Grant privileges on (new) relation based on configuration.

    We always grant all privileges to the ETL user. We may grant read-only access
    or read-write access based on configuration. Note that the access is always based on groups, not users.
    """
    target = relation.target_table_name
    schema_config = relation.schema_config
    owner, reader_groups, writer_groups = schema_config.owner, schema_config.reader_groups, schema_config.writer_groups

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


def delete_whole_table(conn: connection, table: LoadableRelation, dry_run=False) -> None:
    """
    Delete all rows from this table.
    """
    stmt = """DELETE FROM {}""".format(table)
    etl.db.run(conn, "Deleting all rows in table '{:x}'".format(table), stmt, dry_run=dry_run)


def copy_data(conn: connection, relation: LoadableRelation, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.
    A manifest for the CSV files must be provided -- it is an error if the manifest is missing.
    """
    aws_iam_role = etl.config.get_data_lake_config("iam_role")
    s3_uri = "s3://{}/{}".format(relation.bucket_name, relation.manifest_file_name)

    if not relation.has_manifest:
        if dry_run:
            logger.info("Dry-run: Ignoring that relation '%s' is missing manifest file '%s'",
                        relation.identifier, s3_uri)
        else:
            raise MissingManifestError("relation '{}' is missing manifest file '{}'".format(
                relation.identifier, s3_uri))

    etl.design.redshift.copy_from_uri(conn, relation.target_table_name, s3_uri, aws_iam_role,
                                      need_compupdate=relation.is_missing_encoding, dry_run=dry_run)


def insert_from_query(conn: connection, table_name: TableName, columns: List[str], query: str, dry_run=False) -> None:
    """
    Load data into table from its query (aka materializing a view). The table name must be specified since
    the load goes either into the target table or a temporary one.
    """
    stmt = """INSERT INTO {table} (\n  {columns}\n)""".format(table=table_name, columns=join_column_list(columns))
    stmt += "\n" + query
    etl.db.run(conn, "Loading data into '{:x}' from query".format(table_name), stmt, dry_run=dry_run)


def load_ctas_directly(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
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


def load_ctas_using_temp_table(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Run query to fill temp table, then copy data (possibly along with missing dimension) into CTAS relation.
    """
    temp_name = TempTableName.for_table(relation.target_table_name)
    create_table(conn, relation.table_design, temp_name, is_temp=True, dry_run=dry_run)
    try:
        temp_columns = [column["name"] for column in relation.table_design["columns"]
                        if not (column.get("skipped") or column.get("identity"))]
        insert_from_query(conn, temp_name, temp_columns, relation.query_stmt, dry_run=dry_run)

        inner_stmt = "SELECT {} FROM {}".format(join_column_list(relation.unquoted_columns), temp_name)
        if relation.target_table_name.table.startswith("dim_"):
            missing_dimension = create_missing_dimension_row(relation.table_design["columns"])
            inner_stmt += "\nUNION ALL SELECT {}".format(", ".join(missing_dimension))

        insert_from_query(conn, relation.target_table_name, relation.unquoted_columns, inner_stmt, dry_run=dry_run)
    finally:
        stmt = "DROP TABLE {}".format(temp_name)
        etl.db.run(conn, "Dropping temporary table '{:x}'".format(temp_name), stmt, dry_run=dry_run)


def analyze(conn: connection, table: LoadableRelation, dry_run=False) -> None:
    """
    Update table statistics.
    """
    etl.db.run(conn, "Running analyze step on table '{:x}'".format(table), "ANALYZE {}".format(table), dry_run=dry_run)


def verify_constraints(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Raise a FailedConstraintError if :relation's target table doesn't obey its declared constraints.

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

def find_traversed_schemas(relations: List[LoadableRelation]) -> List[DataWarehouseSchema]:
    """
    Return schemas traversed when refreshing relations (in order that they are needed).
    """
    got_it = set()  # type: Set[str]
    traversed_in_order = []
    for relation in relations:
        this_schema = relation.schema_config
        if this_schema.name not in got_it:
            got_it.add(this_schema.name)
            traversed_in_order.append(this_schema)
    return traversed_in_order


# ---- Section 3: Functions that tie table operations together ----

def update_table(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
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


def build_one_relation(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Empty out tables (either with delete or by create-or-replacing them) and fill 'em up.
    Unless in delete mode, this always makes sure tables and views are created.

    Within transaction? Only applies to tables which get emptied and then potentially filled again.
    Not in transaction? Drop and create all relations, for tables, also potentially fill 'em up again.
    """
    with relation.monitor():

        # Step 1 -- clear out existing data (by deletion or by re-creation)
        if relation.delete_to_reset():
            delete_whole_table(conn, relation, dry_run=dry_run)
        elif relation.create_to_reset():
            create_or_replace_relation(conn, relation, dry_run=dry_run)

        # Step 2 -- load data (and verify)
        if relation.is_view_relation:
            pass
        elif relation.skip_copy:
            logger.info("Skipping loading data into '%s'", relation.identifier)
        else:
            update_table(conn, relation, dry_run=dry_run)
            verify_constraints(conn, relation, dry_run=dry_run)


def build_one_relation_using_pool(pool, relation: LoadableRelation, dry_run=False) -> None:
    assert not relation.is_transformation, "submitted view to parallel load"
    conn = pool.getconn()
    try:
        build_one_relation(conn, relation, dry_run=dry_run)
        conn.commit()
    except Exception as exc:
        # Add (some) exception information close to when it happened
        message = str(exc).split('\n', 1)[0]
        if relation.is_required:
            logger.error("Exception information for required relation '%s': %s", relation.identifier, message)
        else:
            logger.warning("Exception information for relation '%s': %s", relation.identifier, message)
        pool.putconn(conn, close=True)
        raise
    else:
        pool.putconn(conn, close=False)


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

def create_source_tables_with_data(relations: List[LoadableRelation], max_concurrency=1, dry_run=False) -> None:
    """
    Pick out all tables in source schemas from the list of relations and build up just those.
    Return list of identifiers for relations that are now empty (failed to load).

    Since we know that these relations never have dependencies, we don't have to track any
    failures while loading them.
    """
    timer = Timer()
    dsn_etl = etl.config.get_dw_config().dsn_etl
    pool = etl.db.connection_pool(max_concurrency, dsn_etl)
    futures = {}  # type: Dict[str, concurrent.futures.Future]
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            for relation in relations:
                future = executor.submit(build_one_relation_using_pool, pool, relation, dry_run=dry_run)
                futures[relation.identifier] = future
            # TODO for fail fast, switch to FIRST_EXCEPTION
            done, not_done = concurrent.futures.wait(futures.values(), return_when=concurrent.futures.ALL_COMPLETED)
            cancelled = [future for future in not_done if future.cancel()]
            logger.info("Wrapping up work in %d worker(s): %d done, %d not done (%d cancelled) (%s)",
                        max_concurrency, len(done), len(not_done), len(cancelled), timer)
    finally:
        pool.closeall()

    for relation in relations:
        try:
            futures[relation.identifier].result()
        except concurrent.futures.CancelledError:
            pass
        except (RelationConstructionError, RelationDataError):
            relation.failed = True
            if relation.is_required:
                logger.error("Failed to build required relation '%s':", relation.identifier, exc_info=True)
            else:
                logger.warning("Failed to build relation '%s':", relation.identifier, exc_info=True)

    failed_and_required = [relation.identifier for relation in relations if relation.failed and relation.is_required]
    if failed_and_required:
        raise RequiredRelationLoadError(failed_and_required)

    failed = [relation.identifier for relation in relations if relation.failed]
    if failed:
        logger.warning("These %d relation(s) failed to build: %s", len(failed), join_with_quotes(failed))
    logger.info("Finished with %d relation(s) in source schemas (%s)", len(relations), timer)


def create_transformations_with_data(relations: List[LoadableRelation], wlm_query_slots: int, dry_run=False) -> None:
    """
    Pick out all tables in transformation schemas from the list of relations and build up just those.

    These relations may be dependent on relations that may have failed before we reached them.
    If dependencies were left empty, we'll fall back to skip_copy mode.
    Unless the relation is "required" in which case we abort here.
    """
    transformations = [relation for relation in relations if relation.is_transformation]
    if not transformations:
        logger.info("None of the relations are in transformation schemas")
        return

    timer = Timer()
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        set_redshift_wlm_slots(conn, wlm_query_slots, dry_run=dry_run)
        for relation in transformations:
            try:
                build_one_relation(conn, relation, dry_run=dry_run)
            except (RelationConstructionError, RelationDataError) as exc:
                if relation.is_required:
                    raise RequiredRelationLoadError([relation.identifier]) from exc
                dependent_relations = relation.find_dependents(transformations)
                dependent_required = [relation.identifier for relation in dependent_relations if relation.is_required]
                if dependent_required:
                    raise RequiredRelationLoadError(dependent_required, relation.identifier) from exc
                logger.warning("Failed relation '%s' is not required, ignoring this exception:",
                               relation.identifier, exc_info=True)
                if dependent_relations:
                    for deb in dependent_relations:
                        deb.skip_copy = True
                    dependents = [relation.identifier for relation in dependent_relations]
                    logger.warning("Continuing while omitting dependent relation(s): %s", join_with_quotes(dependents))

    failed = [relation.identifier for relation in relations if relation.failed]
    if failed:
        logger.warning("These %d relation(s) failed to build: %s", len(failed), join_with_quotes(failed))
    skipped = [relation.identifier for relation in relations if relation.failed]
    if 0 < len(skipped) < len(relations):
        logger.warning("These %d relation(s) were left empty: %s", len(skipped), join_with_quotes(skipped))
    logger.info("Finished with %d relation(s) in transformation schemas (%s)", len(transformations), timer)


def set_redshift_wlm_slots(conn: connection, slots: int, dry_run: bool) -> None:
    etl.db.run(conn, "Using {} WLM queue slots for transformations.".format(slots),
               "SET wlm_query_slot_count TO {}".format(slots), dry_run=dry_run)


def create_relations_with_data(relations: List[LoadableRelation], max_concurrency=1, wlm_query_slots=1,
                               dry_run=False) -> None:
    """
    "Building" relations refers to creating them, granting access, and if they should hold data, loading them.
    """
    source_relations = [relation for relation in relations if not relation.is_transformation]
    if not source_relations:
        logger.info("None of the relations are in source schemas")
    else:
        create_source_tables_with_data(source_relations, max_concurrency, dry_run=dry_run)

    create_transformations_with_data(relations, wlm_query_slots, dry_run=dry_run)


def select_execution_order(relations: List[RelationDescription], selector: TableSelector,
                           include_dependents=False) -> List[RelationDescription]:
    """
    Return list of relations that were selected and optionally, expand the list by the dependents of the selected ones.
    """
    logger.info("Pondering execution order of %d relation(s)", len(relations))
    execution_order = etl.relation.order_by_dependencies(relations)
    selected = etl.relation.find_matches(execution_order, selector)
    if include_dependents:
        dependents = etl.relation.find_dependents(execution_order, selected)
        combined = frozenset(relation.identifier for relation in chain(selected, dependents))
        selected = [relation for relation in execution_order if relation.identifier in combined]
    return selected


def take_starting_from(predicate: Callable, iterable: Iterable) -> Iterator:
    """
    Generate sequence of elements from the input which start from the first match (based on
    predicate function).

    >>> ints = range(5)
    >>> gen = take_starting_from(lambda i: i == 3, ints)
    >>> list(gen)
    [3, 4]
    >>> gen = take_starting_from(lambda i: i > 5, ints)
    >>> list(gen)
    []
    """
    found = False
    for element in iterable:
        if not found and predicate(element):
            found = True
        if found:
            yield element


# ---- Section 5: "Callbacks" (functions that implement commands) ----

def load_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector, use_staging=True,
                        max_concurrency=1, wlm_query_slots=1, skip_copy=False, no_rollback=False, dry_run=False):
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
    selected_relations = select_execution_order(all_relations, selector, include_dependents=True)
    if not selected_relations:
        logger.warning("Found no relations matching: %s", selector)
        return

    relations = LoadableRelation.from_descriptions(selected_relations, "load",
                                                   skip_copy=skip_copy, use_staging=use_staging)
    traversed_schemas = find_traversed_schemas(relations)
    logger.info("Starting to load %d relation(s) in %d schema(s)", len(relations), len(traversed_schemas))

    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True)) as conn:
        logger.info("Open connections:\n%s", etl.db.format_result(etl.db.list_connections(conn)))
        logger.info("Open transactions:\n%s", etl.db.format_result(etl.db.list_transactions(conn)))

    create_schemas_for_rebuild(traversed_schemas, use_staging=use_staging, dry_run=dry_run)
    try:
        create_relations_with_data(relations, max_concurrency, wlm_query_slots, dry_run=dry_run)
    except ETLRuntimeError:
        if not (no_rollback or use_staging):
            etl.data_warehouse.restore_schemas(traversed_schemas, dry_run=dry_run)
        raise

    if use_staging:
        # We've successfully built staging schemas, so roll them out
        etl.data_warehouse.publish_schemas(traversed_schemas, dry_run=dry_run)


def upgrade_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                           max_concurrency=1, wlm_query_slots=1,
                           only_selected=False, continue_from: Optional[str]=None, use_staging=False,
                           skip_copy=False, dry_run=False) -> None:
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
    selected_relations = select_execution_order(all_relations, selector, include_dependents=not only_selected)
    if not selected_relations:
        logger.warning("Found no relations matching: %s", selector)
        return
    if continue_from is not None:
        logger.info("Trying to fast forward to '%s'", continue_from)
        selected_relations = list(take_starting_from(lambda relation: relation.identifier == continue_from,
                                                     selected_relations))
        if not selected_relations:
            logger.warning("Found no relations matching relation '%s'", continue_from)
            return

    relations = LoadableRelation.from_descriptions(selected_relations, "upgrade",
                                                   skip_copy=skip_copy, use_staging=use_staging)

    traversed_schemas = find_traversed_schemas(relations)
    logger.info("Starting to upgrade %d relation(s) in %d schema(s)", len(relations), len(traversed_schemas))

    etl.data_warehouse.create_schemas(traversed_schemas, use_staging=use_staging, dry_run=dry_run)
    create_relations_with_data(relations, max_concurrency, wlm_query_slots, dry_run=dry_run)


def update_data_warehouse(all_relations: List[RelationDescription], selector: TableSelector,
                          wlm_query_slots=1, only_selected=False, run_vacuum=False, dry_run=False):
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
    selected_relations = select_execution_order(all_relations, selector, include_dependents=not only_selected)
    tables = [relation for relation in selected_relations if not relation.is_view_relation]
    if not tables:
        logger.warning("Found no tables matching: %s", selector)
        return

    relations = LoadableInTransactionRelation.from_descriptions(selected_relations, "update")
    logger.info("Starting to update %d tables(s)", len(relations))

    # Run update within a transaction:
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, readonly=dry_run)) as tx_conn, tx_conn as conn:
        set_redshift_wlm_slots(conn, wlm_query_slots, dry_run=dry_run)
        for relation in relations:
            build_one_relation(conn, relation, dry_run=dry_run)

    if run_vacuum:
        vacuum(tables, dry_run=dry_run)


def show_downstream_dependents(relations: List[RelationDescription], selector: TableSelector,
                               continue_from: Optional[str]=None) -> None:
    """
    List the execution order of loads or updates.

    Relations are marked based on whether they were directly selected or selected as
    part of the propagation of new data.
    They are also marked whether they'd lead to a fatal error since they're required for full load.
    """
    complete_sequence = select_execution_order(relations, selector, include_dependents=True)
    if len(complete_sequence) == 0:
        logger.warning("Found no matching relations for: %s", selector)
        return
    if continue_from is not None:
        logger.info("Trying to fast forward to '%s'", continue_from)
        complete_sequence = list(take_starting_from(lambda relation: relation.identifier == continue_from,
                                                    complete_sequence))
        if len(complete_sequence) == 0:
            logger.warning("Found no relations matching relation '%s'", continue_from)
            return
    selected_relations = etl.relation.find_matches(complete_sequence, selector)

    selected = frozenset(relation.identifier for relation in selected_relations)
    immediate = set(selected)
    for relation in complete_sequence:
        if relation.is_view_relation and any(name in immediate for name in relation.dependencies):
            immediate.add(relation.identifier)
    immediate -= selected
    logger.info("Execution order includes %d selected, %d immediate, and %d other downstream relation(s)",
                len(selected), len(immediate), len(complete_sequence) - len(selected) - len(immediate))

    max_len = max(len(relation.identifier) for relation in complete_sequence)
    line_template = ("{relation.identifier:{width}s}"
                     " # index={index:4d}, flag={flag:.9s}, kind={relation.kind}, is_required={relation.is_required}")
    for i, relation in enumerate(complete_sequence):
        if relation.identifier in selected:
            flag = "selected"
        elif relation.identifier in immediate:
            flag = "immediate"
        else:
            flag = "dependent"
        print(line_template.format(index=i + 1, relation=relation, width=max_len, flag=flag))


def show_upstream_dependencies(relations: List[RelationDescription], selector: TableSelector):
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
    line_template = ("{relation.identifier:{width}s}"
                     " # index={index:4d}, kind={relation.kind}, is_required={relation.is_required}")
    for i, relation in enumerate(execution_order):
        if relation.identifier in dependencies:
            print(line_template.format(index=i + 1, relation=relation, width=max_len))
