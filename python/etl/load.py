"""
This is the "load and transform" part of our ELT.

There are three options for accomplishing this:

(1) a "load" command will start building the data warehouse from scratch, usually, after backing up.
(2) an "upgrade" command will try to load new data and changed relations (without any back up).
(3) an "update" command will attempt to bring in new data and new relations, then let it percolate.

The "load" command is used in the rebuild pipeline. It is important to notice that
it always operates on all tables in any schemas impacted by the load.

The "update" command is used in the refresh pipeline. It is safe to run (and safe to
re-run) to bring in new data.  It cannot be used to make a structural change. Basically,
"update" will respect the boundaries set by "publishing" the data warehouse state in S3
as "current".

The "upgrade" command should probably be used only during development. Think of an "upgrade" as a
"load" without the safety net. Unlike "load", it will upgrade more surgically and not expand
to modify entire schemas, but unlike "update" it will not go gently about it.
(It is a bit expensive still given the cautious build up and permission changes.)

It is possible to bring up an "empty" data warehouse where all the structure exists
(meaning all the tables and views are in place) but no data was actually loaded.
This is used during the validation pipeline. See the "skip copy" options.

These are the general pre-requisites:

    * "Tables" that have upstream sources must have data files and a manifest file from a prior
      extract.

    * "CTAS" tables are derived from queries so must have a SQL file.

        * For every derived table (CTAS) a SQL file must exist in S3 with a valid
          expression to create the content of the table (meaning: just the select without
          closing ';'). The actual DDL statement (CREATE TABLE AS ...) and the table
          attributes / constraints are added from the matching table design file.

    * "VIEWS" are views and so must have a SQL file in S3.

Currently data files that are CSV, Avro or JSON-formatted are supported.
"""

import concurrent.futures
import logging
import queue
import re
import threading
import time
from calendar import timegm
from collections import defaultdict
from contextlib import closing
from datetime import datetime, timedelta
from functools import partial
from typing import Any, Dict, List, Optional, Sequence, Set

import funcy
from psycopg2.extensions import connection  # only for type annotation

import etl
import etl.data_warehouse
import etl.db
import etl.dialect.redshift
import etl.monitor
import etl.relation
from etl.config.dw import DataWarehouseSchema
from etl.errors import (
    ETLRuntimeError,
    FailedConstraintError,
    MissingExtractEventError,
    MissingManifestError,
    RelationConstructionError,
    RelationDataError,
    RequiredRelationLoadError,
    UpdateTableError,
)
from etl.names import TableName, TableSelector, TempTableName
from etl.relation import RelationDescription
from etl.text import format_lines, join_with_double_quotes, join_with_single_quotes
from etl.util.retry import call_with_retry
from etl.util.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# --- Section 0: Foundation: LoadableRelation


class LoadableRelation:
    """
    Wrapper for RelationDescription that adds state machinery useful for loading.

    Loading here refers to the step in the ETL, so includes the load, upgrade, and update commands.

    We use composition here to avoid copying from the RelationDescription.
    Also, inheritance would work less well given how lazy loading is used in RelationDescription.
    You're welcome.

    Being 'Loadable' means that load-relevant RelationDescription properties may get new values
    here. In particular:
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
        """Grab everything from the contained relation. Fail if it's actually not available."""
        if hasattr(self._relation_description, name):
            return getattr(self._relation_description, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __str__(self) -> str:
        return str(self.target_table_name)

    def __format__(self, code):
        r"""
        Format target table as delimited identifier (with quotes) or just as an identifier.

        With the default or ':s', it's a delimited identifier with quotes.
        With ':x", the name is left bare but single quotes are around it.

        Compared to RelationDescription, we have the additional complexity of dealing with
        the position (staging or not) of a table.

        >>> import etl.file_sets
        >>> import etl.config
        >>> from collections import namedtuple
        >>> MockDWConfig = namedtuple('MockDWConfig', ['schemas'])
        >>> MockSchema = namedtuple('MockSchema', ['name'])
        >>> etl.config._dw_config = MockDWConfig(schemas=[MockSchema(name='c')])
        >>> fs = etl.file_sets.RelationFileSet(TableName("a", "b"), TableName("c", "b"), None)
        >>> relation = LoadableRelation(RelationDescription(fs), {}, skip_copy=True)
        >>> "As delimited identifier: {:s}, as string: {:x}".format(relation, relation)
        'As delimited identifier: "c"."b", as string: \'c.b\''
        >>> relation_with_staging = LoadableRelation(
        ...     RelationDescription(fs), {}, use_staging=True, skip_copy=True)
        >>> "As delimited identifier: {:s}, as string: {:x}".format(
        ...     relation_with_staging, relation_with_staging)
        'As delimited identifier: "etl_staging$c"."b", as string: \'c.b\' (in staging)'
        """
        if (not code) or (code == "s"):
            return str(self)
        if code == "x":
            if self.use_staging:
                return "'{:s}' (in staging)".format(self.identifier)
            return "'{:s}'".format(self.identifier)
        raise ValueError("unsupported format code '{}' passed to LoadableRelation".format(code))

    def __init__(
        self,
        relation: RelationDescription,
        info: dict,
        use_staging=False,
        target_schema: Optional[str] = None,
        skip_copy=False,
        in_transaction=False,
    ) -> None:
        self._relation_description = relation
        self.info = info
        self.in_transaction = in_transaction
        self.skip_copy = skip_copy
        self.use_staging = use_staging
        self.failed = False

        if target_schema is not None:
            self.target_table_name = TableName(
                target_schema, self._relation_description.target_table_name.table
            )
        elif self.use_staging:
            self.target_table_name = self._relation_description.target_table_name.as_staging_table_name()
        else:
            self.target_table_name = self._relation_description.target_table_name

    def monitor(self):
        return etl.monitor.Monitor(**self.info)

    @property
    def identifier(self) -> str:
        # Load context should not change the identifier for logging.
        if self.use_staging:
            return self._relation_description.identifier
        return self.target_table_name.identifier

    def find_dependents(self, relations: Sequence["LoadableRelation"]) -> List["LoadableRelation"]:
        unpacked = [
            r._relation_description for r in relations
        ]  # do DAG operations in terms of RelationDescriptions
        dependent_relations = etl.relation.find_dependents(unpacked, [self._relation_description])
        dependent_relation_identifiers = {r.identifier for r in dependent_relations}
        return [
            loadable for loadable in relations if loadable.identifier in dependent_relation_identifiers
        ]

    def mark_failure(self, relations: Sequence["LoadableRelation"], exc_info=True) -> None:
        """Mark this relation as failed and set dependents (stored in :relations) to skip_copy."""
        self.failed = True
        if self.is_required:
            logger.error("Failed to build required relation '%s':", self.identifier, exc_info=exc_info)
        else:
            logger.warning("Failed to build relation '%s':", self.identifier, exc_info=exc_info)
        # Skip copy on all dependents
        dependents = self.find_dependents(relations)
        for dep in dependents:
            dep.skip_copy = True
        identifiers = [dependent.identifier for dependent in dependents]
        if identifiers:
            logger.warning(
                "Continuing while leaving %d relation(s) empty: %s",
                len(identifiers),
                join_with_single_quotes(identifiers),
            )

    @property
    def query_stmt(self) -> str:
        stmt = self._relation_description.query_stmt
        if self.use_staging:
            # Rewrite the query to use staging schemas by changing identifiers to their staging
            # version. This requires all tables to be fully qualified. There is a small chance
            # that we're too aggressive and change a table name inside a string.
            for dependency in self.dependencies:
                staging_dependency = dependency.as_staging_table_name()
                stmt = re.sub(dependency.identifier_as_re, staging_dependency.identifier, stmt)
        return stmt

    @property
    def table_design(self) -> Dict[str, Any]:
        design = self._relation_description.table_design
        if self.use_staging:
            # Rewrite foreign table references to point into the correct table
            for column in design["columns"]:
                if "references" in column:
                    [foreign_table, [foreign_column]] = column["references"]
                    column["references"] = [
                        TableName.from_identifier(foreign_table).as_staging_table_name().identifier,
                        [foreign_column],
                    ]
        return design

    @classmethod
    def from_descriptions(
        cls,
        relations: Sequence[RelationDescription],
        command: str,
        use_staging=False,
        target_schema: Optional[str] = None,
        skip_copy=False,
        skip_loading_sources=False,
        in_transaction=False,
    ) -> List["LoadableRelation"]:
        """Build a list of "loadable" relations."""
        dsn_etl = etl.config.get_dw_config().dsn_etl
        database = dsn_etl["database"]
        base_index = {"name": database, "current": 0, "final": len(relations)}
        base_destination = {"name": database}

        managed = frozenset(relation.identifier for relation in relations)

        loadable = []
        for i, relation in enumerate(relations):
            this_skip_copy = skip_copy
            if skip_loading_sources:
                # Only load transformations and only if no dependency is external.
                if not relation.is_transformation or any(
                    dependency.identifier not in managed for dependency in relation.dependencies
                ):
                    this_skip_copy = True

            target = relation.target_table_name
            source = {"bucket_name": relation.bucket_name}
            if relation.is_transformation:
                source["object_key"] = relation.sql_file_name
            else:
                source["object_key"] = relation.manifest_file_name
            destination = dict(base_destination, schema=target.schema, table=target.table)
            monitor_info = {
                "target": target.identifier,
                "step": command,
                "source": source,
                "destination": destination,
                "options": {"use_staging": use_staging, "skip_copy": this_skip_copy},
                "index": dict(base_index, current=i + 1),
            }
            loadable.append(
                cls(relation, monitor_info, use_staging, target_schema, this_skip_copy, in_transaction)
            )

        return loadable


# --- Section 1: Functions that work on relations (creating them, filling them, adding permissions)


def create_table(
    conn: connection, relation: LoadableRelation, table_name: Optional[TableName] = None, dry_run=False
) -> None:
    """
    Create a table matching this design (but possibly under another name).

    If a name is specified, we'll assume that this should be an intermediate, aka temp table.

    Columns must have a name and a SQL type (compatible with Redshift).
    They may have an attribute of the compression encoding and the nullable constraint.

    Other column attributes and constraints should be resolved as table
    attributes (e.g. distkey) and table constraints (e.g. primary key).

    Tables may have attributes such as a distribution style and sort key.
    Depending on the distribution style, they may also have a distribution key.
    """
    if table_name is None:
        ddl_table_name = relation.target_table_name
        message = "Creating table {:x}".format(relation)
        is_temp = False
    else:
        ddl_table_name = table_name
        message = "Creating temporary table for {:x}".format(relation)
        is_temp = True

    ddl_stmt = etl.dialect.redshift.build_table_ddl(
        ddl_table_name, relation.table_design, is_temp=is_temp
    )
    etl.db.run(conn, message, ddl_stmt, dry_run=dry_run)


def create_view(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """Create VIEW using the relation's query."""
    ddl_view_name = relation.target_table_name
    stmt = etl.dialect.redshift.build_view_ddl(
        ddl_view_name, relation.unquoted_columns, relation.query_stmt
    )
    etl.db.run(conn, "Creating view {:x}".format(relation), stmt, dry_run=dry_run)


def drop_relation_if_exists(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Run either DROP VIEW or DROP TABLE depending on type of existing relation.

    It's ok if the relation doesn't already exist.
    """
    try:
        kind = etl.db.relation_kind(
            conn, relation.target_table_name.schema, relation.target_table_name.table
        )
        if kind is not None:
            stmt = """DROP {} {} CASCADE""".format(kind, relation)
            etl.db.run(conn, "Dropping {} {:x}".format(kind.lower(), relation), stmt, dry_run=dry_run)
    except Exception as exc:
        raise RelationConstructionError(exc) from exc


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
            create_table(conn, relation, dry_run=dry_run)
        if not relation.use_staging:
            grant_access(conn, relation, dry_run=dry_run)
    except Exception as exc:
        raise RelationConstructionError(exc) from exc


def grant_access(conn: connection, relation: LoadableRelation, dry_run=False):
    """
    Grant privileges on (new) relation based on configuration.

    We always grant all privileges to the ETL user. We may grant read-only access
    or read-write access based on configuration. Note that the access is always based on groups,
    not users.
    """
    target = relation.target_table_name
    schema_config = relation.schema_config
    reader_groups, writer_groups = schema_config.reader_groups, schema_config.writer_groups

    if reader_groups:
        if dry_run:
            logger.info(
                "Dry-run: Skipping granting of select access on {:x} to {}".format(
                    relation, join_with_single_quotes(reader_groups)
                )
            )
        else:
            logger.info(
                "Granting select access on {:x} to {}".format(
                    relation, join_with_single_quotes(reader_groups)
                )
            )
            for reader in reader_groups:
                etl.db.grant_select(conn, target.schema, target.table, reader)

    if writer_groups:
        if dry_run:
            logger.info(
                "Dry-run: Skipping granting of write access on {:x} to {}".format(
                    relation, join_with_single_quotes(writer_groups)
                )
            )
        else:
            logger.info(
                "Granting write access on {:x} to {}".format(
                    relation, join_with_single_quotes(writer_groups)
                )
            )
            for writer in writer_groups:
                etl.db.grant_select_and_write(conn, target.schema, target.table, writer)


def delete_whole_table(conn: connection, table: LoadableRelation, dry_run=False) -> None:
    """Delete all rows from this table."""
    stmt = """DELETE FROM {}""".format(table)
    etl.db.run(conn, "Deleting all rows in table {:x}".format(table), stmt, dry_run=dry_run)


def copy_data(conn: connection, relation: LoadableRelation, dry_run=False):
    """
    Load data into table in the data warehouse using the COPY command.

    A manifest for the CSV files must be provided -- it is an error if the manifest is missing.
    """
    aws_iam_role = str(etl.config.get_config_value("object_store.iam_role"))
    s3_uri = "s3://{}/{}".format(relation.bucket_name, relation.manifest_file_name)

    if not relation.has_manifest:
        if dry_run:
            logger.info(
                "Dry-run: Ignoring that relation '{}' is missing manifest file '{}'".format(
                    relation.identifier, s3_uri
                )
            )
        else:
            raise MissingManifestError(
                "relation '{}' is missing manifest file '{}'".format(relation.identifier, s3_uri)
            )

    copy_func = partial(
        etl.dialect.redshift.copy_from_uri,
        conn,
        relation.target_table_name,
        relation.unquoted_columns,
        s3_uri,
        aws_iam_role,
        data_format=relation.schema_config.s3_data_format.format,
        format_option=relation.schema_config.s3_data_format.format_option,
        file_compression=relation.schema_config.s3_data_format.compression,
        dry_run=dry_run,
    )
    if relation.in_transaction:
        copy_func()
    else:
        call_with_retry(etl.config.get_config_int("arthur_settings.copy_data_retries"), copy_func)


def insert_from_query(
    conn: connection,
    relation: LoadableRelation,
    table_name: Optional[TableName] = None,
    columns: Optional[Sequence[str]] = None,
    query_stmt: Optional[str] = None,
    dry_run=False,
) -> None:
    """
    Load data into table from its query (aka materializing a view).

    The table name, query, and columns may be overridden from their defaults, which are the
    values from the relation.
    """
    if table_name is None:
        table_name = relation.target_table_name
    if columns is None:
        columns = relation.unquoted_columns
    if query_stmt is None:
        query_stmt = relation.query_stmt

    insert_func = partial(
        etl.dialect.redshift.insert_from_query, conn, table_name, columns, query_stmt, dry_run=dry_run
    )
    if relation.in_transaction:
        insert_func()
    else:
        call_with_retry(etl.config.get_config_int("arthur_settings.insert_data_retries"), insert_func)


def load_ctas_directly(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Run query to fill CTAS relation.

    Not to be used for dimensions etc.)
    """
    insert_from_query(conn, relation, dry_run=dry_run)


def create_missing_dimension_row(columns: Sequence[dict]) -> List[str]:
    """Return row that represents missing dimension values."""
    na_values_row = []
    for column in columns:
        if column.get("skipped", False):
            continue
        elif column.get("identity", False):
            na_values_row.append("0")
        else:
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
    """Run query to fill temp table and copy data (option: with missing dimension) into target."""
    temp_name = TempTableName.for_table(relation.target_table_name)
    create_table(conn, relation, table_name=temp_name, dry_run=dry_run)
    try:
        temp_columns = [
            column["name"]
            for column in relation.table_design["columns"]
            if not (column.get("skipped") or column.get("identity"))
        ]
        insert_from_query(conn, relation, table_name=temp_name, columns=temp_columns, dry_run=dry_run)

        inner_stmt = "SELECT {} FROM {}".format(
            join_with_double_quotes(relation.unquoted_columns), temp_name
        )
        if relation.target_table_name.table.startswith("dim_"):
            missing_dimension = create_missing_dimension_row(relation.table_design["columns"])
            inner_stmt += "\nUNION ALL SELECT {}".format(", ".join(missing_dimension))

        insert_from_query(conn, relation, query_stmt=inner_stmt, dry_run=dry_run)
    finally:
        stmt = "DROP TABLE {}".format(temp_name)
        etl.db.run(conn, "Dropping temporary table for {:x}".format(relation), stmt, dry_run=dry_run)


def analyze(conn: connection, table: LoadableRelation, dry_run=False) -> None:
    """Update table statistics."""
    etl.db.run(
        conn,
        "Running analyze step on table {:x}".format(table),
        "ANALYZE {}".format(table),
        dry_run=dry_run,
    )


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
        logger.info("No constraints to verify for '{:s}'".format(relation.identifier))
        return

    # To make this work in DataGrip, define '\{(\w+)\}' under Tools -> Database -> User Parameters.
    # Then execute the SQL using command-enter, enter the values for `cols` and `table`, et voilà!
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
        quoted_columns = join_with_double_quotes(columns)
        if constraint_type == "unique":
            condition = " AND ".join('"{}" IS NOT NULL'.format(name) for name in columns)
        else:
            condition = "TRUE"

        statement = statement_template.format(
            columns=quoted_columns, table=relation, condition=condition, limit=limit
        )
        if dry_run:
            logger.info(
                "Dry-run: Skipping check of {} constraint in {:x} on column(s): {}".format(
                    constraint_type, relation, join_with_single_quotes(columns)
                )
            )
            etl.db.skip_query(conn, statement)
        else:
            logger.info(
                "Checking {} constraint in {:x} on column(s): {}".format(
                    constraint_type, relation, join_with_single_quotes(columns)
                )
            )
            results = etl.db.query(conn, statement)
            if results:
                if len(results) == limit:
                    logger.error(
                        "Constraint check for {:x} failed on at least {:d} row(s)".format(
                            relation, len(results)
                        )
                    )
                else:
                    logger.error(
                        "Constraint check for {:x} failed on {:d} row(s)".format(relation, len(results))
                    )
                raise FailedConstraintError(relation, constraint_type, columns, results)


# --- Section 2: Functions that work on schemas


def find_traversed_schemas(relations: Sequence[LoadableRelation]) -> List[DataWarehouseSchema]:
    """Return schemas traversed when refreshing relations (in order that they are needed)."""
    got_it: Set[str] = set()
    traversed_in_order = []
    for relation in relations:
        this_schema = relation.schema_config
        if this_schema.name not in got_it:
            got_it.add(this_schema.name)
            traversed_in_order.append(this_schema)
    return traversed_in_order


def create_schemas_for_rebuild(
    schemas: Sequence[DataWarehouseSchema], use_staging: bool, dry_run=False
) -> None:
    """
    Create schemas necessary for a full rebuild of data warehouse.

    If `use_staging`, only create new staging schemas.
    Otherwise, move standard position schemas out of the way by renaming them. Then create new ones.
    """
    if use_staging:
        etl.data_warehouse.create_schemas(schemas, use_staging=use_staging, dry_run=dry_run)
    else:
        etl.data_warehouse.backup_schemas(schemas, dry_run=dry_run)
        etl.data_warehouse.create_schemas(schemas, dry_run=dry_run)


# --- Section 3: Functions that tie table operations together


def update_table(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Update table contents either from CSV files from upstream sources or by running some SQL query.

    This assumes that the table was previously created.

    1. For tables backed by upstream sources, data is copied in.
    2. If the CTAS doesn't have a key (no identity column), then values are inserted straight
        from a view.
    3. If a column is marked as being a key (identity is true), then a temporary table is built
        from the query and then copied into the "CTAS" relation. If the name of the relation starts
        with "dim_", then it's assumed to be a dimension and a row with missing values (mostly 0,
        false, etc.) is added as well.

    Finally, we run an ANALYZE statement to update table statistics (unless we're updating the
    table within a transaction since -- we've been having problems with locks so skip the ANALYZE
    for updates).
    """
    try:
        if relation.is_ctas_relation:
            if relation.has_identity_column:
                load_ctas_using_temp_table(conn, relation, dry_run=dry_run)
            else:
                load_ctas_directly(conn, relation, dry_run=dry_run)
        else:
            copy_data(conn, relation, dry_run=dry_run)
        if not relation.in_transaction:
            analyze(conn, relation, dry_run=dry_run)
    except Exception as exc:
        raise UpdateTableError(exc) from exc


def build_one_relation(conn: connection, relation: LoadableRelation, dry_run=False) -> None:
    """
    Empty out tables (either with delete or by create-or-replacing them) and fill 'em up.

    Unless in delete mode, this always makes sure tables and views are created.

    Within transaction? Only applies to tables which get emptied and then potentially filled again.
    Not in transaction? Drop and create all relations and for tables also potentially fill 'em up
    again.
    """
    with relation.monitor() as monitor:

        # Step 1 -- clear out existing data (by deletion or by re-creation)
        if relation.in_transaction:
            if not relation.is_view_relation:
                delete_whole_table(conn, relation, dry_run=dry_run)
        else:
            create_or_replace_relation(conn, relation, dry_run=dry_run)

        # Step 2 -- load data (and verify)
        if relation.is_view_relation:
            pass
        elif relation.skip_copy:
            logger.info(f"Skipping loading data into {relation:x} (skip copy is active)")
        elif relation.failed:
            logger.info(f"Bypassing already failed relation {relation:x}")
        else:
            update_table(conn, relation, dry_run=dry_run)
            verify_constraints(conn, relation, dry_run=dry_run)

        # Step 3 -- log size of table
        if relation.is_view_relation or relation.failed or relation.skip_copy:
            return
        stmt = f"SELECT COUNT(*) AS rowcount FROM {relation}"
        if dry_run:
            etl.db.skip_query(conn, stmt)
            return
        rows = etl.db.query(conn, stmt)
        if rows:
            rowcount = rows[0]["rowcount"]
            logger.info(f"Found {rowcount:d} row(s) in {relation:x}")
            monitor.add_extra("rowcount", rowcount)


def build_one_relation_using_pool(pool, relation: LoadableRelation, dry_run=False) -> None:
    conn = pool.getconn()
    conn.set_session(autocommit=True, readonly=dry_run)
    try:
        build_one_relation(conn, relation, dry_run=dry_run)
    except Exception as exc:
        # Add (some) exception information close to when it happened
        message = str(exc).split("\n", 1)[0]
        if relation.is_required:
            logger.error(
                "Exception information for required relation {:x}: {}".format(relation, message)
            )
        else:
            logger.error("Exception information for relation {:x}: {}".format(relation, message))
        pool.putconn(conn, close=True)
        raise
    else:
        pool.putconn(conn, close=False)


def vacuum(relations: Sequence[RelationDescription], dry_run=False) -> None:
    """
    Tidy up the warehouse before guests come over.

    This needs to open a new connection since it needs to happen outside a transaction.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    timer = Timer()
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        for relation in relations:
            etl.db.run(
                conn,
                "Running vacuum on {:x}".format(relation),
                "VACUUM {}".format(relation),
                dry_run=dry_run,
            )
    if not dry_run:
        logger.info("Ran vacuum for %d table(s) (%s)", len(relations), timer)


# --- Section 4: Functions related to control flow


def create_source_tables_when_ready(
    relations: Sequence[LoadableRelation],
    max_concurrency=1,
    look_back_minutes=15,
    dry_run=False,
) -> None:
    """
    Create source relations in several threads, as we observe their extracts to be done.

    We assume here that the relations have no dependencies on each other and just gun it.
    This will only raise an Exception if one of the created relations was marked as "required".

    Since these relations may have downstream dependents, we make sure to mark skip_copy on
    any relation from the full set of relations that depends on a source relation that failed
    to load.
    """
    idle_termination_seconds = etl.config.get_config_int(
        "arthur_settings.concurrent_load_idle_termination_seconds"
    )
    source_relations = [relation for relation in relations if not relation.is_transformation]
    if not source_relations:
        logger.info("None of the relations are in source schemas")
        return

    dsn_etl = etl.config.get_dw_config().dsn_etl
    pool = etl.db.connection_pool(max_concurrency, dsn_etl)

    recent_cutoff = datetime.utcnow() - timedelta(minutes=look_back_minutes)
    cutoff_epoch = timegm(recent_cutoff.utctimetuple())
    sleep_time = 30
    checkpoint_time_cutoff = sleep_time + idle_termination_seconds
    timer = Timer()

    for dispatcher in etl.monitor.MonitorPayload.dispatchers:
        if isinstance(dispatcher, etl.monitor.DynamoDBStorage):
            table = dispatcher.get_table()  # Note, not thread-safe, so we can only have one poller

    def poll_worker():
        """
        Check DynamoDB for successful extracts.

        Get items from the queue 'to_poll'.
        When the item
            - is an identifier: poll DynamoDB
            - is an int: sleep that many seconds
        """
        while True:
            try:
                item = to_poll.get(block=False)
            except queue.Empty:
                logger.info("Poller: Nothing left to poll")
                return

            if isinstance(item, int):
                logger.debug("Poller: Reached end of relation list")
                logger.debug(
                    "Poller: Checking that we have fewer than %s tasks left by %s",
                    checkpoint_queue_size_cutoff,
                    checkpoint_time_cutoff,
                )
                logger.debug(
                    "Poller: %s left to poll, %s ready to load, %s elapsed",
                    to_poll.qsize(),
                    to_load.qsize(),
                    timer.elapsed,
                )
                if (
                    checkpoint_queue_size_cutoff == to_poll.qsize()
                    and timer.elapsed > checkpoint_time_cutoff
                ):
                    raise ETLRuntimeError(
                        "No new extracts found in last %s seconds, bailing out"
                        % idle_termination_seconds
                    )
                else:
                    if to_poll.qsize():
                        logger.debug("Poller: Sleeping for %s seconds", item)
                        time.sleep(item)
                        to_poll.put(item)
                    continue

            res = table.query(
                ConsistentRead=True,
                KeyConditionExpression="#ts > :dt and target = :table",
                FilterExpression="step = :step and event in (:fail_event, :finish_event)",
                ExpressionAttributeNames={"#ts": "timestamp"},
                ExpressionAttributeValues={
                    ":dt": cutoff_epoch,
                    ":table": item.identifier,
                    ":step": "extract",
                    ":fail_event": etl.monitor.STEP_FAIL,
                    ":finish_event": etl.monitor.STEP_FINISH,
                },
            )
            if res["Count"] == 0:
                to_poll.put(item)
            else:
                for extract_payload in res["Items"]:
                    if extract_payload["event"] == etl.monitor.STEP_FINISH:
                        logger.info(
                            "Poller: Recently completed extract found for '%s', marking as ready.",
                            item.identifier,
                        )
                    elif extract_payload["event"] == etl.monitor.STEP_FAIL:
                        logger.info(
                            "Poller: Recently failed extract found for '%s', marking as failed.",
                            item.identifier,
                        )
                        # We are not inside an exception handling so have no exception info.
                        item.mark_failure(relations, exc_info=None)
                    # We'll create the relation on success and failure (but skip copy on failure)
                    to_load.put(item)
                    # There should be only one event, but definitely don't queue for loading twice.
                    break

    uncaught_load_worker_exception = threading.Event()

    def load_worker():
        """
        Look for a ready-to-load relation from queue 'to_load'.

        If the item
            - is a relation: load it using connection pool 'pool'
            - is None: we're giving up, so return
        """
        while True:
            item = to_load.get()
            if item is None:
                break
            logger.info("Loader: Found %s ready to be loaded", item.identifier)
            try:
                build_one_relation_using_pool(pool, item, dry_run=dry_run)
            except (RelationConstructionError, RelationDataError):
                item.mark_failure(relations)
            except Exception:
                logger.error(
                    "Loader: Uncaught exception in load worker while loading '%s':",
                    item.identifier,
                    exc_info=True,
                )
                uncaught_load_worker_exception.set()
                raise

    to_poll = queue.Queue()  # type: ignore
    to_load = queue.Queue()  # type: ignore
    threads = []
    for _ in range(max_concurrency):
        t = threading.Thread(target=load_worker)
        t.start()
        threads.append(t)

    for relation in source_relations:
        logger.debug("Putting %s into poller queue", relation.identifier)
        to_poll.put(relation)
    to_poll.put(sleep_time)  # Give DynamoDB a periodic break

    # Track the queue size to detect progress
    checkpoint_queue_size_cutoff = to_poll.qsize() - 1  # -1 because worker will have one task 'in hand'
    poller = threading.Thread(target=poll_worker)
    poller.start()
    threads.append(poller)
    logger.info("Poller started; %s left to poll, %s ready to load", to_poll.qsize(), to_load.qsize())

    while poller.is_alive():
        # Give the poller time to realize it's passed the idle checkpoint if it was sleeping
        poller.join(idle_termination_seconds + sleep_time)
        logger.info("Poller joined or checkpoint timeout reached")
        # Update checkpoint for timer to have made an update
        checkpoint_time_cutoff = timer.elapsed + idle_termination_seconds
        logger.info(
            "Current elapsed time: %s; cancel if no progress by %s", timer, checkpoint_time_cutoff
        )
        # Update last known queue size
        checkpoint_queue_size_cutoff = to_poll.qsize()
        logger.info("Current queue length: %s", checkpoint_queue_size_cutoff)

    # When poller is done, send workers a 'stop' event and wait
    for _ in range(max_concurrency):
        to_load.put(None)
    for t in threads:
        t.join()
    # If the poller queue wasn't emptied, it exited unhappily
    if to_poll.qsize():
        raise ETLRuntimeError("Extract poller exited while to-poll queue was not empty")

    if uncaught_load_worker_exception.is_set():
        raise ETLRuntimeError("Data source loader thread(s) exited with uncaught exception")

    logger.info("Wrapping up work in %d worker(s): (%s)", max_concurrency, timer)
    failed_and_required = [rel.identifier for rel in source_relations if rel.failed and rel.is_required]
    if failed_and_required:
        raise RequiredRelationLoadError(failed_and_required)

    failed = [relation.identifier for relation in source_relations if relation.failed]
    if failed:
        logger.error(
            "These %d relation(s) failed to build: %s", len(failed), join_with_single_quotes(failed)
        )
    logger.info("Finished with %d relation(s) in source schemas (%s)", len(source_relations), timer)


# --- Section 4: Functions related to control flow


def create_source_tables_in_parallel(
    relations: Sequence[LoadableRelation], max_concurrency=1, dry_run=False
) -> None:
    """
    Create relations in parallel, using a connection pool, a thread pool, and a kiddie pool.

    We assume here that the relations have no dependencies on each other and just gun it.
    This will only raise an Exception if one of the created relations was marked as "required".

    Since these relations may have downstream dependents, we make sure to mark skip_copy on
    any relation from the full set of relations that depends on a source relation that failed
    to load.
    """
    source_relations = [relation for relation in relations if not relation.is_transformation]
    if not source_relations:
        logger.info("None of the relations are in source schemas")
        return
    timer = Timer()
    dsn_etl = etl.config.get_dw_config().dsn_etl
    pool = etl.db.connection_pool(max_concurrency, dsn_etl)
    futures: Dict[str, concurrent.futures.Future] = {}
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            for relation in source_relations:
                future = executor.submit(build_one_relation_using_pool, pool, relation, dry_run=dry_run)
                futures[relation.identifier] = future
            # For fail-fast, switch to FIRST_EXCEPTION below.
            done, not_done = concurrent.futures.wait(
                futures.values(), return_when=concurrent.futures.ALL_COMPLETED
            )
            cancelled = [future for future in not_done if future.cancel()]
            logger.info(
                "Wrapping up work in %d worker(s): %d done, %d not done (%d cancelled) (%s)",
                max_concurrency,
                len(done),
                len(not_done),
                len(cancelled),
                timer,
            )
    finally:
        pool.closeall()

    for relation in source_relations:
        try:
            futures[relation.identifier].result()
        except concurrent.futures.CancelledError:
            pass
        except (RelationConstructionError, RelationDataError):
            relation.mark_failure(relations)

    failed_and_required = [rel.identifier for rel in source_relations if rel.failed and rel.is_required]
    if failed_and_required:
        raise RequiredRelationLoadError(failed_and_required)

    failed = [relation.identifier for relation in source_relations if relation.failed]
    if failed:
        logger.error(
            "These %d relation(s) failed to build: %s", len(failed), join_with_single_quotes(failed)
        )
    logger.info("Finished with %d relation(s) in source schemas (%s)", len(source_relations), timer)


def create_transformations_sequentially(
    relations: Sequence[LoadableRelation], wlm_query_slots: int, statement_timeout: int, dry_run=False
) -> None:
    """
    Create relations one-by-one.

    If relations do depend on each other, we don't get ourselves in trouble here.
    If we trip over a "required" relation, an exception is raised.
    If dependencies were left empty, we'll fall back to skip_copy mode.

    Given a dependency tree of:
      A(required) <- B(required, per selector) <- C(not required) <- D (not required)
    Then failing to create either A or B will stop us. But failure on C will just leave D empty.
    N.B. It is not possible for a relation to be not required but have dependents that are
    (by construction).
    """
    transformations = [relation for relation in relations if relation.is_transformation]
    if not transformations:
        logger.info("None of the selected relations are in transformation schemas")
        return

    timer = Timer()
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True, readonly=dry_run)) as conn:
        etl.dialect.redshift.set_wlm_slots(conn, wlm_query_slots, dry_run=dry_run)
        etl.dialect.redshift.set_statement_timeout(conn, statement_timeout, dry_run=dry_run)
        for relation in transformations:
            try:
                build_one_relation(conn, relation, dry_run=dry_run)
            except (RelationConstructionError, RelationDataError) as exc:
                if relation.is_required:
                    raise RequiredRelationLoadError([relation.identifier]) from exc
                relation.mark_failure(relations)

    failed = [relation.identifier for relation in transformations if relation.failed]
    if failed:
        logger.error(
            "These %d relation(s) failed to build: %s", len(failed), join_with_single_quotes(failed)
        )
    skipped = [
        relation.identifier
        for relation in transformations
        if relation.skip_copy and not relation.is_view_relation
    ]
    if 0 < len(skipped) < len(transformations):
        logger.warning(
            "These %d relation(s) were left empty: %s", len(skipped), join_with_single_quotes(skipped)
        )
    logger.info(
        "Finished with %d relation(s) in transformation schemas (%s)", len(transformations), timer
    )


def create_relations(
    relations: Sequence[LoadableRelation],
    max_concurrency=1,
    wlm_query_slots=1,
    statement_timeout=0,
    concurrent_extract=False,
    dry_run=False,
) -> None:
    """Build relations by creating them, granting access, and loading them (if they hold data)."""
    if concurrent_extract:
        create_source_tables_when_ready(relations, max_concurrency, dry_run=dry_run)
    else:
        create_source_tables_in_parallel(relations, max_concurrency, dry_run=dry_run)

    create_transformations_sequentially(relations, wlm_query_slots, statement_timeout, dry_run=dry_run)


# --- Section 5: "Callbacks" (functions that implement commands)

# --- Section 5A: commands that modify tables and views


def load_data_warehouse(
    all_relations: Sequence[RelationDescription],
    selector: TableSelector,
    use_staging=True,
    max_concurrency=1,
    wlm_query_slots=1,
    statement_timeout=0,
    concurrent_extract=False,
    skip_copy=False,
    skip_loading_sources=False,
    dry_run=False,
):
    """
    Fully "load" the data warehouse after creating a blank slate.

    By default, we use staging positions of schemas to load and thus wait with
    moving existing schemas out of the way until the load is complete.

    This function allows only complete schemas as selection.

    1 Determine schemas that house any of the selected or dependent relations.
    2 Move old schemas in the data warehouse out of the way (for "backup").
    3 Create new schemas (and give access)
    4 Loop over all relations in selected schemas:
      4.1 Create relation (and give access)
      4.2 Load data into tables or CTAS (no further action for views)
          If it's a source table, use COPY to load data.
          If it's a CTAS with an identity column, create temp table, then move data into final table.
          If it's a CTAS without an identity column, insert values straight into final table.
    On error: exit if use_staging, otherwise restore schemas from backup position

    N.B. If arthur gets interrupted (eg. because the instance is inadvertently shut down),
    then there will be an incomplete state.

    This is a callback of a command.
    """
    selected_relations = etl.relation.select_in_execution_order(
        all_relations, selector, include_dependents=True
    )
    if not selected_relations:
        return

    relations = LoadableRelation.from_descriptions(
        selected_relations,
        "load",
        skip_copy=skip_copy,
        skip_loading_sources=skip_loading_sources,
        use_staging=use_staging,
    )
    traversed_schemas = find_traversed_schemas(relations)
    logger.info(
        "Starting to load %d relation(s) in %d schema(s)", len(relations), len(traversed_schemas)
    )

    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, autocommit=True)) as conn:
        tx_info = etl.data_warehouse.list_open_transactions(conn)
        etl.db.print_result("List of sessions that have open transactions:", tx_info)

    etl.data_warehouse.create_groups(dry_run=dry_run)
    create_schemas_for_rebuild(traversed_schemas, use_staging=use_staging, dry_run=dry_run)
    try:
        create_relations(
            relations,
            max_concurrency,
            wlm_query_slots,
            statement_timeout,
            concurrent_extract=concurrent_extract,
            dry_run=dry_run,
        )
    except ETLRuntimeError:
        if not use_staging:
            logger.info("Restoring %d schema(s) after load failure", len(traversed_schemas))
            etl.data_warehouse.restore_schemas(traversed_schemas, dry_run=dry_run)
        raise

    if use_staging:
        logger.info("Publishing %d schema(s) after load success", len(traversed_schemas))
        etl.data_warehouse.publish_schemas(traversed_schemas, dry_run=dry_run)


def upgrade_data_warehouse(
    all_relations: Sequence[RelationDescription],
    selector: TableSelector,
    max_concurrency=1,
    wlm_query_slots=1,
    statement_timeout=0,
    only_selected=False,
    include_immediate_views=False,
    continue_from: Optional[str] = None,
    use_staging=False,
    target_schema: Optional[str] = None,
    skip_copy=False,
    dry_run=False,
) -> None:
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

    Since views that directly hang off tables are deleted when their tables are deleted, the option
    exists to include those immediate views in the upgrade.

    If a target schema is provided, then the data is loaded into that schema instead of where the
    relation would normally land. Note that a check prevents loading tables from different
    execution levels in order to avoid loading tables that depend on each other. (In that case,
    the query would have to be rewritten to use the relocated table.)

    This is a callback of a command.
    """
    selected_relations = etl.relation.select_in_execution_order(
        all_relations,
        selector,
        include_dependents=not only_selected,
        include_immediate_views=include_immediate_views,
        continue_from=continue_from,
    )
    if not selected_relations:
        return

    involved_execution_levels = frozenset(
        funcy.distinct(relation.execution_level for relation in selected_relations)
    )
    if target_schema and len(involved_execution_levels) != 1:
        raise ETLRuntimeError(
            f"relations might depend on each other while target schema is in effect "
            f"(involved execution levels: {join_with_single_quotes(involved_execution_levels)})"
        )

    if only_selected and not include_immediate_views:
        immediate_views = [
            view.identifier for view in etl.relation.find_immediate_dependencies(all_relations, selector)
        ]
        if immediate_views:
            logger.warning(
                "These views are not part of the upgrade: %s", join_with_single_quotes(immediate_views)
            )
            logger.info(
                "Any views that depend in their query on tables that are part of the upgrade but"
                " are not selected will be missing once the upgrade completes."
            )

    relations = LoadableRelation.from_descriptions(
        selected_relations,
        "upgrade",
        skip_copy=skip_copy,
        use_staging=use_staging,
        target_schema=target_schema,
    )

    if target_schema:
        logger.info("Starting to load %d relation(s) into schema '%s'", len(relations), target_schema)
    else:
        traversed_schemas = find_traversed_schemas(relations)
        logger.info(
            "Starting to upgrade %d relation(s) in %d schema(s)", len(relations), len(traversed_schemas)
        )
        etl.data_warehouse.create_schemas(traversed_schemas, use_staging=use_staging, dry_run=dry_run)

    create_relations(relations, max_concurrency, wlm_query_slots, statement_timeout, dry_run=dry_run)


def update_data_warehouse(
    all_relations: Sequence[RelationDescription],
    selector: TableSelector,
    wlm_query_slots=1,
    statement_timeout=0,
    start_time: Optional[datetime] = None,
    only_selected=False,
    run_vacuum=False,
    dry_run=False,
):
    """
    Let new data percolate through the data warehouse.

    Within a transaction:
        Iterate over relations (selected or (selected and transitively dependent)):
            1 Delete rows
            2.Load data from upstream sources using COPY command, load data into CTAS using views
              for queries
            3 Verify constraints

    Note that a failure will rollback the transaction -- there is no distinction between required
    or not-required.
    Finally, if elected, run vacuum (in new connection) for all tables that were modified.

    This is a callback of a command.
    """
    selected_relations = etl.relation.select_in_execution_order(
        all_relations, selector, include_dependents=not only_selected
    )

    tables = [relation for relation in selected_relations if not relation.is_view_relation]
    if not tables:
        logger.warning("Found no tables matching: %s", selector)
        return

    source_relations = [relation for relation in selected_relations if not relation.is_transformation]
    if source_relations and start_time is not None:
        logger.info(
            "Verifying that all source relations have extracts after %s;"
            " fails if incomplete and no update for 1 hour" % start_time
        )
        extracted_targets = etl.monitor.recently_extracted_targets(source_relations, start_time)
        if len(source_relations) > len(extracted_targets):
            raise MissingExtractEventError(source_relations, extracted_targets)
    elif source_relations:
        logger.info(
            "Attempting to use existing manifests for source relations without verifying recency."
        )

    relations = LoadableRelation.from_descriptions(selected_relations, "update", in_transaction=True)
    logger.info("Starting to update %d tables(s) within a transaction", len(relations))
    dsn_etl = etl.config.get_dw_config().dsn_etl
    with closing(etl.db.connection(dsn_etl, readonly=dry_run)) as tx_conn, tx_conn as conn:
        etl.dialect.redshift.set_wlm_slots(conn, wlm_query_slots, dry_run=dry_run)
        etl.dialect.redshift.set_statement_timeout(conn, statement_timeout, dry_run=dry_run)
        for relation in relations:
            build_one_relation(conn, relation, dry_run=dry_run)

    if run_vacuum:
        vacuum(tables, dry_run=dry_run)


# --- Section 5B: commands that provide information about relations


def run_query(relation: RelationDescription, limit=None, use_staging=False) -> None:
    """
    Run the query for the relation (which must be a transformation, not a source).

    This is a callback of a command.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    loadable_relation = LoadableRelation(relation, {}, use_staging)
    timer = Timer()

    # We cannot use psycopg2's '%s' with LIMIT since the query may contain
    # arbitrary text, including "LIKE '%something%', which would break mogrify.
    limit_clause = "LIMIT NULL" if limit is None else f"LIMIT {limit:d}"
    query_stmt = loadable_relation.query_stmt + f"\n{limit_clause}\n"

    with closing(etl.db.connection(dsn_etl)) as conn:
        logger.info(
            "Running query underlying '%s' (with '%s')",
            relation.identifier,
            limit_clause,
        )
        results = etl.db.query(conn, query_stmt)
    logger.info(
        "Ran query underlying '%s' and received %d row(s) (%s)", relation.identifier, len(results), timer
    )

    # TODO(tom): This should grab the column names from the query to help with debugging.
    if relation.has_identity_column:
        columns = relation.unquoted_columns[1:]
    else:
        columns = relation.unquoted_columns
    print(format_lines(results, header_row=columns))


def check_constraints(relations: Sequence[RelationDescription], use_staging=False) -> None:
    """
    Check the table constraints of selected relations.

    This is a callback of a command.
    """
    dsn_etl = etl.config.get_dw_config().dsn_etl
    loadable_relations = [LoadableRelation(relation, {}, use_staging) for relation in relations]
    timer = Timer()

    with closing(etl.db.connection(dsn_etl)) as conn:
        for relation in loadable_relations:
            logger.info("Checking table constraints of '%s'", relation.identifier)
            verify_constraints(conn, relation)
    logger.info("Checked table constraints of %d relation(s) (%s)", len(loadable_relations), timer)


def show_downstream_dependents(
    relations: Sequence[RelationDescription],
    selector: TableSelector,
    continue_from: Optional[str] = None,
    with_dependencies: Optional[bool] = False,
    with_dependents: Optional[bool] = False,
) -> None:
    """
    List the execution order of loads or updates.

    Relations are marked based on whether they were directly selected or selected as
    part of the propagation of new data.
    They are also marked whether they'd lead to a fatal error since they're required for full load.

    This is a callback of a command.
    """
    # Relations are directly selected by pattern or by being somewhere downstream of a selected one.
    selected_relations = etl.relation.select_in_execution_order(
        relations, selector, include_dependents=True, continue_from=continue_from
    )
    if not selected_relations:
        return

    directly_selected_relations = etl.relation.find_matches(selected_relations, selector)
    selected = frozenset(relation.identifier for relation in directly_selected_relations)
    immediate_views = etl.relation.find_immediate_dependencies(selected_relations, selector)
    immediate = frozenset(relation.identifier for relation in immediate_views)
    logger.info(
        "Execution order includes %d selected, %d immediate, and %d other downstream relation(s)",
        len(selected),
        len(immediate),
        len(selected_relations) - len(selected) - len(immediate),
    )
    flag = {}
    for relation in selected_relations:
        if relation.identifier in selected:
            flag[relation.identifier] = "selected"
        elif relation.identifier in immediate:
            flag[relation.identifier] = "immediate"
        else:
            flag[relation.identifier] = "dependent"

    dependents = defaultdict(list)
    for relation in relations:
        for dependency in relation.dependencies:
            dependents[dependency.identifier].append(relation.identifier)

    # See computation of execution order -- anything depending on pg_catalog must come last.
    pg_catalog_dependency = {
        dependency.identifier
        for relation in selected_relations
        for dependency in relation.dependencies
        if dependency.schema == "pg_catalog"
    }
    current_index = {relation.identifier: i + 1 for i, relation in enumerate(selected_relations)}
    # Note that external tables are not in the list of relations (always level = 0),
    # and if a relation isn't part of downstream, they're considered built already (level = 0).
    current_level: Dict[str, int] = defaultdict(int)
    # Now set the level that we show so that it starts at 1 for the relations we're building here.
    # Pass 1: find out the largest level, ignoring pg_catalog dependencies.
    for relation in selected_relations:
        current_level[relation.identifier] = 1 + max(
            (current_level[dependency.identifier] for dependency in relation.dependencies), default=0
        )
    # Pass 2: update levels assuming pg_catalog is built at that largest level so far.
    pg_catalog_level = max(current_level.values())
    for identifier in pg_catalog_dependency:
        current_level[identifier] = pg_catalog_level
    for relation in selected_relations:
        current_level[relation.identifier] = 1 + max(
            (current_level[dependency.identifier] for dependency in relation.dependencies), default=0
        )

    width_selected = max(len(identifier) for identifier in current_index)
    width_dep = max(len(identifier) for identifier in current_level)
    line_template = (
        "{relation.identifier:{width}s}"
        " # {relation.source_type} index={index:4d} level={level:3d}"
        " flag={flag:9s}"
        " is_required={relation.is_required}"
    )
    dependency_template = "  #<- {identifier:{width}s} level={level:3d}"
    dependent_template = "  #-> {identifier:{width}s} index={index:4d} level={level:3d}"

    for relation in selected_relations:
        print(
            line_template.format(
                flag=flag[relation.identifier],
                index=current_index[relation.identifier],
                level=current_level[relation.identifier],
                relation=relation,
                width=width_selected,
            )
        )
        if with_dependencies:
            for dependency in sorted(relation.dependencies):
                print(
                    dependency_template.format(
                        identifier=dependency.identifier,
                        level=current_level[dependency.identifier],
                        width=width_dep,
                    )
                )
        if with_dependents:
            for dependent in sorted(dependents[relation.identifier], key=lambda x: current_index[x]):
                print(
                    dependent_template.format(
                        identifier=dependent,
                        index=current_index[dependent],
                        level=current_level[dependent],
                        width=width_dep,
                    )
                )


def show_upstream_dependencies(relations: Sequence[RelationDescription], selector: TableSelector):
    """
    List the relations upstream (towards sources) from the selected ones in execution order.

    This is a callback of a command.
    """
    execution_order = etl.relation.order_by_dependencies(relations)
    selected_relations = etl.relation.find_matches(execution_order, selector)
    if len(selected_relations) == 0:
        logger.warning("Found no matching relations for: %s", selector)
        return

    dependencies = {relation.identifier for relation in selected_relations}
    for relation in execution_order[::-1]:
        if relation.identifier in dependencies:
            dependencies.update(dep.identifier for dep in relation.dependencies)

    max_len = max(len(identifier) for identifier in dependencies)
    line_template = (
        "{relation.identifier:{width}s} # {relation.source_type} index={index:4d}"
        " is_required={relation.is_required}"
    )
    for i, relation in enumerate(execution_order):
        if relation.identifier in dependencies:
            print(line_template.format(index=i + 1, relation=relation, width=max_len))
