"""
The purpose of "validate" is to exercise the configuration, schemas, and queries.

This includes static and runtime checks of our the description of the data warehouse.

As always, the "dry-run" option tries to run as much as possible but doesn't change any state
(i.e. doesn't write or create tables).

Validating the warehouse schemas (definition of upstream tables and their transformations) includes:
* Syntax checks (valid JSON?, valid against the JSON schema?)
* Semantic checks (meaningful design files -- checks that are not described by a JSON schema)
* Reload-ability (finds source schemas corresponding to unloaded tables)
* Valid SQL in CTAS or views
# Correct list of columns in CTAS or views
* Correct description of dependencies
"""

import concurrent.futures
import difflib
import logging
import threading
from contextlib import closing
from copy import deepcopy
from itertools import groupby
from operator import attrgetter
from typing import Iterable, List, Optional

import psycopg2
from psycopg2.extensions import connection as Connection  # only for type annotation

import etl.db
import etl.design.bootstrap
import etl.relation
from etl.config.dw import DataWarehouseSchema
from etl.errors import (
    ETLConfigError,
    ETLDelayedExit,
    ETLRuntimeError,
    TableDesignValidationError,
    UpstreamValidationError,
)
from etl.names import TableName, TempTableName
from etl.relation import RelationDescription
from etl.text import join_with_single_quotes
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_error_occurred = threading.Event()


def validate_relation_description(
    relation: RelationDescription, keep_going=False
) -> Optional[RelationDescription]:
    """
    Load table design (which always also validates against the schema).

    If we try to keep_going, then we don't fail but return None for invalid table designs.
    """
    logger.info("Loading and validating file '%s'", relation.design_file_name)
    try:
        relation.load()
    except ETLConfigError:
        if keep_going:
            _error_occurred.set()
            logger.exception(
                "Ignoring failure to validate '%s' and proceeding as requested:", relation.identifier
            )
            return None
        else:
            raise
    return relation


def validate_semantics(relations: List[RelationDescription], keep_going=False) -> List[RelationDescription]:
    """
    Load local design files and validate them along the way against schemas and semantics.

    Return list of successfully validated relations or raise exception on validation error.
    """
    # TODO With Python 3.6, we should pass in a thread_name_prefix
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        result = executor.map(lambda relation: validate_relation_description(relation, keep_going), relations)
    # Drop all relations from the result which returned None (meaning they failed validation).
    return list(filter(None, result))  # type: ignore


def compare_query_to_design(from_query: Iterable, from_design: Iterable) -> Optional[str]:
    """
    Calculate difference between the two lists and return human-interpretable string.

    The assumption here is that we can run a query to find dependencies
    and compare that list to what is stored in the table design file.

    >>> compare_query_to_design(["a", "b"], ["b", "a"])
    >>> compare_query_to_design(["a", "b"], ["a"])
    "not listed in design = 'b'"
    >>> compare_query_to_design(["a"], ["a", "b"])
    "not found from query = 'b'"
    >>> compare_query_to_design(["a", "b"], ["d", "c", "a"])
    "not listed in design = 'b'; not found from query = 'c', 'd'"
    """
    actual = frozenset(from_query)
    design = frozenset(from_design)

    not_in_design = join_with_single_quotes(actual - design)
    not_in_query = join_with_single_quotes(design - actual)

    if not_in_design and not_in_query:
        return "not listed in design = {}; not found from query = {}".format(not_in_design, not_in_query)
    elif not_in_design:
        return "not listed in design = {}".format(not_in_design)
    elif not_in_query:
        return "not found from query = {}".format(not_in_query)
    else:
        return None


def validate_dependencies(
    conn: Connection, relation: RelationDescription, tmp_view_name: TempTableName
) -> None:
    """Download the dependencies (based on a temporary view) and compare with table design."""
    if tmp_view_name.is_late_binding_view:
        dependencies = etl.design.bootstrap.fetch_dependency_hints(conn, relation.query_stmt)
        if dependencies is None:
            logger.warning("Unable to validate '%s' which depends on external tables", relation.identifier)
            return
        logger.info(
            "Dependencies of '%s' per query plan: %s",
            relation.identifier,
            join_with_single_quotes(dependencies),
        )
    else:
        dependencies = etl.design.bootstrap.fetch_dependencies(conn, tmp_view_name)
        logger.info(
            "Dependencies of '%s' per catalog: %s", relation.identifier, join_with_single_quotes(dependencies)
        )

    difference = compare_query_to_design(dependencies, relation.table_design.get("depends_on", []))
    if difference:
        logger.error("Mismatch in dependencies of '{}': {}".format(relation.identifier, difference))
        raise TableDesignValidationError("mismatched dependencies in '%s'" % relation.identifier)
    logger.info("Dependencies listing in design file for '%s' matches SQL", relation.identifier)


def validate_column_ordering(
    conn: Connection, relation: RelationDescription, tmp_view_name: TempTableName
) -> None:
    """Download the column order (using the temporary view) and compare with table design."""
    attributes = etl.design.bootstrap.fetch_attributes(conn, tmp_view_name)
    actual_columns = [attribute.name for attribute in attributes]

    if not actual_columns and tmp_view_name.is_late_binding_view:
        # Thanks to late-binding views it is not an error for a view to not be able to resolve
        # its columns.
        logger.warning(
            "Order of columns in design of '%s' cannot be validated because external table is missing",
            relation.identifier,
        )
        return

    # Identity columns are inserted after the query has been run, so skip them here.
    expected_columns = [
        column["name"]
        for column in relation.table_design["columns"]
        if not (column.get("skipped") or column.get("identity"))
    ]

    diff = get_list_difference(expected_columns, actual_columns)
    if diff:
        logger.error(
            "Order of columns in design of '%s' does not match result of running its query",
            relation.identifier,
        )
        logger.error(
            "You need to replace, insert and/or delete in '%s' some column(s): %s",
            relation.identifier,
            join_with_single_quotes(diff),
        )
        raise TableDesignValidationError("invalid columns or column order in '%s'" % relation.identifier)
    else:
        logger.info(
            "Order of columns in design of '%s' matches result of running SQL query", relation.identifier
        )


def validate_single_transform(
    conn: Connection, relation: RelationDescription, keep_going: bool = False
) -> None:
    """
    Test-run a relation (CTAS or VIEW) by creating a temporary view.

    With a view created, we can extract dependency information and a list of columns
    to make sure table design and query match up.
    """
    has_external_dependencies = any(dependency.is_external for dependency in relation.dependencies)
    try:
        with relation.matching_temporary_view(
            conn, as_late_binding_view=has_external_dependencies
        ) as tmp_view_name:
            validate_dependencies(conn, relation, tmp_view_name)
            validate_column_ordering(conn, relation, tmp_view_name)
    except (ETLConfigError, ETLRuntimeError, psycopg2.Error):
        if not keep_going:
            raise
        _error_occurred.set()
        logger.exception(
            "Ignoring failure to validate '%s' and proceeding as requested:", relation.identifier
        )


def validate_transforms(dsn: dict, relations: List[RelationDescription], keep_going: bool = False) -> None:
    """
    Validate transforms (CTAS or VIEW relations) by trying to run them in the database.

    This allows us to check their syntax, their dependencies, etc.
    """
    transforms = [
        relation for relation in relations if relation.is_ctas_relation or relation.is_view_relation
    ]
    if not transforms:
        logger.info("No transforms found or selected, skipping CTAS or VIEW validation")
        return

    # TODO Parallelize but use separate connections per thread
    with closing(etl.db.connection(dsn, autocommit=True)) as conn:
        for relation in transforms:
            validate_single_transform(conn, relation, keep_going=keep_going)


def get_list_difference(list1: List[str], list2: List[str]) -> List[str]:
    """
    Return list of elements that help turn list1 into list2.

    This should be a "minimal" list of differences based on changing one list into the other.

    >>> get_list_difference(["a", "b"], ["b", "a"])
    ['b']
    >>> get_list_difference(["a", "b"], ["a", "c", "b"])
    ['c']
    >>> get_list_difference(["a", "b", "c", "d", "e"], ["a", "c", "b", "e"])
    ['c', 'd']

    The last list happens because the matcher asks to insert 'c', then delete 'c' and 'd' later.
    """
    diff = set()
    matcher = difflib.SequenceMatcher(None, list1, list2)  # None means skip junk detection
    for code, left1, right1, left2, right2 in matcher.get_opcodes():
        if code != "equal":
            diff.update(list1[left1:right1])
            diff.update(list2[left2:right2])
    return sorted(diff)


def validate_reload(
    schemas: List[DataWarehouseSchema], relations: List[RelationDescription], keep_going: bool
):
    """
    Verify that columns between unloaded tables and reloaded tables are the same.

    Once the designs are validated, we can unload a relation 's.t' with a target 'u' and
    then extract and load it back into 'u.t'.

    Note that the order matters for these lists of columns.  (Which is also why we can't take
    the (symmetric) difference between columns but must be careful checking the column lists.)
    """
    unloaded_relations = [d for d in relations if d.is_unloadable]
    target_lookup = {schema.name for schema in schemas if schema.is_an_unload_target}
    relations_lookup = {d.identifier: d for d in relations}

    for unloaded in unloaded_relations:
        try:
            if unloaded.unload_target not in target_lookup:
                raise TableDesignValidationError(
                    "invalid target '{}' in unloadable relation '{}'".format(
                        unloaded.unload_target, unloaded.identifier
                    )
                )
            else:
                logger.debug("Checking whether '%s' is loaded back in", unloaded.identifier)
                reloaded = TableName(unloaded.unload_target, unloaded.target_table_name.table)
                if reloaded.identifier in relations_lookup:
                    relation = relations_lookup[reloaded.identifier]
                    logger.info(
                        "Checking for consistency between '%s' and '%s'",
                        unloaded.identifier,
                        relation.identifier,
                    )
                    unloaded_columns = unloaded.unquoted_columns
                    reloaded_columns = relation.unquoted_columns
                    if unloaded_columns != reloaded_columns:
                        diff = get_list_difference(reloaded_columns, unloaded_columns)
                        logger.error(
                            "Column difference detected between '%s' and '%s'",
                            unloaded.identifier,
                            relation.identifier,
                        )
                        logger.error(
                            "You need to replace, insert and/or delete in '%s' some column(s): %s",
                            relation.identifier,
                            join_with_single_quotes(diff),
                        )
                        raise TableDesignValidationError(
                            "unloaded relation '%s' failed to match counterpart" % unloaded.identifier
                        )
        except TableDesignValidationError:
            if keep_going:
                _error_occurred.set()
                logger.exception(
                    "Ignoring failure to validate '%s' and proceeding as requested:", unloaded.identifier
                )
            else:
                raise


def check_select_permission(conn: Connection, table_name: TableName):
    """Check whether permissions on table will allow us to read from database."""
    # Why mess with querying the permissions table when you can just try to read (EAFP).
    statement = """SELECT 1 AS check_permission FROM {} WHERE FALSE""".format(table_name)
    try:
        etl.db.execute(conn, statement)
    except psycopg2.Error as exc:
        raise UpstreamValidationError(
            "failed to read from upstream table '%s'" % table_name.identifier
        ) from exc


def validate_upstream_columns(conn: Connection, table: RelationDescription) -> None:
    """
    Compare columns in upstream table to the table design file.

    It is an ERROR if the design lists columns that do not exist in the upstream table. Exceptions
    here are calculated columns (those starting with etl__) or columns that are marked as skipped.

    It causes a WARNING to have more columns in the upstream table than are defined in the design
    or to have columns skipped in the design that do not exist upstream.
    """
    source_table_name = table.source_table_name

    columns_info = etl.design.bootstrap.fetch_attributes(conn, source_table_name)
    if not columns_info:
        raise UpstreamValidationError(
            "table '%s' is gone or has no columns left" % source_table_name.identifier
        )
    logger.info("Found %d column(s) in relation '%s'", len(columns_info), source_table_name.identifier)

    current_columns = frozenset(column.name for column in columns_info)
    design_columns = frozenset(
        column["name"] for column in table.table_design["columns"] if not column["name"].startswith("etl__")
    )
    design_required_columns = frozenset(
        column["name"]
        for column in table.table_design["columns"]
        if column["name"] in design_columns and not column.get("skipped", False)
    )

    missing_required_columns = design_required_columns.difference(current_columns)
    if missing_required_columns:
        raise UpstreamValidationError(
            "design of '%s' has columns that do not exist upstream: %s"
            % (source_table_name.identifier, join_with_single_quotes(missing_required_columns))
        )

    extra_design_columns = design_columns.difference(current_columns)
    if extra_design_columns:
        logger.warning(
            "Column(s) that are in the design of '%s' but do not exist upstream in '%s': %s",
            table.identifier,
            table.source_name,
            join_with_single_quotes(extra_design_columns),
        )

    missing_design_columns = current_columns.difference(design_columns)
    if missing_design_columns:
        logger.warning(
            "Column(s) that exist upstream in '%s' but not in the design '%s': %s",
            table.source_name,
            table.identifier,
            join_with_single_quotes(missing_design_columns),
        )

    current_is_not_null = {column.name for column in columns_info if column.not_null}
    for column in table.table_design["columns"]:
        if column.get("not_null") and column["name"] not in current_is_not_null:
            raise TableDesignValidationError(
                "not null constraint of column '{}' in '{}' not enforced upstream".format(
                    column["name"], table.identifier
                )
            )


def validate_upstream_constraints(conn: Connection, table: RelationDescription) -> None:
    """
    Compare table constraints between database and table design file.

    Note that "natural_key" or "surrogate_key" constraints are not valid in upstream
    (source) tables. Also, a "primary_key" in upstream may be used as a "unique" constraint
    in the design (but not vice versa).
    """
    current_constraint = etl.design.bootstrap.fetch_constraints(conn, table.source_table_name)
    design_constraint = table.table_design.get("constraints", [])

    current_primary_key = frozenset(col for c in current_constraint for col in c.get("primary_key", []))
    current_uniques = [frozenset(c["unique"]) for c in current_constraint if "unique" in c]

    design_primary_key = frozenset(col for c in design_constraint for col in c.get("primary_key", []))
    design_uniques = [frozenset(c["unique"]) for c in design_constraint if "unique" in c]

    # We'll pluck from the not_used info and report if anything wasn't used in the design.
    not_used = deepcopy(current_constraint)

    if design_primary_key:
        if current_primary_key == design_primary_key:
            for i in range(len(not_used)):
                if "primary_key" in not_used[i]:
                    del not_used[i]
                    break
        elif current_primary_key:
            raise TableDesignValidationError(
                "the primary_key constraint in '%s' (%s) does not match upstream (%s)"
                % (
                    table.identifier,
                    join_with_single_quotes(design_primary_key),
                    join_with_single_quotes(current_primary_key),
                )
            )
        else:
            raise TableDesignValidationError(
                "the primary key constraint in '%s' (%s) is not enforced upstream"
                % (table.identifier, join_with_single_quotes(design_primary_key))
            )

    for design_unique in design_uniques:
        if current_primary_key == design_unique:
            for i in range(len(not_used)):
                if "primary_key" in not_used[i]:
                    del not_used[i]
                    break
        if design_unique in current_uniques:
            for i in range(len(not_used)):
                if "unique" in not_used[i] and frozenset(not_used[i]["unique"]) == design_unique:
                    del not_used[i]
                    break
        if current_primary_key != design_unique and design_unique not in current_uniques:
            raise TableDesignValidationError(
                "the unique constraint in '%s' (%s) is not enforced upstream"
                % (table.identifier, join_with_single_quotes(design_unique))
            )

    for constraint in not_used:
        for constraint_type, columns in constraint.items():
            logger.warning(
                "Upstream source has additional %s constraint (%s) for '%s'",
                constraint_type,
                join_with_single_quotes(columns),
                table.table_design["source_name"],
            )


def validate_upstream_table(conn: Connection, table: RelationDescription, keep_going: bool = False) -> None:
    """Validate table design of an upstream table against its source database."""
    try:
        with etl.db.log_error():
            check_select_permission(conn, table.source_table_name)
            validate_upstream_columns(conn, table)
            validate_upstream_constraints(conn, table)
        logger.info("Successfully validated '%s' against its upstream source", table.identifier)
    except (ETLConfigError, ETLRuntimeError, psycopg2.Error):
        if keep_going:
            _error_occurred.set()
            logger.exception(
                "Ignoring failure to validate '%s' and proceeding as requested:", table.identifier
            )
        else:
            raise


def validate_upstream_sources(
    schemas: List[DataWarehouseSchema], relations: List[RelationDescription], keep_going: bool = False
) -> None:
    """
    Validate the designs (and the current configuration) in comparison to upstream databases.

    This can fail if
    (1) the upstream database is not reachable
    (2) the upstream table is not readable
    (3) the upstream table does not exist
    (4) the upstream columns are not a superset of the columns in the table design
    (5) the upstream column does not have the null constraint set while the table design does
    """
    source_lookup = {schema.name: schema for schema in schemas if schema.is_database_source}

    # Re-sort relations by source so that we need to only connect once to each source
    upstream_tables = [relation for relation in relations if relation.source_name in source_lookup]
    if not upstream_tables:
        logger.info("No upstream tables found or selected, skipping source validation")
        return
    upstream_tables.sort(key=attrgetter("source_name"))

    # TODO Parallelize around sources (like extract)
    for source_name, table_group in groupby(upstream_tables, attrgetter("source_name")):
        tables = list(table_group)
        source = source_lookup[source_name]
        logger.info("Checking %d table(s) in upstream source '%s'", len(tables), source_name)
        with closing(etl.db.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            for table in tables:
                validate_upstream_table(conn, table, keep_going=keep_going)


def validate_execution_order(relations: List[RelationDescription], keep_going=False):
    """
    Make sure we can build an execution order.

    We'll catch an exception and set a flag if the keep_going option is true.
    """
    try:
        ordered_relations = etl.relation.order_by_dependencies(relations)
    except ETLConfigError:
        if keep_going:
            _error_occurred.set()
            logger.exception("Failed to determine evaluation order, proceeding as requested:")
            return relations
        else:
            raise
    return ordered_relations


def validate_designs(
    relations: List[RelationDescription],
    keep_going=False,
    skip_sources=False,
    skip_dependencies=False,
) -> None:
    """
    Make sure that all table design files pass the validation checks.

    See module documentation for list of checks.
    """
    config = etl.config.get_dw_config()
    _error_occurred.clear()

    valid_descriptions = validate_semantics(relations, keep_going=keep_going)
    ordered_descriptions = validate_execution_order(valid_descriptions, keep_going=keep_going)

    validate_reload(config.schemas, valid_descriptions, keep_going=keep_going)

    if skip_sources:
        logger.info("Skipping validation of designs against upstream sources")
    else:
        with Timer() as timer:
            validate_upstream_sources(config.schemas, ordered_descriptions, keep_going=keep_going)
            logger.info("Validated designs against upstream sources (%s)", timer)

    if skip_dependencies:
        logger.info("Skipping validation of transforms against data warehouse")
    else:
        with Timer() as timer:
            validate_transforms(config.dsn_etl, ordered_descriptions, keep_going=keep_going)
            logger.info("Validated transforms against data warehouse (%s)", timer)

    if _error_occurred.is_set():
        raise ETLDelayedExit("At least one error occurred while validating with 'keep going' option")
