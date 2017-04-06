"""
The purpose of "validate" is to exercise the configuration, schemas, and queries that make up
the data warehouse description (and eventually, its state).

This includes static and runtime checks.  As always, the "dry-run" option tries to run as much
as possible but doesn't change any state (i.e. doesn't write or create tables or...)

Validating the warehouse schemas (definition of upstream tables and their transformations) includes:
* Syntax checks (valid JSON?, valid against the JSON schema?)
* Semantic checks (meaningful design files -- checks that are not described by a JSON schema)
* Reload-ability (finds source schemas corresponding to unloaded tables)
* Valid SQL in CTAS or views
# Correct list of columns in CTAS or views
* Correct description of dependencies
"""

import concurrent.futures
from contextlib import closing
import difflib
from itertools import groupby
import logging
from operator import attrgetter
import threading
from typing import FrozenSet, Iterable, List, Union

import psycopg2
from psycopg2.extensions import connection  # For type annotation
import simplejson as json

from etl.config.dw import DataWarehouseConfig, DataWarehouseSchema
import etl.design.bootstrap
from etl.errors import ETLConfigError, ETLDelayedExit, ETLRuntimeError  # Exception classes that we might catch
from etl.errors import TableDesignValidationError, UpstreamValidationError  # Exception classes that we might raise
from etl.names import join_with_quotes, TableName
import etl.pg
import etl.relation
from etl.relation import RelationDescription
from etl.timer import Timer


_error_occurred = threading.Event()


def validate_relation_description(description: RelationDescription, known_upstream_sources: FrozenSet,
                                  keep_going=False) -> Union[RelationDescription, None]:
    """
    Load table design (which always also validates against the schema) and add a check
    based on whether the relation is part of an upstream source or not.

    If we try to keep_going, then we don't fail but return None for invalid table designs.
    (This is the exception in this module.)
    """
    logger = logging.getLogger(__name__)
    logger.info("Loading and validating file '%s'", description.design_file_name)
    try:
        # Note that evaluating whether it's a CTAS or VIEW will trigger a load.
        has_upstream_source_name = description.table_design["source_name"] not in ["CTAS", "VIEW"]
        is_in_upstream_source = description.target_table_name.schema in known_upstream_sources
        if is_in_upstream_source and not has_upstream_source_name:
            raise TableDesignValidationError("invalid source name '%s' in upstream table '%s'" %
                                             (description.table_design["source_name"], description.identifier))
        elif not is_in_upstream_source and has_upstream_source_name:
            raise TableDesignValidationError("invalid source name '%s' in CTAS or VIEW '%s'" %
                                             (description.table_design["source_name"], description.identifier))
    except ETLConfigError:
        if keep_going:
            _error_occurred.set()
            logger.exception("Ignoring failure to validate '%s' and proceeding as requested:", description.identifier)
            return None
        else:
            raise
    return description


def validate_semantics(schemas: List[DataWarehouseSchema], descriptions: List[RelationDescription],
                       keep_going=False) -> List[RelationDescription]:
    """
    Load local design files and validate them along the way against schemas and semantics.

    Return list of successfully validated description or raise exception on validation error.
    """
    upstream_sources = frozenset(schema.name for schema in schemas if schema.is_upstream_source)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        result = executor.map(lambda v: validate_relation_description(v, upstream_sources, keep_going), descriptions)
    return list(filter(None, result))


def compare_query_to_design(from_query: Iterable, from_design: Iterable) -> Union[str, None]:
    """
    Calculate differences between what was found while running the query to what was declared in the design.

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

    not_in_design = join_with_quotes(actual - design)
    not_in_query = join_with_quotes(design - actual)

    if not_in_design and not_in_query:
        return "not listed in design = {}; not found from query = {}".format(not_in_design, not_in_query)
    elif not_in_design:
        return "not listed in design = {}".format(not_in_design)
    elif not_in_query:
        return "not found from query = {}".format(not_in_query)
    else:
        return None


def validate_dependencies(conn: connection, description: RelationDescription, tmp_view_name: TableName) -> None:
    """
    Download the dependencies (usually, based on the temporary view) and compare with table design.
    """
    logger = logging.getLogger(__name__)

    dependencies = etl.design.bootstrap.fetch_dependencies(conn, tmp_view_name)
    # We break with tradition and show the list of dependencies such that they can be copied into a design file.
    logger.info("Dependencies of '%s' per catalog: %s", description.identifier, json.dumps(dependencies))

    difference = compare_query_to_design(dependencies, description.table_design.get("depends_on", []))
    if difference:
        logger.error("Mismatch in dependencies of '{}': {}".format(description.identifier, difference))
        raise TableDesignValidationError("mismatched dependencies in '%s'" % description.identifier)
    else:
        logger.info('Dependencies listing in design file matches SQL')


def validate_column_ordering(conn: connection, description: RelationDescription, tmp_view_name: TableName) -> None:
    """
    Download the column order (using the temporary view) and compare with table design.
    """
    logger = logging.getLogger(__name__)

    attributes = etl.design.bootstrap.fetch_attributes(conn, tmp_view_name)
    actual_columns = [attribute.name for attribute in attributes]

    # Identity columns are inserted after the query has been run, so skip them here.
    expected_columns = [column["name"] for column in description.table_design["columns"]
                        if not (column.get("skipped") or column.get("identity"))]

    diff = get_list_difference(expected_columns, actual_columns)
    if diff:
        logger.error("Order of columns in design of '%s' does not match result of running its query",
                     description.identifier)
        logger.error("You need to replace, insert and/or delete in '%s' some column(s): %s",
                     description.identifier, join_with_quotes(diff))
        raise TableDesignValidationError("invalid columns or column order in '%s'" % description.identifier)
    else:
        logger.info("Order of columns in design of '%s' matches result of running SQL query", description.identifier)


def validate_single_transform(conn: connection, description: RelationDescription, keep_going: bool= False) -> None:
    """
    Test-run a relation (CTAS or VIEW) by creating a temporary view.

    With a view created, we can extract dependency information and a list of columns
    to make sure table design and query match up.
    """
    logger = logging.getLogger(__name__)
    try:
        with etl.relation.matching_temporary_view(conn, description) as tmp_view_name:
            validate_dependencies(conn, description, tmp_view_name)
            validate_column_ordering(conn, description, tmp_view_name)
    except (ETLConfigError, ETLRuntimeError, psycopg2.Error):
        if keep_going:
            _error_occurred.set()
            logger.exception("Ignoring failure to validate '%s' and proceeding as requested:",
                             description.identifier)
        else:
            raise


def validate_transforms(dsn: dict, descriptions: List[RelationDescription], keep_going: bool=False) -> None:
    """
    Validate transforms (CTAS or VIEW relations) by trying to run them in the database.
    This allows us to check their syntax, their dependencies, etc.
    """
    logger = logging.getLogger(__name__)
    transforms = [description for description in descriptions
                  if description.is_ctas_relation or description.is_view_relation]
    if not transforms:
        logger.info("No transforms found or selected, skipping CTAS or VIEW validation")
        return

    # TODO Can we run validation steps in parallel?
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in transforms:
            validate_single_transform(conn, description, keep_going=keep_going)


def get_list_difference(list1, list2):
    """
    Return list of elements that help turn list1 into list2 -- a "minimal" list of
    differences based on changing one list into the other.

    >>> sorted(get_list_difference(["a", "b"], ["b", "a"]))
    ['b']
    >>> sorted(get_list_difference(["a", "b"], ["a", "c", "b"]))
    ['c']
    >>> sorted(get_list_difference(["a", "b", "c", "d", "e"], ["a", "c", "b", "e"]))
    ['c', 'd']

    The last list happens because the matcher asks to insert 'c', then delete 'c' and 'd' later.
    """
    diff = set()
    matcher = difflib.SequenceMatcher(None, list1, list2)  # None means skip junk detection
    for code, left1, right1, left2, right2 in matcher.get_opcodes():
        if code != 'equal':
            diff.update(list1[left1:right1])
            diff.update(list2[left2:right2])
    return diff


def validate_reload(descriptions: List[RelationDescription], keep_going: bool):
    """
    Verify that columns between unloaded tables and reloaded tables are the same.

    Once the designs are validated, we can unload a relation 's.t' with a target 'u' and
    then extract and load it back into 'u.t'.

    Note that the order matters for these lists of columns.  (Which is also why we
    can't take the (symmetric) difference between columns but must be careful checking
    the column lists.)
    """
    logger = logging.getLogger(__name__)
    unloaded_descriptions = [d for d in descriptions if d.is_unloadable]
    descriptions_lookup = {d.identifier: d for d in descriptions}

    for unloaded in unloaded_descriptions:
        logger.debug("Checking whether '%s' is loaded back in", unloaded.identifier)
        reloaded = TableName(unloaded.unload_target, unloaded.target_table_name.table)
        if reloaded.identifier in descriptions_lookup:
            description = descriptions_lookup[reloaded.identifier]
            logger.info("Checking for consistency between '%s' and '%s'", unloaded.identifier, description.identifier)
            unloaded_columns = unloaded.unquoted_columns
            reloaded_columns = description.unquoted_columns
            if unloaded_columns != reloaded_columns:
                diff = get_list_difference(reloaded_columns, unloaded_columns)
                logger.error("Column difference detected between '%s' and '%s'",
                             unloaded.identifier, description.identifier)
                logger.error("You need to replace, insert and/or delete in '%s' some column(s): %s",
                             description.identifier, join_with_quotes(diff))
                if keep_going:
                    _error_occurred.set()
                else:
                    raise TableDesignValidationError("unloaded relation '%s' failed to match counterpart" %
                                                     unloaded.identifier)


def check_select_permission(conn: connection, table_name: TableName):
    """
    Check whether permissions on table will allow us to read from database.
    """
    # Why mess with querying the permissions table when you can just try to read (EAFP).
    statement = """SELECT 1 FROM {} WHERE FALSE""".format(table_name)
    try:
        etl.pg.execute(conn, statement)
    except psycopg2.Error as exc:
        raise UpstreamValidationError("failed to read from upstream table '%s'" % table_name.identifier) from exc


def validate_upstream_columns(conn: connection, table: RelationDescription) -> None:
    logger = logging.getLogger(__name__)
    source_table_name = table.source_table_name

    columns_info = etl.design.bootstrap.fetch_attributes(conn, source_table_name)
    if not columns_info:
        raise UpstreamValidationError("Table '%s' is gone or has no columns left" % source_table_name.identifier)
    logger.info("Found %d column(s) in relation '%s'", len(columns_info), source_table_name.identifier)

    current_columns = frozenset(column.name for column in columns_info)
    design_columns = frozenset(column for column in table.unquoted_columns if not column.startswith("etl__"))
    if not current_columns.issuperset(design_columns):
        extra_columns = design_columns.difference(current_columns)
        raise UpstreamValidationError("design of '%s' has columns that do not exist upstream: %s" %
                                      (source_table_name.identifier, join_with_quotes(extra_columns)))

    current_is_not_null = {column.name for column in columns_info if column.not_null}
    for column in table.table_design["columns"]:
        if column.get("not_null") and column["name"] not in current_is_not_null:
            # FIXME Too many errors ... be lenient when starting this test
            # raise TableDesignError("not null constraint of column '%s' in '%s' not enforced upstream" %
            #                        (column["name"], table.identifier))
            logger.warning("Not null constraint of column '%s' in '%s' not enforced upstream",
                           column["name"], table.identifier)


def validate_upstream_table(conn: connection, table: RelationDescription, keep_going: bool=False) -> None:
    """
    Validate table design of an upstream table against its source database.
    """
    logger = logging.getLogger(__name__)
    try:
        with etl.pg.log_error():
            check_select_permission(conn, table.source_table_name)
            validate_upstream_columns(conn, table)
        logger.info("Successfully validated '%s' against its upstream source", table.identifier)
    except (ETLConfigError, ETLRuntimeError, psycopg2.Error):
        if keep_going:
            _error_occurred.set()
            logger.exception("Ignoring failure to validate '%s' and proceeding as requested:", table.identifier)
        else:
            raise


def validate_upstream_sources(schemas: List[DataWarehouseSchema], descriptions: List[RelationDescription],
                              keep_going: bool=False) -> None:
    """
    Validate the designs (and the current configuration) in comparison to upstream databases.

    This can fail if
    (1) the upstream database is not reachable
    (2) the upstream table is not readable
    (3) the upstream table does not exist
    (4) the upstream columns are not a superset of the columns in the table design
    (5) the upstream column does not have the null constraint set while the table design does
    """
    logger = logging.getLogger(__name__)
    source_lookup = {schema.name: schema for schema in schemas if schema.is_database_source}

    # Re-sort descriptions by source so that we need to only connect once to each source
    upstream_tables = [description for description in descriptions if description.source_name in source_lookup]
    if not upstream_tables:
        logger.info("No upstream tables found or selected, skipping source validation")
        return
    upstream_tables.sort(key=attrgetter("source_name"))

    for source_name, table_group in groupby(upstream_tables, attrgetter("source_name")):
        tables = list(table_group)
        source = source_lookup[source_name]
        logger.info("Checking %d table(s) in upstream source '%s'", len(tables), source_name)
        with closing(etl.pg.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            for table in tables:
                validate_upstream_table(conn, table, keep_going=keep_going)


def validate_execution_order(descriptions: List[RelationDescription], keep_going=False):
    """
    Wrapper around order_by_dependencies to deal with our keep_going predilection.
    """
    logger = logging.getLogger(__name__)
    try:
        ordered_descriptions = etl.relation.order_by_dependencies(descriptions)
    except ETLConfigError:
        if keep_going:
            _error_occurred.set()
            logger.exception("Failed to determine evaluation order, proceeding as requested:")
            return descriptions
        else:
            raise
    return ordered_descriptions


def validate_designs(config: DataWarehouseConfig, descriptions: List[RelationDescription], keep_going=False,
                     skip_sources=False, skip_dependencies=False) -> None:
    """
    Make sure that all table design files pass the validation checks.

    See module documentation for list of checks.
    """
    logger = logging.getLogger(__name__)
    _error_occurred.clear()

    valid_descriptions = validate_semantics(config.schemas, descriptions, keep_going=keep_going)
    validate_reload(valid_descriptions, keep_going=keep_going)
    ordered_descriptions = validate_execution_order(valid_descriptions, keep_going=keep_going)

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
