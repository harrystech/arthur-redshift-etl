"""
The purpose of "validate" is to exercise the configuration, schemas, and queries that make up
the data warehouse description (and eventually, its state).

This includes static and runtime checks.  As always, the "dry-run" option tries to run as much
as possible but doesn't change any state (i.e. doesn't write or create tables or...)

Validating the warehouse schemas (definition of upstream tables and their transformations) includes:
* Syntax checks (valid json?)
* "Schema" checks (valid design files, "schema" here refers to the description of valid fields etc.)
* Semantic checks (meaningful design files -- checks that are not described by a "schema")
* Reload-ability (finds source schemas corresponding to unloaded tables)
* Valid SQL in CTAS or views
* Correct description of dependencies
"""

from contextlib import closing
import difflib
from itertools import groupby
import logging
from operator import attrgetter
import threading
from typing import List

import psycopg2
from psycopg2.extensions import connection  # For type annotation
import simplejson as json

from etl import join_with_quotes, TableName
from etl.config.dw import DataWarehouseConfig, DataWarehouseSchema
import etl.design.bootstrap
from etl.errors import ETLConfigError, ETLDelayedExit, ETLError, UpstreamValidationError, TableDesignError
import etl.pg
import etl.relation
from etl.relation import RelationDescription


_error_occurred = threading.Event()  # overkill but at least thread-safe overkill


def validate_design_file_semantics(schemas: List[DataWarehouseSchema], descriptions: List[RelationDescription],
                                   keep_going=False) -> List[RelationDescription]:
    """
    Load local design files and validate them along the way against schemas and semantics.

    Return list of successfully validated descriptions if you want to keep going.
    Or raise exception on validation error.
    """
    logger = logging.getLogger(__name__)
    ok = []
    is_upstream_source = {schema.name: schema.is_upstream_source for schema in schemas}
    for description in descriptions:
        logger.info("Loading and validating file '%s'", description.design_file_name)
        try:
            # Note that evaluating whether it's a ctas or view will trigger a load and basic validation.
            source_name = description.target_table_name.schema
            if is_upstream_source[source_name]:
                if description.is_ctas_relation or description.is_view_relation:
                    raise TableDesignError("Invalid source name '%s' in upstream table '%s'" %
                                           (description.table_design["source_name"], description.identifier))
            else:
                if not (description.is_ctas_relation or description.is_view_relation):
                    raise TableDesignError("Invalid source name '%s' in CTAS or VIEW '%s'" %
                                           (description.table_design["source_name"], description.identifier))
            ok.append(description)
        except TableDesignError:
            if keep_going:
                _error_occurred.set()
                logger.exception("Ignoring failure to validate '%s' and proceeding as requested:",
                                 description.identifier)
            else:
                raise
    return ok


def _check_dependencies(observed, table_design):
    """
    Compare actual dependencies to a table design object and return instructions for logger

    >>> _check_dependencies(['abc.123', '123.abc'], dict(name='fish', depends_on=['123.abc']))  # doctest: +ELLIPSIS
    'Dependency tracking mismatch! payload =...]}'

    >>> _check_dependencies(['abc.123', '123.abc'], dict(name='fish', depends_on=['123.abc', 'abc.123']))

    """
    expected = table_design.get('depends_on', [])

    observed_deps = set(observed)
    expected_deps = set(expected)
    if not observed_deps == expected_deps:
        return 'Dependency tracking mismatch! payload = {}'.format(json.dumps(dict(
                 full_dependencies=observed,
                 table=table_design['name'],
                 actual_but_unlisted=list(observed_deps - expected_deps),
                 listed_but_not_actual=list(expected_deps - observed_deps))))


def create_temporary_view(conn: connection, description: RelationDescription, keep_going: bool=False) -> TableName:
    """
    Create a temporary view for testing the description.

    This will be a view for both, CTAS and VIEW.
    """
    logger = logging.getLogger(__name__)
    # TODO Switch to using a temporary view (starts with '#' and has no schema)
    tmp_view_name = TableName(schema=description.target_table_name.schema,
                              table='$'.join(["arthur_temp", description.target_table_name.table]))
    ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(tmp_view_name, description.query_stmt)
    logger.info("Creating view '%s' for relation '%s'" % (tmp_view_name.identifier, description.identifier))
    try:
        with etl.pg.log_error():
            etl.pg.execute(conn, ddl_stmt)
    except psycopg2.Error:
        if keep_going:
            _error_occurred.set()
        else:
            raise
    return tmp_view_name


def validate_dependencies(conn: connection, description: RelationDescription, tmp_view_name: TableName,
                          keep_going: bool=False):
    """
    Download the dependencies (usually, based on the temporary view) and compare with table design.
    """
    logger = logging.getLogger(__name__)

    dependencies = etl.design.bootstrap.fetch_dependencies(conn, tmp_view_name)
    logger.info("Dependencies of '%s' per catalog: %s", description.identifier, join_with_quotes(dependencies))

    comparison_output = _check_dependencies(dependencies, description.table_design)
    if comparison_output:
        if keep_going:
            _error_occurred.set()
            logger.warning(comparison_output)
        else:
            raise TableDesignError(comparison_output)
    else:
        logger.info('Dependencies listing in design file matches SQL')


def validate_column_ordering(conn: connection, description: RelationDescription, tmp_view_name: TableName,
                             keep_going: bool=False):
    """
    Download the column order (usually, based on the temporary view) and compare with table design.
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
                     description.identifier, etl.join_with_quotes(diff))
        if keep_going:
            _error_occurred.set()
        else:
            raise TableDesignError("Invalid columns or column order in '%s'" % description.identifier)
    else:
        logger.info("Order of columns in design of '%s' matches result of running SQL query", description.identifier)


def validate_single_transform(conn: connection, description: RelationDescription, keep_going: bool=False):
    """
    Test-run a relation (CTAS or VIEW) by creating a temporary view.

    With a view created, we can extract dependency information and a list of columns
    to make sure table design and query match up.
    """
    logger = logging.getLogger(__name__)
    tmp_view_name = create_temporary_view(conn, description, keep_going)

    validate_dependencies(conn, description, tmp_view_name, keep_going)
    validate_column_ordering(conn, description, tmp_view_name, keep_going)

    logger.info("Dropping view '%s'", tmp_view_name.identifier)
    etl.pg.execute(conn, "DROP VIEW IF EXISTS {}".format(tmp_view_name))


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

    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in transforms:
            try:
                with etl.pg.log_error():
                    validate_single_transform(conn, description, keep_going=keep_going)
            except (ETLError, psycopg2.Error):
                if keep_going:
                    _error_occurred.set()
                    logger.exception("Ignoring failure to validate '%s' and proceeding as requested:",
                                     description.identifier)
                else:
                    raise


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
                             description.identifier, etl.join_with_quotes(diff))
                if keep_going:
                    _error_occurred.set()
                else:
                    raise TableDesignError("Unloaded relation '%s' failed to match counterpart" % unloaded.identifier)


def check_select_permission(conn: connection, table_name: TableName):
    """
    Check whether permissions on table will allow us to read from database.
    """
    # Why mess with querying the permissions table when you can just try to read (EAFP).
    statement = """SELECT 1 FROM {} WHERE FALSE""".format(table_name)
    try:
        etl.pg.execute(conn, statement)
    except psycopg2.Error as exc:
        raise UpstreamValidationError("Failed to read from upstream table '%s'" % table_name.identifier) from exc


def validate_upstream_table(conn: connection, table: RelationDescription, keep_going=False):
    """
    Validate table design of an upstream table against its source database.
    """
    logger = logging.getLogger(__name__)
    source_table_name = table.source_table_name
    try:
        check_select_permission(conn, source_table_name)
        columns_info = etl.design.bootstrap.fetch_attributes(conn, source_table_name)
        if not columns_info:
            raise TableDesignError("Table '%s' is gone or has no columns left" % source_table_name.identifier)
        current_columns = frozenset(column.name for column in columns_info)
        design_columns = frozenset(column for column in table.unquoted_columns if not column.startswith("etl__"))
        if not current_columns.issuperset(design_columns):
            extra_columns = design_columns.difference(current_columns)
            raise TableDesignError("Design of '%s' has columns that do not exist upstream: %s" %
                                   (source_table_name.identifier, join_with_quotes(extra_columns)))
        current_is_not_null = {column.name for column in columns_info if column.not_null}
        for column in table.table_design["columns"]:
            if column.get("not_null") and column["name"] not in current_is_not_null:
                # FIXME Too many errors ... be lenient when starting this test
                # raise TableDesignError("Not null constraint of column '%s' in '%s' not enforced upstream" %
                #                        (column["name"], table.identifier))
                logger.warning("Not null constraint of column '%s' in '%s' not enforced upstream",
                               column["name"], table.identifier)
        logger.info("Successfully validated '%s' against its upstream source", table.identifier)
    except (psycopg2.Error, ETLError):
        if keep_going:
            _error_occurred.set()
            logger.exception("Ignoring failure to validate '%s' and proceeding as requested:",
                             source_table_name.identifier)
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
        try:
            with closing(etl.pg.connection(source.dsn, autocommit=True, readonly=True)) as conn:
                for table in tables:
                    validate_upstream_table(conn, table, keep_going=keep_going)
        except psycopg2.Error:
            if keep_going:
                _error_occurred.set()
                logger.exception("Ignoring failure to validate '%s' and proceeding as requested:", source_name)
            else:
                raise


def validate_designs(config: DataWarehouseConfig, descriptions: List[RelationDescription], keep_going=False,
                     skip_sources=False, skip_dependencies=False) -> None:
    """
    Make sure that all table design files pass the validation checks.
    """
    logger = logging.getLogger(__name__)
    _error_occurred.clear()

    valid_descriptions = validate_design_file_semantics(config.schemas, descriptions, keep_going=keep_going)
    validate_reload(valid_descriptions, keep_going=keep_going)
    try:
        ordered_descriptions = etl.relation.order_by_dependencies(valid_descriptions)
    except ETLConfigError:
        if keep_going:
            _error_occurred.set()
            logger.exception("Failed to determine evaluation order, proceeding as requested")
            ordered_descriptions = valid_descriptions
        else:
            raise

    if skip_sources:
        logger.info("Skipping validation of designs against upstream sources")
    else:
        validate_upstream_sources(config.schemas, ordered_descriptions, keep_going=keep_going)

    if skip_dependencies:
        logger.info("Skipping validation of transforms against data warehouse")
    else:
        validate_transforms(config.dsn_etl, ordered_descriptions, keep_going=keep_going)

    if _error_occurred.is_set():
        raise ETLDelayedExit("At least one error occurred while validating with 'keep going' option")
