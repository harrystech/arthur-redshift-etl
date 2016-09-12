"""
Work with relations: tables, CTAS tables, or views

Table descriptions -- model around the notion of relations and their support files
    "Table" relations are tables created based on data probably coming from an upstream source.
    "CTAS" relations are tables that are created using queries and transform data; unlike
        views they have disk storage and thus distributions and column encodings.
    "VIEW" relations are views that are queries but unlike CTAS don't use disk space.

The descriptions of relations contain access to:
    "table designs" which describe the name and columns as well as constraints and attributes
         for the table or its columns
    "queries" which are the SQL SELECT statements backing the CTAS or VIEW
    "manifests" which are lists of data files for tables backed by upstream sources
"""

from contextlib import closing
import difflib
from functools import partial
import logging
from operator import attrgetter
from queue import PriorityQueue

import simplejson as json

import etl
import etl.config
import etl.design
import etl.dump
import etl.pg
import etl.file_sets


class MissingQueryError(etl.ETLException):
    pass


class CyclicDependencyError(etl.ETLException):
    pass


class RelationDescription:

    def __init__(self, discovered_files: etl.file_sets.TableFileSet, bucket_name=None):
        # Basic properties to locate files describing the relation
        # TODO Make this pass-thru to TableFileSet
        self.source_path_name = discovered_files.source_path_name
        self.source_table_name = discovered_files.source_table_name
        self.target_table_name = discovered_files.target_table_name
        self.design_file_name = discovered_files.design_file
        self.sql_file_name = discovered_files.sql_file
        self.manifest_file_name = discovered_files.manifest_file
        # Remove or not? If bucket_name is specified, assume S3 otherwise go with local files
        self.bucket_name = bucket_name
        # Lazy-loading of table design, query statement, etc.
        self._table_design = None
        self._query_stmt = None
        # Properties for ordering relations
        self.order = None
        self._dependencies = None

    def __str__(self):
        return "{}({}:{},#{})".format(self.__class__.__name__, self.identifier, self.source_path_name, self.order)

    @property
    def identifier(self):
        return self.target_table_name.identifier

    @property
    def table_design(self):
        if self._table_design is None:
            if self.bucket_name:
                loader = partial(etl.design.download_table_design, self.bucket_name)
            else:
                loader = partial(etl.design.validate_table_design_from_file)
            self._table_design = loader(self.design_file_name, self.target_table_name)
        return self._table_design

    @property
    def is_ctas_relation(self):
        return self.table_design["source_name"] == "CTAS"

    @property
    def is_view_relation(self):
        return self.table_design["source_name"] == "VIEW"

    @property
    def query_stmt(self):
        if self._query_stmt is None:
            if self.sql_file_name is None:
                raise MissingQueryError("Missing SQL file for '{}'".format(self.identifier))
            if self.bucket_name:
                with closing(etl.file_sets.get_file_content(self.bucket_name, self.sql_file_name)) as content:
                    query_stmt = content.read().decode()
            else:
                with open(self.sql_file_name) as f:
                    query_stmt = f.read()
            self._query_stmt = query_stmt.strip().rstrip(';')
        return self._query_stmt

    @property
    def dependencies(self):
        if self._dependencies is None:
            self._dependencies = set(self.table_design.get("depends_on", []))
        return self._dependencies

    @dependencies.setter
    def dependencies(self, value):
        self._dependencies = value


def order_by_dependencies(table_descriptions):
    """
    Sort the relations such that any dependents surely are loaded afterwards.

    If a table (or view) depends on other tables, then its order is larger
    than any of its dependencies.
    If a table (or view) depends on a table that is not actually part of our input,
    then the order is forced to have a gap and start at N where N is the total number
    of known tables. (This creates an imaginary table at the end of the list that that
    table depends on. Of sorts.)
    """
    known_tables = frozenset({description.identifier for description in table_descriptions})
    n = len(known_tables)

    queue = PriorityQueue()
    for i, description in enumerate(table_descriptions):
        unknown = description.dependencies.difference(known_tables)
        if unknown:
            description.dependencies = description.dependencies.difference(unknown)
            queue.put((n + 1, i, description))
        else:
            queue.put((1, i, description))

    table_map = {description.identifier: description for description in table_descriptions}
    latest = 0
    while not queue.empty():
        minimum, tie_breaker, description = queue.get()
        if minimum > 2 * n:
            raise CyclicDependencyError("Cannot determine order, suspect cycle in DAG of dependencies")
        others = [table_map[dep].order for dep in description.dependencies]
        if not others:
            latest = description.order = max(latest + 1, minimum)
        elif all(others):
            latest = description.order = max(max(others) + 1, latest + 1, minimum)
        elif any(others):
            at_least = max(order for order in others if order is not None)
            queue.put((max(at_least, latest, minimum) + 1, tie_breaker, description))
        else:
            queue.put((max(latest, minimum) + 1, tie_breaker, description))

    return sorted(table_descriptions, key=attrgetter("order"))


def validate_table_as_view(conn, description):
    """
    Test-run a relation (CTAS or VIEW) by creating a temporary view.

    With a view created, we can extract dependency information and a list of columns
    to make sure table design and query match up.
    """
    logger = logging.getLogger(__name__)
    # FIXME Switch to using a temporary view (starts with '#' and has no schema)
    tmp_view_name = etl.TableName(schema=description.target_table_name.schema,
                                  table='$'.join(["arthur_temp", description.target_table_name.table]))
    ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(tmp_view_name, description.query_stmt)
    logger.info("Creating view '%s' for table '%s'" % (tmp_view_name.identifier, description.target_table_name))
    etl.pg.execute(conn, ddl_stmt)

    # based off example query in AWS docs; *_p is for parent, *_c is for child
    dependency_stmt = """SELECT DISTINCT
           n_c.nspname AS dependency_schema
         , c_c.relname AS dependency_name
      FROM pg_class c_p
      JOIN pg_depend d_p ON c_p.relfilenode = d_p.refobjid
      JOIN pg_depend d_c ON d_p.objid = d_c.objid
      -- the following OR statement covers the case where a COPY has issued a new OID for an upstream table
      JOIN pg_class c_c ON d_c.refobjid = c_c.relfilenode OR d_c.refobjid = c_c.oid
      LEFT JOIN pg_namespace n_p ON c_p.relnamespace = n_p.oid
      LEFT JOIN pg_namespace n_c ON c_c.relnamespace = n_c.oid
     WHERE c_p.relname = '{table}' AND n_p.nspname = '{schema}'
        -- do not include the table itself in its dependency list
       AND c_p.oid != c_c.oid""".format(
        schema=tmp_view_name.schema, table=tmp_view_name.table)

    dependencies = [etl.TableName(schema=row['dependency_schema'], table=row['dependency_name']).identifier
                    for row in etl.pg.query(conn, dependency_stmt)]
    dependencies.sort()
    logger.info("Dependencies discovered: [{}]".format(', '.join(dependencies)))

    comparison_output = _check_dependencies(dependencies, description.table_design)
    if comparison_output:
        logger.warning(comparison_output)
    else:
        logger.info('Dependencies listing in design file matches SQL')

    columns_stmt = """SELECT a.attname
      FROM pg_class c, pg_attribute a, pg_type t, pg_namespace n
     WHERE c.relname = '{table}'
       AND a.attnum > 0
       AND a.attrelid = c.oid
       AND a.atttypid = t.oid
       AND c.relnamespace = n.oid
       AND n.nspname = '{schema}'
     ORDER BY attnum ASC""".format(
        schema=tmp_view_name.schema, table=tmp_view_name.table)

    actual_columns = [row['attname'] for row in etl.pg.query(conn, columns_stmt)]
    comparison_output = _check_columns(actual_columns, description.table_design)
    if comparison_output:
        logger.warning(comparison_output)
    else:
        logger.info('Column listing in design file matches column listing in SQL')

    logger.info("Dropping view '%s'", tmp_view_name.identifier)
    etl.pg.execute(conn, "DROP VIEW IF EXISTS {}".format(tmp_view_name))


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


def _check_columns(observed, table_design):
    """
    Compare actual columns in query to those in table design object and return instructions for logger

    >>> _check_columns(['a', 'b'], dict(columns=[dict(name='b'), dict(name='a')])) # doctest: +ELLIPSIS
    'Column listing mismatch! Diff of observed vs expected follows:...'

    >>> _check_columns(['a', 'b'], dict(columns=[dict(name='a'), dict(name='b')]))

    """
    observed = list(observed)
    expected = [column["name"] for column in table_design["columns"] if not column.get('skipped', False)]

    # handle identity columns by inserting a column into observed in the expected position
    for index, column in enumerate(table_design['columns']):
        if column.get('identity', False):
            observed.insert(index, column['name'])

    if observed != expected:
        return 'Column listing mismatch! Diff of observed vs expected follows: {}'.format(
                 '\n'.join(difflib.context_diff(observed, expected)))


def validate_design_file_semantics(descriptions, keep_going=False):
    """
    Load local design files and validate them along the way against schemas and semantics.
    Return list for successfully validated descriptions if you want to keep going.
    Or raise exception on validation error.
    """
    logger = logging.getLogger(__name__)
    ok = []
    for description in descriptions:
        try:
            logger.info("Loading and validating file '%s'", description.design_file_name)
            table_design = description.table_design
            logger.debug("Validated table design for '%s'", table_design["name"])
            ok.append(description)
        except etl.design.TableDesignError:
            if not keep_going:
                raise
    return ok


def validate_dependency_ordering(table_descriptions):
    """
    Try to build a dependency order and report whether graph is possible
    """
    logger = logging.getLogger(__name__)
    logger.info("Validating dependency ordering")
    execution_order = order_by_dependencies(table_descriptions)
    max_ok = len(execution_order)
    has_missing_deps = [description for description in execution_order if description.order > max_ok]
    logger.warning("Relation(s) with missing dependencies: %s",
                   ', '.join("'{}'".format(d.identifier) for d in sorted(has_missing_deps)))


def validate_designs_using_views(dsn, table_descriptions, keep_going=False):
    """
    Iterate over all relations (CTAS or VIEW) to test how table design and query match up.
    """
    logger = logging.getLogger(__name__)
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in table_descriptions:
            try:
                validate_table_as_view(conn, description)
            except Exception:
                if keep_going:
                    logger.exception("Failed to run '{}' as view:".format(description.target_table_name))
                else:
                    raise


def validate_designs(dsn, file_sets, bucket_name=None, keep_going=False, skip_deps=False):
    """
    Make sure that all table design files pass the validation checks.

    If a bucket name is given, assume files are objects in that bucket.
    Otherwise they better be in the local filesystem.
    """
    logger = logging.getLogger(__name__)
    descriptions = [RelationDescription(file_set, bucket_name) for file_set in file_sets]
    valid_descriptions = validate_design_file_semantics(descriptions, keep_going=keep_going)
    validate_dependency_ordering(valid_descriptions)

    tables_to_validate_as_views = [description for description in valid_descriptions
                                   if description.is_ctas_relation or description.is_view_relation]
    if skip_deps:
        logger.info("Skipping validation against database")
    elif tables_to_validate_as_views:
        validate_designs_using_views(dsn, tables_to_validate_as_views, keep_going=keep_going)
    else:
        logger.info("Skipping validation against database (nothing to do)")


def test_queries(dsn, file_sets, bucket_name=None):
    """
    Test queries by running EXPLAIN with the query.

    If a bucket name is given, assume files are objects in that bucket.
    Otherwise they better be in the local filesystem.
    """
    logger = logging.getLogger(__name__)
    descriptions = [RelationDescription(file_set, bucket_name) for file_set in file_sets]

    # We can't use a read-only connection here because Redshift needs to (or wants to) create
    # temporary tables when building the query plan if temporary tables (probably from CTEs)
    # will be needed during query execution.  (Look for scans on volt_tt_* tables.)
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in descriptions:
            if description.is_ctas_relation or description.is_view_relation:
                logger.debug("Testing query for %s", description.identifier)
                plan = etl.pg.query(conn, "EXPLAIN\n" + description.query_stmt)
                logger.info("Explain plan for query of '%s':\n | %s",
                            description.identifier,
                            "\n | ".join(row[0] for row in plan))
