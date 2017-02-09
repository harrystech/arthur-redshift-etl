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
import os.path
from queue import PriorityQueue
from typing import Any, Dict, List, Union

import psycopg2
import simplejson as json

import etl
import etl.design
import etl.pg
import etl.file_sets
import etl.s3


class MissingQueryError(etl.ETLError):
    pass


class CyclicDependencyError(etl.ETLError):
    pass


class UniqueConstraintError(etl.ETLError):
    def __init__(self, relation, constraint, keys, examples):
        self.relation = relation
        self.constraint = constraint
        self.keys = keys
        self.example_string = ', '.join(map(str, examples))

    def __str__(self):
        return ("Relation {r} violates {c} constraint: "
                "Example duplicate values of {k} are: {e}".format(
                    r=self.relation, c=self.constraint,
                    k=self.keys,
                    e=self.example_string)
                )


class RelationDescription:
    """
    Handy object for working with relations (tables or views) created with Arthur.
    Created from a collection of files that pertain to the same table.
    Offers helpful properties for lazily loading contents of relation files.
    Modified by other functions in relations module to set attributes related to dependency graph.
    """

    def __getattr__(self, attr):
        """
        Pass-through access to file set
        """
        if hasattr(self._fileset, attr):
            return getattr(self._fileset, attr)
        raise AttributeError("Neither '%s' nor '%s' has attribute '%s'" % (self.__class__.__name__,
                                                                           self._fileset.__class__.__name__,
                                                                           attr))

    def __init__(self, discovered_files: etl.file_sets.TableFileSet):
        # Basic properties to locate files describing the relation
        self._fileset = discovered_files
        self.bucket_name = discovered_files.netloc if discovered_files.scheme == "s3" else None
        self.prefix = discovered_files.path
        # Note the subtle different to TableFileSet -- here the manifest_file_name is always present since it's computed
        self.manifest_file_name = os.path.join(self.prefix, "data", self.source_path_name + ".manifest")
        self.has_manifest = discovered_files.manifest_file_name is not None
        # Lazy-loading of table design, query statement, etc.
        self._table_design = None
        self._query_stmt = None
        self._unload_target = None
        self._dependencies = None
        # Deferred evaluation whether this relation is required
        self._is_required = None

    @property
    def identifier(self):
        return self.target_table_name.identifier

    def __str__(self):
        return str(self.target_table_name)

    def __repr__(self):
        return "{}({}:{})".format(self.__class__.__name__, self.identifier, self.source_path_name)

    @property
    def table_design(self) -> Dict[str, Any]:
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
    def is_unloadable(self):
        return "unload_target" in self.table_design

    @property
    def is_required(self):
        if self._is_required is None:
            raise RuntimeError("State of 'is_required' for RelationDescription '{}' is unknown".format(self.identifier))
        return self._is_required

    @property
    def unload_target(self):
        return self.table_design.get("unload_target")

    @property
    def query_stmt(self):
        if self._query_stmt is None:
            if self.sql_file_name is None:
                raise MissingQueryError("Missing SQL file for '{}'".format(self.identifier))
            if self.bucket_name:
                with closing(etl.s3.get_s3_object_content(self.bucket_name, self.sql_file_name)) as content:
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

    @property
    def unquoted_columns(self):
        """
        List of the column names of this relation
        """
        return [column["name"] for column in self.table_design["columns"] if not column.get("skipped")]

    @property
    def columns(self):
        """
        List of delimited column names of this relation
        """
        return ['"{}"'.format(column) for column in self.unquoted_columns]

    @property
    def source_name(self):
        return self.target_table_name.schema

    @classmethod
    def from_file_sets(cls, file_sets, required_relation_selector=None):
        """
        Return a list of relation descriptions based on a list of file sets.

        If there's a file set without a table design file, then there's a warning and that file set
        is skipped.  (This comes in handy when creating the design file for a CTAS or VIEW automatically.)

        If provided, the required_relation_selector will be used to mark dependencies of high-priority.  A failure
        to dump or load in these relations will end the ETL run.
        """
        logger = logging.getLogger(__name__)
        descriptions = []
        for file_set in file_sets:
            if file_set.design_file_name is not None:
                descriptions.append(cls(file_set))
            else:
                logger.warning("Found file(s) without matching table design: %s",
                               etl.join_with_quotes(file_set.files))

        if required_relation_selector:
            set_required_relations(descriptions, required_relation_selector)

        return descriptions

    def get_columns_with_casts(self) -> List[str]:
        """
        Pick columns and decide how they are selected (as-is or with an expression).

        Whether there's an expression or just a name the resulting column is always
        called out delimited.
        """
        selected_columns = []
        for column in self.table_design["columns"]:
            if not column.get("skipped", False):
                if column.get("expression"):
                    selected_columns.append('{expression} AS "{name}"'.format(**column))
                else:
                    selected_columns.append('"{name}"'.format(**column))
        return selected_columns

    def find_primary_key(self) -> Union[str, None]:
        """
        Return primary key (single column) from the table design, if defined, else None.
        """
        if "primary_key" in self.table_design.get("constraints", {}):
            # Note that column constraints such as primary key are stored as one-element lists, hence:
            return self.table_design["constraints"]["primary_key"][0]
        else:
            return None


class SortableRelationDescription:
    """
    Add decoration around relation descriptions so that we can easily
    compute the execution order and then throw away our intermediate results.
    """

    def __init__(self, original_description: RelationDescription):
        self.original_description = original_description
        self.identifier = original_description.identifier
        self.dependencies = set(original_description.dependencies)
        self.order = None


def order_by_dependencies(relation_descriptions):
    """
    Sort the relations such that any dependents surely are loaded afterwards.

    If a table (or view) depends on other tables, then its order is larger
    than any of its dependencies. Ties are resolved based on the initial order
    of the tables. (This motivates the use of a priority queue.)

    If a table depends on some system catalogs (living in pg_catalog), then the table
    is treated as if it depended on all other tables.

    Provides warnings about:
        * relations that directly depend on relations not in the input
        * relations that are depended upon but are not in the input
    """
    logger = logging.getLogger(__name__)

    descriptions = [SortableRelationDescription(description) for description in relation_descriptions]
    known_tables = frozenset({description.identifier for description in descriptions})
    nr_tables = len(known_tables)

    # Phase 1 -- build up the priority queue all the while making sure we have only dependencies that we know about
    has_unknown_dependencies = set()
    has_internal_dependencies = set()
    known_unknowns = set()
    queue = PriorityQueue()
    for initial_order, description in enumerate(descriptions):
        pg_internal_dependencies = set(d for d in description.dependencies if d.startswith('pg_catalog'))
        unknowns = description.dependencies - known_tables - pg_internal_dependencies
        if unknowns:
            known_unknowns.update(unknowns)
            has_unknown_dependencies.add(description.identifier)
            # Drop the unknowns from the list of dependencies so that the loop below doesn't wait for their resolution.
            description.dependencies = description.dependencies.difference(unknowns)
        if pg_internal_dependencies:
            description.dependencies = description.dependencies.difference(pg_internal_dependencies)
            has_internal_dependencies.add(description.identifier)
        queue.put((1, initial_order, description))
    if has_unknown_dependencies:
        # TODO In a "strict" or "pedantic" mode, if known_unkowns is not an empty set, this should error out.
        logger.warning('These relations have unknown dependencies: %s', etl.join_with_quotes(has_unknown_dependencies))
        logger.warning("These relations were unknown during dependency ordering: %s",
                       etl.join_with_quotes(known_unknowns))
    has_no_internal_dependencies = known_tables - known_unknowns - has_internal_dependencies
    for description in descriptions:
        if description.identifier in has_internal_dependencies:
            description.dependencies.update(has_no_internal_dependencies)

    # Phase 2 -- keep looping until all relations have their dependencies ordered before them
    table_map = {description.identifier: description for description in descriptions}
    latest = 0
    while not queue.empty():
        minimum, tie_breaker, description = queue.get()
        if minimum > 2 * nr_tables:
            raise CyclicDependencyError("Cannot determine order, suspect cycle in DAG of dependencies")
        others = [table_map[dep].order for dep in description.dependencies]
        if not others:
            latest = description.order = latest + 1
        elif all(others):
            latest = description.order = max(max(others), latest) + 1
        elif any(others):
            at_least = max(order for order in others if order is not None)
            queue.put((max(at_least, latest, minimum) + 1, tie_breaker, description))
        else:
            queue.put((max(latest, minimum) + 1, tie_breaker, description))

    return [description.original_description for description in sorted(descriptions, key=attrgetter("order"))]


def set_required_relations(descriptions, required_selector) -> None:
    """
    Set the required property of the descriptions if they are directly or indirectly feeding
    into relations selected by the :required_selector.
    """
    ordered_descriptions = order_by_dependencies(descriptions)
    # Start with all descriptions that are matching the required selector
    required_relations = [d for d in ordered_descriptions if required_selector.match(d.target_table_name)]
    # Walk through descriptions in reverse dependency order, expanding required set based on dependency fan-out
    for description in ordered_descriptions[::-1]:
        if any([description.identifier in required.dependencies for required in required_relations]):
            required_relations.append(description)

    for relation in ordered_descriptions:
        relation._is_required = False
    for relation in required_relations:
        relation._is_required = True


def validate_table_as_view(conn, description, keep_going=False):
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
        if keep_going:
            logger.warning(comparison_output)
        else:
            raise etl.design.TableDesignError(comparison_output)
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
        if keep_going:
            logger.warning(comparison_output)
        else:
            raise etl.design.TableDesignError(comparison_output)
    else:
        logger.info('Column listing in design file matches column listing in SQL')

    logger.info("Dropping view '%s'", tmp_view_name.identifier)
    etl.pg.execute(conn, "DROP VIEW IF EXISTS {}".format(tmp_view_name))


def validate_constraints(conn, description, dry_run=False, only_warn=False):
    """
    Raises a UniqueConstraintError if :description's target table doesn't obey unique constraints declared in its design
    Returns None
    """
    logger = logging.getLogger(__name__)
    design = description.table_design
    if 'constraints' not in design:
        logger.info("No constraints discovered for '%s'", description.identifier)
        return

    statement_template = """
        SELECT {cols}
        FROM {table}
        GROUP BY {cols}
        HAVING COUNT(*) > 1
        LIMIT 5
    """

    constraints = design['constraints']
    for constraint in ["primary_key", "natural_key", "surrogate_key", "unique"]:
        if constraint in constraints:
            logger.info("Checking %s constraint on '%s'", constraint, description.identifier)
            columns = constraints[constraint]
            quoted_columns = ", ".join('"{}"'.format(name) for name in columns)
            statement = statement_template.format(cols=quoted_columns, table=description.target_table_name)
            if dry_run:
                logger.info('Dry-run: Skipping duplicate row query, checking explain plan instead')
                etl.pg.execute(conn, "EXPLAIN\n" + statement)
                continue
            results = etl.pg.query(conn, statement)
            if results:
                error = UniqueConstraintError(description, constraint, columns, results)
                if only_warn:
                    logger.warning(error)
                else:
                    raise error


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
            if description.table_design:
                ok.append(description)
        except etl.design.TableDesignError:
            if not keep_going:
                raise
    return ok


def validate_designs_using_views(dsn, table_descriptions, keep_going=False):
    """
    Iterate over all relations (CTAS or VIEW) to test how table design and query match up.
    """
    logger = logging.getLogger(__name__)
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in table_descriptions:
            try:
                with etl.pg.log_error():
                    validate_table_as_view(conn, description, keep_going=keep_going)
            except (etl.ETLError, psycopg2.Error):
                if keep_going:
                    logger.exception("Ignoring failure to create '%s' and proceeding as requested:",
                                     description.target_table_name)
                else:
                    raise


def validate_designs(dsn: dict, descriptions: List[RelationDescription], keep_going=False, skip_deps=False) -> None:
    """
    Make sure that all table design files pass the validation checks.
    """
    logger = logging.getLogger(__name__)

    valid_descriptions = validate_design_file_semantics(descriptions, keep_going=keep_going)

    logger.info("Validating dependency ordering")
    order_by_dependencies(valid_descriptions)

    tables_to_validate_as_views = [description for description in valid_descriptions
                                   if description.is_ctas_relation or description.is_view_relation]
    if skip_deps:
        logger.info("Skipping validation against database")
    elif tables_to_validate_as_views:
        validate_designs_using_views(dsn, tables_to_validate_as_views, keep_going=keep_going)
    else:
        logger.info("Skipping validation against database (nothing to do)")


def test_queries(dsn: dict, descriptions: List[RelationDescription]) -> None:
    """
    Test queries by running EXPLAIN with the query.
    """
    logger = logging.getLogger(__name__)

    # We can't use a read-only connection here because Redshift needs to (or wants to) create
    # temporary tables when building the query plan if temporary tables (probably from CTEs)
    # will be needed during query execution.  (Look for scans on volt_tt_* tables.)
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in descriptions:
            if description.is_ctas_relation or description.is_view_relation:
                logger.debug("Testing query for '%s'", description.identifier)
                plan = etl.pg.query(conn, "EXPLAIN\n" + description.query_stmt)
                logger.info("Explain plan for query of '%s':\n | %s",
                            description.identifier,
                            "\n | ".join(row[0] for row in plan))


def copy_to_s3(descriptions: List[RelationDescription], bucket_name: str, prefix: str, dry_run: bool=False) -> None:
    """
    Copy (validated) table design and SQL files from local directory to S3 bucket.

    Essentially "publishes" data-warehouse code.
    """
    logger = logging.getLogger(__name__)

    for description in descriptions:
        files = [description.design_file_name]
        if description.is_ctas_relation or description.is_view_relation:
            if description.sql_file_name:
                files.append(description.sql_file_name)
            else:
                raise MissingQueryError("Missing matching SQL file for '%s'" % description.design_file_name)

        for local_filename in files:
            object_key = os.path.join(prefix, description.norm_path(local_filename))
            if dry_run:
                logger.info("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", local_filename, bucket_name, object_key)
            else:
                etl.s3.upload_to_s3(local_filename, bucket_name, object_key)
    if not dry_run:
        logger.info("Uploaded %d file(s) to 's3://%s/%s/'", len(descriptions), bucket_name, prefix)
