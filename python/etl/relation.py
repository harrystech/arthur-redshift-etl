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

import concurrent.futures
import logging
import os.path
from contextlib import closing, contextmanager
from functools import partial
from operator import attrgetter
from queue import PriorityQueue
from typing import Any, Dict, FrozenSet, Optional, Union, List

import etl.config
import etl.design.load
import etl.file_sets
import etl.pg
import etl.s3
import etl.timer
from etl.config.dw import DataWarehouseSchema
from etl.errors import CyclicDependencyError, MissingQueryError
from etl.names import join_with_quotes, TableName, TableSelector

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class RelationDescription:
    """
    Handy object for working with relations (tables or views) created with Arthur.
    Created from a collection of files that pertain to the same table.
    Offers helpful properties for lazily loading contents of relation files.
    Modified by other functions in relations module to set attributes related to dependency graph.
    """

    def __getattr__(self, attr):
        """
        Pass-through access to file set -- if the relation doesn't know about an attribute
        we'll pick up the attribute from the file set!
        """
        if hasattr(self._fileset, attr):
            return getattr(self._fileset, attr)
        raise AttributeError("Neither '%s' nor '%s' has attribute '%s'" % (self.__class__.__name__,
                                                                           self._fileset.__class__.__name__,
                                                                           attr))

    def __init__(self, discovered_files: etl.file_sets.TableFileSet) -> None:
        # Basic properties to locate files describing the relation
        self._fileset = discovered_files
        self.bucket_name = discovered_files.netloc if discovered_files.scheme == "s3" else None
        self.prefix = discovered_files.path
        # Note the subtle difference to TableFileSet--here the manifest_file_name is always present since it's computed
        self.manifest_file_name = os.path.join(self.prefix, "data", self.source_path_name + ".manifest")
        self.has_manifest = discovered_files.manifest_file_name is not None
        # Lazy-loading of table design and query statement and any derived information from the table design
        self._table_design = None  # type: Optional[Dict[str, Any]]
        self._query_stmt = None  # type: Optional[str]
        self._dependencies = None  # type: Optional[FrozenSet[str]]
        self._is_required = None  # type: Union[None, bool]

    @property
    def identifier(self) -> str:
        return self.target_table_name.identifier

    def __str__(self):
        return str(self.target_table_name)

    def __repr__(self):
        return "{}({}:{})".format(self.__class__.__name__, self.identifier, self.source_path_name)

    def load(self) -> None:
        """
        Force a loading of the table design (which is normally loaded "on demand").

        Strictly speaking, this isn't thread-safe.  But if you worry about thread safety here, rethink your code.
        """
        if self._table_design is None:
            if self.bucket_name:
                loader = partial(etl.design.load.load_table_design_from_s3, self.bucket_name)
            else:
                loader = partial(etl.design.load.load_table_design_from_localfile)
            self._table_design = loader(self.design_file_name, self.target_table_name)

    @staticmethod
    def load_in_parallel(relations: List["RelationDescription"]) -> None:
        """
        Load all relations' table design file in parallel.
        """
        logger.info("Loading table design for %d relation(s)", len(relations))
        with etl.timer.Timer() as timer:
            # TODO With Python 3.6, we should pass in a thread_name_prefix
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                executor.map(lambda relation: relation.load(), relations)
        logger.info("Finished loading %d table design file(s) (%s)", len(relations), timer)

    @property  # This property is lazily loaded
    def table_design(self) -> Dict[str, Any]:
        self.load()
        return self._table_design  # type: ignore

    @property
    def kind(self) -> str:
        if self.table_design["source_name"] in ("CTAS", "VIEW"):
            return self.table_design["source_name"]
        else:
            return "DATA"

    @property
    def is_ctas_relation(self) -> bool:
        return self.kind == "CTAS"

    @property
    def is_view_relation(self) -> bool:
        return self.kind == "VIEW"

    @property
    def is_transformation(self) -> bool:
        return self.kind != "DATA"

    @property
    def is_unloadable(self) -> bool:
        return "unload_target" in self.table_design

    @property
    def is_required(self) -> bool:
        if self._is_required is None:
            raise RuntimeError("State of 'is_required' unknown for RelationDescription '{0.identifier}'".format(self))
        return self._is_required

    @property
    def unload_target(self):
        return self.table_design.get("unload_target")

    @property
    def query_stmt(self) -> str:
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
        return str(self._query_stmt)  # The str(...) shuts up the type checker.

    @property
    def dependencies(self) -> FrozenSet[str]:
        if self._dependencies is None:
            self._dependencies = frozenset(self.table_design.get("depends_on", []))
        return self._dependencies

    @property
    def unquoted_columns(self) -> List[str]:
        """
        List of the column names of this relation
        """
        return [column["name"] for column in self.table_design["columns"] if not column.get("skipped")]

    @property
    def columns(self) -> List[str]:
        """
        List of delimited column names of this relation
        """
        return ['"{}"'.format(column) for column in self.unquoted_columns]

    @property
    def has_identity_column(self) -> bool:
        """
        Return whether any of the columns is marked as identity column.
        (Should only ever be possible for CTAS, see validation code).
        """
        return any(column.get("identity") for column in self.table_design["columns"])

    # FIXME This is a bit confusingly named.
    @property
    def source_name(self):
        return self.target_table_name.schema

    @property
    def dw_schema(self) -> DataWarehouseSchema:
        dw_config = etl.config.get_dw_config()
        return dw_config.schema_lookup(self.source_name)

    @classmethod
    def from_file_sets(cls, file_sets, required_relation_selector=None) -> List["RelationDescription"]:
        """
        Return a list of relation descriptions based on a list of file sets.

        If there's a file set without a table design file, then there's a warning and that file set
        is skipped.  (This comes in handy when creating the design file for a CTAS or VIEW automatically.)

        If provided, the required_relation_selector will be used to mark dependencies of high-priority.  A failure
        to dump or load in these relations will end the ETL run.
        """
        relations = []
        for file_set in file_sets:
            if file_set.design_file_name is not None:
                relations.append(cls(file_set))
            else:
                logger.warning("Found file(s) without matching table design: %s",
                               join_with_quotes(file_set.files))

        if required_relation_selector:
            set_required_relations(relations, required_relation_selector)

        return relations

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

    def find_partition_key(self) -> Union[str, None]:
        """
        Return valid partition key for a relation which fulfills the conditions that
        (1) the column is marked as a primary key
        (2) the table's primary key is a single column
        (3) the column has a numeric type.

        If the table design provides extract_settings with a split_by column, provide that instead.

        If no partition key can be found, returns None.
        """
        constraints = self.table_design.get("constraints", [])
        extract_settings = self.table_design.get("extract_settings", {})
        [split_by_key] = extract_settings.get('split_by', [None])

        try:
            [primary_key] = [col for constraint in constraints for col in constraint.get("primary_key", [])]
            partition_key = split_by_key or primary_key
        except ValueError:
            logger.debug("Found no single-column primary key for table '%s'", self.identifier)
            partition_key = split_by_key

        if not partition_key:
            logger.debug("Found no partition key for table '%s'", self.identifier)
            return None

        for column in self.table_design["columns"]:
            if column["name"] == partition_key:
                # We check here the "generic" type which abstracts the SQL types like smallint, int4, bigint, ...
                if column["type"] in ("int", "long"):
                    logger.debug("Partition key for table '%s' is '%s'", self.identifier, partition_key)
                    return partition_key
                logger.warning("Partition key '%s' is not a number and is not usable as a partition key for '%s'",
                               partition_key, self.identifier)
                break

        return None

    @contextmanager
    def matching_temporary_view(self, conn):
        """
        Create a temporary view (with a name loosely based around the reference passed in).

        We look up which temp schema the view landed in so that we can use TableName.
        """
        # Redshift seems to cut off identifier so we might as well not pass in something longer than 127.
        view_identifier = "#{0.schema}${0.table}".format(self.target_table_name)[:127]

        with etl.pg.log_error():
            ddl_stmt = """CREATE OR REPLACE VIEW "{}" AS\n{}""".format(view_identifier, self.query_stmt)
            logger.info("Creating view '%s' to match relation '%s'", view_identifier, self.identifier)
            etl.pg.execute(conn, ddl_stmt)

            lookup_stmt = """
                SELECT nsp.nspname AS "schema"
                     , cls.relname AS "table"
                  FROM pg_catalog.pg_class cls
                  JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                 WHERE cls.relname = %s
                   AND cls.relkind = 'v'
                """
            row = etl.pg.query(conn, lookup_stmt, (view_identifier,))[0]
            view_name = TableName(**row)

            try:
                yield view_name
            finally:
                etl.pg.execute(conn, "DROP VIEW {}".format(view_identifier))


class SortableRelationDescription:
    """
    Add decoration around relation descriptions so that we can easily
    compute the execution order and then throw away our intermediate results.
    """

    def __init__(self, original_description: RelationDescription) -> None:
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
    RelationDescription.load_in_parallel(relation_descriptions)
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
        logger.warning("These relations were unknown during dependency ordering: %s",
                       join_with_quotes(known_unknowns))
        logger.warning('This caused these relations to have dependencies that are not known: %s',
                       join_with_quotes(has_unknown_dependencies))
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


def set_required_relations(relations: List[RelationDescription], required_selector: TableSelector) -> None:
    """
    Set the required property of the relations if they are directly or indirectly feeding
    into relations selected by the :required_selector.
    """
    ordered_descriptions = order_by_dependencies(relations)
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

    logger.info("Marked %d relation(s) as required based on selector: %s", len(required_relations), required_selector)


def find_matches(relations: List[RelationDescription], selector: TableSelector):
    """
    Return list of matching relations.
    """
    return [relation for relation in relations if selector.match(relation.target_table_name)]


def find_dependents(relations: List[RelationDescription], seed_relations: List[RelationDescription]):
    """
    Return list of relations that depend on the seed relations (directly or transitively).
    For this to really work, the list of relations should be sorted in "execution order"!
    """
    seeds = frozenset(relation.identifier for relation in seed_relations)
    in_dependency_path = set(seeds)
    for relation in relations:
        if any(dependency in in_dependency_path for dependency in relation.dependencies):
            in_dependency_path.add(relation.identifier)
    dependents = in_dependency_path - seeds
    return [relation for relation in relations if relation.identifier in dependents]
