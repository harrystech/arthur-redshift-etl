"""
Work with relations: tables, CTAS tables, or views.

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

When called as a module, dumps the current configuration of table designs.
"""

import codecs
import concurrent.futures
import logging
import os.path
from contextlib import closing, contextmanager
from copy import deepcopy
from functools import partial
from itertools import chain, dropwhile
from operator import attrgetter
from queue import PriorityQueue
from typing import Any, Dict, FrozenSet, List, Optional, Tuple, Union

import funcy as fy

import etl.config
import etl.db
import etl.design.load
import etl.file_sets
import etl.s3
import etl.timer
from etl.config.dw import DataWarehouseSchema
from etl.errors import CyclicDependencyError, ETLRuntimeError, MissingQueryError
from etl.names import TableName, TableSelector, TempTableName
from etl.text import join_with_quotes

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
        Pass-through access to file set underlying the relation description.

        If the relation doesn't know about an attribute we'll pick up the attribute from
        the file set.
        """
        if hasattr(self._fileset, attr):
            return getattr(self._fileset, attr)
        raise AttributeError(
            "Neither '%s' nor '%s' has attribute '%s'"
            % (self.__class__.__name__, self._fileset.__class__.__name__, attr)
        )

    def __init__(self, discovered_files: etl.file_sets.RelationFileSet) -> None:
        # Basic properties to locate files describing the relation
        self._fileset = discovered_files
        if discovered_files.scheme == "s3":
            self.bucket_name = discovered_files.netloc
            self.prefix = discovered_files.path
        else:
            self.bucket_name = None
            self.prefix = None
        # Note the subtle difference to RelationFileSet: the manifest_file_name is always present
        # since it's computed.
        self.manifest_file_name = os.path.join(discovered_files.path or "", "data", self.source_path_name + ".manifest")
        # Lazy-loading of table design and query statement and any derived information from the
        # table design.
        self._table_design: Optional[Dict[str, Any]] = None
        self._query_stmt: Optional[str] = None

        self._dependencies: Optional[FrozenSet[TableName]] = None
        self._execution_level: Optional[int] = None
        self._execution_order: Optional[int] = None
        self._is_required: Optional[bool] = None

    @property
    def target_table_name(self) -> TableName:
        return self._fileset.target_table_name

    @property
    def has_manifest(self):
        return etl.s3.get_s3_object_last_modified(self.bucket_name, self.manifest_file_name, wait=False) is not None

    @property
    def identifier(self) -> str:
        return self.target_table_name.identifier

    def __str__(self):
        return str(self.target_table_name)

    def __repr__(self):
        return "{}({}:{})".format(self.__class__.__name__, self.identifier, self.source_path_name)

    def __format__(self, code):
        r"""
        Format target table as delimited identifier (with quotes) or just as an identifier.

        With the default or ':s', it's a delimited identifier with quotes.
        With ':x", the name is left bare but single quotes are around it.

        >>> fs = etl.file_sets.RelationFileSet(TableName("a", "b"), TableName("c", "b"), None)
        >>> relation = RelationDescription(fs)
        >>> "As delimited identifier: {:s}, as loggable string: {:x}".format(relation, relation)
        'As delimited identifier: "c"."b", as loggable string: \'c.b\''
        """
        if (not code) or (code == "s"):
            return str(self.target_table_name)
        elif code == "x":
            return "{:x}".format(self.target_table_name)
        else:
            raise ValueError("unsupported format code '{}' passed to RelationDescription".format(code))

    def load(self) -> None:
        """
        Force a loading of the table design (which is normally loaded "on demand").

        Strictly speaking, this isn't thread-safe. But if you worry about thread safety here,
        rethink your code.
        """
        if self._table_design is None:
            if self.bucket_name:
                loader = partial(etl.design.load.load_table_design_from_s3, self.bucket_name)
            else:
                loader = partial(etl.design.load.load_table_design_from_localfile)
            self._table_design = loader(self.design_file_name, self.target_table_name)

    @staticmethod
    def load_in_parallel(relations: List["RelationDescription"]) -> None:
        """Load all relations' table design file in parallel."""
        max_workers = 8
        logger.debug("Starting parallel load of %d table design file(s) on %d workers.", len(relations), max_workers)
        with etl.timer.Timer() as timer:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="load-parallel"
            ) as executor:
                executor.map(lambda relation: relation.load(), relations)
        logger.info("Finished loading %d table design file(s) (%s)", len(relations), timer)

    @property  # This property is lazily loaded
    def table_design(self) -> Dict[str, Any]:
        self.load()
        return deepcopy(self._table_design)  # type: ignore

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
    def execution_level(self) -> int:
        """All relations of the same level may be loaded in parallel."""
        if self._execution_level is None:
            raise ETLRuntimeError("execution level unknown for RelationDescription '{0.identifier}'".format(self))
        return self._execution_level

    @property
    def execution_order(self) -> int:
        """All relations can be ordered to load properly in series based on their dependencies."""
        if self._execution_order is None:
            raise ETLRuntimeError("execution order unknown for RelationDescription '{0.identifier}'".format(self))
        return self._execution_order

    @property
    def is_required(self) -> bool:
        if self._is_required is None:
            raise ETLRuntimeError(
                "state of 'is_required' unknown for RelationDescription '{0.identifier}'".format(self)
            )
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
                with codecs.open(self.sql_file_name, encoding="utf-8") as f:
                    query_stmt = f.read()

            self._query_stmt = query_stmt.strip().rstrip(";")
        return str(self._query_stmt)  # The str(...) shuts up the type checker.

    @property
    def dependencies(self) -> FrozenSet[TableName]:
        if self._dependencies is None:
            dependent_table_names = [TableName.from_identifier(d) for d in self.table_design.get("depends_on", [])]
            self._dependencies = frozenset(dependent_table_names)
        return self._dependencies

    @property
    def source_name(self):
        return self.target_table_name.schema

    @property
    def schema_config(self) -> DataWarehouseSchema:
        dw_config = etl.config.get_dw_config()
        return dw_config.schema_lookup(self.source_name)

    def data_directory(self, from_prefix=None):
        """Full path to data files (in the schema's data format)."""
        # TODO(tom): Split between source data and static data
        # Either somewhere in S3 for static sources, in S3 for extracted sources or locally.
        return os.path.join(
            from_prefix or self.prefix or ".",
            "data",
            self.source_path_name,
            (self.schema_config.s3_data_format.format or "CSV").lower(),
        )

    @property
    def unquoted_columns(self) -> List[str]:
        """List of the column names of this relation."""
        return [column["name"] for column in self.table_design["columns"] if not column.get("skipped")]

    @property
    def columns(self) -> List[str]:
        """List of delimited column names of this relation."""
        return ['"{}"'.format(column) for column in self.unquoted_columns]

    @property
    def has_identity_column(self) -> bool:
        """
        Return whether any of the columns is marked as identity column.

        (Should only ever be possible for CTAS, see validation code).
        """
        return any(column.get("identity") for column in self.table_design["columns"])

    @property
    def is_missing_encoding(self) -> bool:
        """Return whether any column doesn't have encoding specified."""
        return any(not column.get("encoding") for column in self.table_design["columns"] if not column.get("skipped"))

    @classmethod
    def from_file_sets(cls, file_sets, required_relation_selector=None) -> List["RelationDescription"]:
        """
        Return a list of relation descriptions based on a list of file sets.

        If there's a file set without a table design file, then there's a warning and that file set
        is skipped. (This comes in handy when creating the design file for a CTAS or VIEW
        automatically.)

        If provided, the required_relation_selector will be used to mark dependencies of
        high-priority. A failure to dump or load in these relations will end the ETL run.
        """
        relations = []
        for file_set in file_sets:
            if file_set.design_file_name is not None:
                relations.append(cls(file_set))
            else:
                logger.warning("Found file(s) without matching table design: %s", join_with_quotes(file_set.files))

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

    def get_columns_with_types(self) -> List[Dict[str, str]]:
        """Return list of dicts that describe non-skipped columns with their name and type."""
        return [
            fy.project(column, ["name", "type"]) for column in self.table_design["columns"] if not column.get("skipped")
        ]

    @property
    def num_partitions(self):
        return self.table_design.get("extract_settings", {}).get("num_partitions")

    def find_partition_key(self) -> Union[str, None]:
        """
        Return valid partition key for a relation.

        The partition key will fulfill these conditions:
        (1) the column is marked as a primary key
        (2) the table's primary key is a single column
        (3) the column has a numeric type or can be cast into one (which currently only works for
            timestamps).

        If the table design provides extract_settings with a split_by column setting, provide that
        instead. The column will be numeric (int or long) or a timestamp in this case.

        If no partition key can be found, returns None.
        """
        constraints = self.table_design.get("constraints", [])
        extract_settings = self.table_design.get("extract_settings", {})
        [partition_key] = extract_settings.get("split_by", [None])

        if not partition_key:
            try:
                # Unpacking will fail here if the list of primary keys hasn't exactly one element.
                [primary_key] = [col for constraint in constraints for col in constraint.get("primary_key", [])]
                partition_key = primary_key
            except ValueError:
                logger.debug("Found no single-column primary key for table '%s'", self.identifier)

        if not partition_key:
            logger.debug("Found no partition key for table '%s'", self.identifier)
            return None

        column = fy.first(fy.where(self.table_design["columns"], name=partition_key))

        # We check here the "generic" type which abstracts the SQL types like smallint, int4, etc.
        if column["type"] in ("int", "long", "date", "timestamp"):
            logger.debug("Partition key for table '%s' is '%s'", self.identifier, partition_key)
            return partition_key

        logger.warning(
            "Column '%s' is not int, long, date or timestamp so is not usable as a partition key for '%s'",
            partition_key,
            self.identifier,
        )
        return None

    @contextmanager
    def matching_temporary_view(self, conn, assume_external_schema=False):
        """
        Create a temporary view (with a name loosely based around the reference passed in).

        We look up which temp schema the view landed in so that we can use TableName.
        """
        temp_view = TempTableName.for_table(self.target_table_name)

        with etl.db.log_error():
            ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(temp_view, self.query_stmt)
            if assume_external_schema or any(dep.is_external for dep in self.dependencies):
                ddl_stmt += "\nWITH NO SCHEMA BINDING"
                temp_view.is_late_binding_view = True

            logger.info("Creating view '%s' to match relation '%s'", temp_view.identifier, self.identifier)
            etl.db.execute(conn, ddl_stmt)

            try:
                yield temp_view
            finally:
                etl.db.execute(conn, "DROP VIEW {}".format(temp_view))


class SortableRelationDescription:
    """
    Facade to add modifiable list of dependencies.

    This adds decoration around relation descriptions so that we can easily
    compute the execution order by updating the "dependencies" list.
    """

    def __init__(self, original_description: RelationDescription) -> None:
        self.original_description = original_description
        self.identifier = original_description.identifier
        self.dependencies = set(original_description.dependencies)
        self.target_table_name = original_description.target_table_name
        self.order: Optional[int] = None
        self.level: Optional[int] = None


def _sanitize_dependencies(descriptions: List[SortableRelationDescription]) -> None:
    """
    Pass 1 of ordering -- make sure to drop unknown dependencies.

    This will change the sortable relations in place.
    """
    known_tables = frozenset({description.target_table_name for description in descriptions})
    has_unknown_dependencies = set()
    has_pg_catalog_dependencies = set()
    known_unknowns = set()

    for initial_order, description in enumerate(descriptions):
        unmanaged_dependencies = frozenset(dep for dep in description.dependencies if not dep.is_managed)
        pg_catalog_dependencies = frozenset(dep for dep in description.dependencies if dep.schema == "pg_catalog")
        unknowns = description.dependencies - known_tables - unmanaged_dependencies
        if unknowns:
            known_unknowns.update(unknowns)
            has_unknown_dependencies.add(description.target_table_name)
            # Drop the unknowns from the list of dependencies so that the loop below doesn't wait
            # for their resolution.
            description.dependencies = description.dependencies.difference(unknowns)
        if unmanaged_dependencies:
            logger.info(
                "The following dependencies for relation '%s' are not managed by Arthur: %s",
                description.identifier,
                join_with_quotes([dep.identifier for dep in unmanaged_dependencies]),
            )
        if pg_catalog_dependencies:
            has_pg_catalog_dependencies.add(description.target_table_name)

    if has_unknown_dependencies:
        logger.warning(
            "These relations were unknown during dependency ordering: %s",
            join_with_quotes([dep.identifier for dep in known_unknowns]),
        )
        logger.warning(
            "This caused these relations to have dependencies that are not known: %s",
            join_with_quotes([dep.identifier for dep in has_unknown_dependencies]),
        )

    # Make tables that depend on tables in pg_catalog depend on all our tables (except those
    # depending on pg_catalog tables).
    has_no_internal_dependencies = known_tables - known_unknowns - has_pg_catalog_dependencies
    for description in descriptions:
        if description.target_table_name in has_pg_catalog_dependencies:
            description.dependencies.update(has_no_internal_dependencies)


def _sort_by_dependencies(descriptions: List[SortableRelationDescription]) -> None:
    """
    Pass 2 of ordering -- sort such that dependencies are built before the relation itself is built.

    This will change the sortable relation in place.
    """
    # The queue has tuples of (min expected order number, original sort order to break ties,
    # relation description).
    queue: PriorityQueue[Tuple[int, int, SortableRelationDescription]] = PriorityQueue()
    for initial_order, description in enumerate(descriptions):
        queue.put((1, initial_order + 1, description))

    relation_map = {description.target_table_name: description for description in descriptions}
    nr_relations = len(descriptions)
    latest_order = 0
    while not queue.empty():
        minimum_order, tie_breaker, description = queue.get()

        if minimum_order > nr_relations:
            raise CyclicDependencyError("Cannot determine order, suspect cycle in DAG of dependencies")

        if all(relation_map[dep].order is not None for dep in description.dependencies if dep.is_managed):
            # Relation has no dependencies (all([]) == True) or has all its dependencies evaluated.
            latest_order += 1
            description.order = latest_order

            max_preceding_level = max(
                (relation_map[dep].level or 0 for dep in description.dependencies if dep.is_managed), default=0
            )
            description.level = max_preceding_level + 1

        else:
            # "Latest" order is only smaller than the "minimum" during the first run through.
            queue.put((max(latest_order, minimum_order) + 1, tie_breaker, description))


def order_by_dependencies(relation_descriptions: List[RelationDescription]) -> List[RelationDescription]:
    """
    Sort the relations such that any dependents surely are loaded afterwards.

    If a table (or view) depends on other tables, then its order is larger
    than any of its managed dependencies. Ties are resolved based on the initial order
    of the tables.

    If a table depends on some system catalogs (living in pg_catalog), then the table
    is treated as if it depended on all other tables.

    Provides warnings about:
        * relations that directly depend on relations not in the input
        * relations that are depended upon but are not in the input

    Side-effect: the order and level of the relations is set.
    (So should this be invoked a second time, we'll skip computations if order is already set.)
    """
    RelationDescription.load_in_parallel(relation_descriptions)

    # Sorting is all-or-nothing so we get away with checking for just one relation's order.
    if relation_descriptions and relation_descriptions[0]._execution_order is not None:
        logger.info("Reusing previously computed execution order of %d relation(s)", len(relation_descriptions))
    else:
        sortable_descriptions = [SortableRelationDescription(description) for description in relation_descriptions]
        _sanitize_dependencies(sortable_descriptions)
        _sort_by_dependencies(sortable_descriptions)
        # A functional approach would be to create new instances here. Instead we reach back. Shrug.
        # TODO Sort by level first, then order.
        for sortable in sortable_descriptions:
            sortable.original_description._execution_order = sortable.order
            sortable.original_description._execution_level = sortable.level

    return [description for description in sorted(relation_descriptions, key=attrgetter("execution_order"))]


def set_required_relations(relations: List[RelationDescription], required_selector: TableSelector) -> None:
    """
    Set the "required" property based on the selector.

    The "required" property of the relations is set if they are directly or indirectly feeding
    into relations selected by the :required_selector.

    Side-effect: relations are sorted to determine dependencies and their order and level is set.
    """
    logger.info("Loading table design for %d relation(s) to mark required relations", len(relations))
    ordered_descriptions = order_by_dependencies(relations)
    # Start with all descriptions that are matching the required selector
    required_relations = [
        description for description in ordered_descriptions if required_selector.match(description.target_table_name)
    ]
    # Walk through descriptions in reverse dependency order, expanding required set based on
    # dependency fan-out.
    for description in ordered_descriptions[::-1]:
        if any([description.target_table_name in required.dependencies for required in required_relations]):
            required_relations.append(description)

    for relation in ordered_descriptions:
        relation._is_required = False
    for relation in required_relations:
        relation._is_required = True

    logger.info("Marked %d relation(s) as required based on selector: %s", len(required_relations), required_selector)


def find_matches(relations: List[RelationDescription], selector: TableSelector):
    """Return list of matching relations."""
    return [relation for relation in relations if selector.match(relation.target_table_name)]


def find_dependents(
    relations: List[RelationDescription], seed_relations: List[RelationDescription]
) -> List[RelationDescription]:
    """
    Return list of relations that depend on the seed relations (directly or transitively).

    For this to really work, the list of relations should be sorted in "execution order"!
    """
    seeds = frozenset(relation.identifier for relation in seed_relations)
    in_dependency_path = set(seeds)
    for relation in relations:
        if any(dependency.identifier in in_dependency_path for dependency in relation.dependencies):
            in_dependency_path.add(relation.identifier)
    dependents = in_dependency_path - seeds
    return [relation for relation in relations if relation.identifier in dependents]


def find_immediate_dependencies(
    relations: List[RelationDescription], selector: TableSelector
) -> List[RelationDescription]:
    """
    Return list of VIEW relations that directly (or chained) hang off the selected relations.

    If you "DROP TABLE ..." from the relations, then the returned views would get dropped as well.

    If the relations are: A (CTAS) -> B (VIEW) -> C (VIEW) -> D (CTAS) and you pass in the
    list of A, B, C, D with a selector for A, then this will return the views B and C.
    """
    directly_selected_relations = find_matches(relations, selector)

    directly_selected = frozenset(relation.identifier for relation in directly_selected_relations)
    immediate = set(directly_selected)
    for relation in relations:
        if relation.is_view_relation and any(
            dependency.identifier in immediate for dependency in relation.dependencies
        ):
            immediate.add(relation.identifier)
    return [relation for relation in relations if relation.identifier in (immediate - directly_selected)]


def select_in_execution_order(
    relations: List[RelationDescription],
    selector: TableSelector,
    include_dependents=False,
    continue_from: Optional[str] = None,
) -> List[RelationDescription]:
    """
    Return list of relations that were selected, optionally adding dependents or skipping forward.

    The values supported for skipping forward are:
      - '*' to start from the first relation (which is the same behavior as passing in None)
      - ':transformations' to start with the first transformation (i.e. non-source) relation
      - other value to skip forward to the first matching relation
    """
    logger.info("Pondering execution order of %d relation(s)", len(relations))
    execution_order = order_by_dependencies(relations)
    selected = find_matches(execution_order, selector)
    if include_dependents:
        dependents = find_dependents(execution_order, selected)
        combined = frozenset(relation.identifier for relation in chain(selected, dependents))
        selected = [relation for relation in execution_order if relation.identifier in combined]
    if not selected:
        logger.warning("Found no relations matching: %s", selector)
    elif continue_from == "*":
        logger.info("Continuing from first (selected) relation since '*' was used")
    elif continue_from in (":transformations", ":transformation"):
        selected = list(dropwhile(lambda relation: not relation.is_transformation, selected))
        if selected:
            logger.info("Continuing from first transformation in (selected) relations")
        else:
            logger.warning("Found no transformations in list of relations to continue from")
    elif continue_from is not None:
        logger.info("Trying to fast forward to '%s'", continue_from)
        selected = list(dropwhile(lambda relation: relation.identifier != continue_from, selected))
        if not selected:
            logger.warning("Found no relation to continue from while matching '%s'", continue_from)
    return selected


if __name__ == "__main__":
    import sys

    import simplejson as json

    from etl.design.bootstrap import make_item_sorter
    from etl.json_encoder import FancyJsonEncoder

    config_dir = os.environ.get("DATA_WAREHOUSE_CONFIG", "./config")
    uri_parts = ("file", "localhost", "schemas")
    print(
        "Reading designs from '{schema_dir}' with config in '{config_dir}'.".format(
            schema_dir=uri_parts[2], config_dir=config_dir
        ),
        file=sys.stderr,
    )
    etl.config.load_config([config_dir])
    dw_config = etl.config.get_dw_config()
    base_schemas = [s.name for s in dw_config.schemas]
    selector = etl.names.TableSelector(base_schemas=base_schemas)
    required_selector = dw_config.required_in_full_load_selector
    file_sets = etl.file_sets.find_file_sets(uri_parts, selector)
    descriptions = RelationDescription.from_file_sets(file_sets, required_relation_selector=required_selector)
    if len(sys.argv) > 1:
        selector = etl.names.TableSelector(sys.argv[1:])
        descriptions = [d for d in descriptions if selector.match(d.target_table_name)]

    native = [d.table_design for d in descriptions]
    print(json.dumps(native, cls=FancyJsonEncoder, default=str, indent=4, item_sort_key=make_item_sorter()))
