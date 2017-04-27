from contextlib import closing
import logging
import os.path
from typing import List, Mapping

import simplejson as json
from psycopg2.extensions import connection  # only for type annotation

import etl.config
from etl.config.dw import DataWarehouseSchema
from etl.design import Attribute, ColumnDefinition
import etl.design.load
import etl.file_sets
from etl.names import TableName, TableSelector, join_with_quotes
import etl.pg
from etl.relation import RelationDescription
import etl.s3

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def fetch_tables(cx: connection, source: DataWarehouseSchema, selector: TableSelector) -> List[TableName]:
    """
    Retrieve all tables for this source (and matching the selector) and return them as a list of TableName instances.

    The :source configuration contains a "whitelist" (which tables to include) and a
    "blacklist" (which tables to exclude). Note that "exclude" always overrides "include."
    The list of tables matching the whitelist but not the blacklist can be further narrowed
    down by the pattern in :selector.
    """
    # Look for 'r'elations (ordinary tables), 'm'aterialized views, and 'v'iews in the catalog.
    result = etl.pg.query(cx, """
        SELECT nsp.nspname AS "schema"
             , cls.relname AS "table"
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS nsp ON cls.relnamespace = nsp.oid
         WHERE cls.relname NOT LIKE 'tmp%%'
           AND cls.relname NOT LIKE 'pg_%%'
           AND cls.relkind IN ('r', 'm', 'v')
         ORDER BY nsp.nspname
                , cls.relname
         """)
    found = []
    for row in result:
        source_table_name = TableName(row['schema'], row['table'])
        target_table_name = TableName(source.name, row['table'])
        for reject_pattern in source.exclude_tables:
            if source_table_name.match_pattern(reject_pattern):
                logger.debug("Table '%s' matches blacklist", source_table_name.identifier)
                break
        else:
            for accept_pattern in source.include_tables:
                if source_table_name.match_pattern(accept_pattern):
                    if selector.match(target_table_name):
                        found.append(source_table_name)
                        logger.debug("Table '%s' is included in result set", source_table_name.identifier)
                        break
                    else:
                        logger.debug("Table '%s' matches whitelist but is not selected", source_table_name.identifier)
    logger.info("Found %d table(s) matching patterns; whitelist=%s, blacklist=%s, subset='%s'",
                len(found), source.include_tables, source.exclude_tables, selector)
    return found


def fetch_attributes(cx: connection, table_name: TableName) -> List[Attribute]:
    """
    Retrieve table definition (column names and types).
    """
    # Make sure to turn on "User Parameters" in the Database settings of PyCharm so that `%s` works in the editor.
    attributes = etl.pg.query(cx, """
        SELECT a.attname AS "name"
             , pg_catalog.format_type(t.oid, a.atttypmod) AS "sql_type"
             , a.attnotnull AS "not_null"
          FROM pg_catalog.pg_attribute AS a
          JOIN pg_catalog.pg_class AS cls ON a.attrelid = cls.oid
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
         WHERE a.attnum > 0  -- skip system columns
           AND NOT a.attisdropped
           AND ns.nspname = %s
           AND cls.relname = %s
         ORDER BY a.attnum
         """, (table_name.schema, table_name.table))
    return [Attribute(**att) for att in attributes]


def fetch_constraints(cx: connection, table_name: TableName) -> Mapping[str, List[str]]:
    """
    Retrieve table constraints from database by looking up indices.

    We will only check primary key constraints and unique constraints.
    (To recreate the constraint, we could use `pg_get_indexdef`.)
    """
    # We need to make two trips to the database because Redshift doesn't support functions on int2vector types.
    # So we find out which indices exist (with unique constraints) and how many attributes are related to each,
    # then we look up the attribute names with our exquisitely hand-rolled "where" clause.
    # See http://www.postgresql.org/message-id/10279.1124395722@sss.pgh.pa.us for further explanations.
    indices = etl.pg.query(cx, """
        SELECT i.indexrelid AS index_id
             , ic.relname AS index_name
             , CASE
                   WHEN i.indisprimary THEN 'primary_key'
                   ELSE 'unique'
               END AS "constraint_type"
             , i.indnatts AS nr_atts
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_index AS i ON cls.oid = i.indrelid
          JOIN pg_catalog.pg_class AS ic ON i.indexrelid = ic.oid
         WHERE i.indisunique
           AND ns.nspname = %s
           AND cls.relname = %s
         ORDER BY ic.relname
        """, (table_name.schema, table_name.table))
    found = []
    for index_id, index_name, constraint_type, nr_atts in indices:
        cond = ' OR '.join("a.attnum = i.indkey[%d]" % i for i in range(2))
        attributes = etl.pg.query(cx, """
            SELECT a.attname AS "name"
              FROM pg_catalog.pg_attribute AS a
              JOIN pg_catalog.pg_index AS i ON a.attrelid = i.indrelid
              WHERE i.indexrelid = %%s
                AND (%s)
              ORDER BY a.attname
              """ % cond, (index_id,))
        columns = list(att["name"] for att in attributes)
        logger.info("Index '%s' of '%s' adds constraint %s",
                    index_name, table_name.identifier, json.dumps({constraint_type: columns}))
        found.append({constraint_type: columns})
    return found


def fetch_dependencies(cx: connection, table_name: TableName) -> List[TableName]:
    """
    Lookup dependencies (other tables)
    """
    # See from https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_constraint_dependency.sql
    stmt = """
        SELECT DISTINCT
               target_ns.nspname AS "schema"
             , target_cls.relname AS "table"
          FROM pg_catalog.pg_class AS cls
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_depend AS dep ON cls.oid = dep.refobjid
          JOIN pg_catalog.pg_depend AS target_dep ON dep.objid = target_dep.objid
          JOIN pg_catalog.pg_class AS target_cls ON target_dep.refobjid = target_cls.oid AND cls.oid <> target_cls.oid
          JOIN pg_catalog.pg_namespace AS target_ns ON target_cls.relnamespace = target_ns.oid
         WHERE ns.nspname = %s
           AND cls.relname = %s
        """
    dependencies = etl.pg.query(cx, stmt, (table_name.schema, table_name.table))
    return [TableName(**row).identifier for row in dependencies]


def create_table_design(conn, source_table_name, target_table_name, type_maps):
    """
    Create (and return) new table design
    """
    source_attributes = fetch_attributes(conn, source_table_name)
    target_columns = [ColumnDefinition.from_attribute(att,
                                                      type_maps["as_is_att_type"],
                                                      type_maps["cast_needed_att_type"]) for att in source_attributes]
    table_design = {
        "name": "%s" % target_table_name.identifier,
        "source_name": "%s.%s" % (target_table_name.schema, source_table_name.identifier),
        "columns": [column.to_dict() for column in target_columns]
    }
    constraints = fetch_constraints(conn, source_table_name)
    if constraints:
        table_design["constraints"] = constraints
    return table_design


def create_table_design_for_ctas(conn, table_name, type_maps):
    """
    Create (and return) new table design for a CTAS
    """
    table_design = create_table_design(conn, table_name, table_name, type_maps)
    table_design["source_name"] = "CTAS"
    table_design["depends_on"] = fetch_dependencies(conn, table_name)
    return table_design


def create_table_design_for_view(conn, table_name):
    """
    Create (and return) new table design suited for a view
    """
    columns = fetch_attributes(conn, table_name)
    table_design = {
        "name": "%s" % table_name.identifier,
        "source_name": "VIEW",
        "columns": [{"name": column.name} for column in columns],
        "depends_on": fetch_dependencies(conn, table_name)
    }
    return table_design


def make_key_sorter():
    """
    Return some value that allows sorting keys that appear in any "object" (JSON-speak for dict)
    so that the resulting order of keys is easier to digest by humans.

    That "some value" is a tuple of (preferred order, key name).
    """
    preferred_order = [
        "name", "description",  # always (tables, columns, etc.)
        "source_name", "unload_target", "columns", "constraints", "attributes", "depends_on",  # only tables
        "primary_key", "surrogate_key", "natural_key", "unique",  # only table constraints
        "sql_type", "type", "expression"  # only columns
    ]
    # The ".lower()" calls are here to stay compatible with default implementation.
    order_lookup = {key: (i, key.lower()) for i, key in enumerate(preferred_order)}
    max_index = len(preferred_order)

    def sorter(item):
        key, value = item
        return order_lookup.get(key, (max_index, key.lower()))

    return sorter


def save_table_design(local_dir, source_name, source_table_name, table_design, dry_run=False) -> None:
    """
    Write new table design file to disk.

    Although this files are generated by computers, they get read by humans. So we try to be nice
    here and write them out with a specific order of the keys, like have name and description towards
    the top.
    """
    target_table_name = TableName(source_name, source_table_name.table)
    table = target_table_name.identifier
    # FIXME Move this logic into file sets (note that "source_name" is in table_design)
    filename = os.path.join(local_dir, source_name, "{}-{}.yaml".format(source_table_name.schema,
                                                                        source_table_name.table))
    if dry_run:
        logger.info("Dry-run: Skipping writing new table design file for '%s'", table)
    elif os.path.exists(filename):
        logger.warning("Skipping writing new table design for '%s' since '%s' already exists", table, filename)
    else:
        # Validate before writing to make sure we don't drift between bootstrap and JSON schema.
        etl.design.load.validate_table_design(table_design, target_table_name)
        logger.info("Writing new table design file for '%s' to '%s'", table, filename)
        # We use JSON pretty printing because it is prettier than YAML printing.
        with open(filename, 'w') as o:
            json.dump(table_design, o, indent="    ", item_sort_key=make_key_sorter())
            o.write('\n')
        logger.debug("Completed writing '%s'", filename)


def normalize_and_create(directory: str, dry_run=False) -> str:
    """
    Make sure the directory exists and return normalized path to it.

    This will create all intermediate directories as needed.
    """
    name = os.path.normpath(directory)
    if not os.path.exists(name):
        if dry_run:
            logger.debug("Dry-run: Skipping creation of directory '%s'", name)
        else:
            logger.info("Creating directory '%s'", name)
            os.makedirs(name)
    return name


def create_table_designs_from_source(source, selector, local_dir, local_files, type_maps, dry_run=False):
    """
    Create table design files for tables from a single source to local directory.
    Whenever some table designs already exist locally, validate them against the information found from upstream.
    """
    source_files = {file_set.source_table_name: file_set
                    for file_set in local_files
                    if file_set.source_name == source.name and file_set.design_file_name}
    try:
        logger.info("Connecting to database source '%s' to look for tables", source.name)
        with closing(etl.pg.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            source_tables = fetch_tables(conn, source, selector)
            for source_table_name in source_tables:
                if source_table_name in source_files:
                    logger.info("Skipping '%s' from source '%s' because table design file exists: '%s'",
                                source_table_name.identifier, source.name,
                                source_files[source_table_name].design_file_name)
                else:
                    target_table_name = TableName(source.name, source_table_name.table)
                    table_design = create_table_design(conn, source_table_name, target_table_name, type_maps)
                    save_table_design(local_dir, source.name, source_table_name, table_design, dry_run=dry_run)
        logger.info("Done with %d table(s) from source '%s'", len(source_tables), source.name)
    except Exception:
        logger.critical("Error while processing source '%s'", source.name)
        raise

    existent = frozenset(name.identifier for name in source_files)
    upstream = frozenset(name.identifier for name in source_tables)
    not_found = upstream.difference(existent)
    if not_found:
        logger.warning("New table(s) in '%s' without local design: %s", source.name, join_with_quotes(not_found))
    too_many = existent.difference(upstream)
    if too_many:
        logger.error("Table design(s) without upstream table in '%s': %s", source.name, join_with_quotes(too_many))

    return len(source_tables)


def create_views(dsn_etl: dict, relations: List[RelationDescription], dry_run=False):
    """
    Create views for queries that do not already have a table design.

    To avoid modifying the data warehouse by accident, this will fail if any of the relations already exist.
    """
    with closing(etl.pg.connection(dsn_etl)) as conn:
        with conn:
            for relation in relations:
                ddl_stmt = """CREATE VIEW {} AS\n{}""".format(relation, relation.query_stmt)
                if dry_run:
                    logger.info("Dry-run: Skipping creation of view '%s'", relation.identifier)
                    logger.debug("Testing query for '%s' (syntax, dependencies, ...)", relation.identifier)
                    etl.pg.explain(conn, relation.query_stmt)
                else:
                    logger.info("Creating view for '%s' which has no design file", relation.identifier)
                    etl.pg.execute(conn, ddl_stmt)


def drop_views(dsn_etl: dict, relations: List[RelationDescription], dry_run=False):
    """
    Delete views that were created at the beginning of bootstrap.
    """
    with closing(etl.pg.connection(dsn_etl, autocommit=True)) as conn:
        for relation in relations:
            # Since this mirrors the all-or-nothing create_views we recklessly call "DROP VIEW" without "IF EXISTS".
            ddl_stmt = """DROP VIEW {}""".format(relation)
            if dry_run:
                logger.info("Dry-run: Skipping deletion of view '%s'", relation.identifier)
            else:
                logger.info("Dropping view for '%s'", relation.identifier)
                etl.pg.execute(conn, ddl_stmt)


def bootstrap_sources(schemas, selector, table_design_dir, local_files, type_maps, dry_run=False):
    """
    Download schemas from database tables and compare against local design files (if available).
    This will create new design files locally if they don't already exist for any relations tied
    to upstream database sources.
    """
    total = 0
    for schema in schemas:
        if selector.match_schema(schema.name):
            if not schema.is_database_source:
                logger.info("Skipping schema which is not an upstream database source: '%s'", schema.name)
            else:
                normalize_and_create(os.path.join(table_design_dir, schema.name), dry_run=dry_run)
                total += create_table_designs_from_source(schema, selector, table_design_dir, local_files,
                                                          type_maps, dry_run=dry_run)
    if not total:
        logger.warning("Found no matching tables in any upstream source for '%s'", selector)


def bootstrap_transformations(dsn_etl, schemas, local_dir, local_files, type_maps, as_view, dry_run=False):
    """
    Download design information for transformations.
    """
    is_upstream = {schema.name for schema in schemas if schema.is_upstream_source}
    new_files = [file_set for file_set in local_files
                 if not file_set.design_file_name and file_set.source_name not in is_upstream]
    if not new_files:
        logger.info("Found no queries without matching design files")
        return

    transforms = [RelationDescription(file_set) for file_set in new_files]

    create_views(dsn_etl, transforms, dry_run=dry_run)
    try:
        with closing(etl.pg.connection(dsn_etl, readonly=True)) as conn:
            for relation in transforms:
                table_name = relation.target_table_name
                if as_view:
                    table_design = create_table_design_for_view(conn, table_name)
                else:
                    table_design = create_table_design_for_ctas(conn, table_name, type_maps)
                save_table_design(local_dir, table_name.schema, table_name, table_design, dry_run=dry_run)
    finally:
        drop_views(dsn_etl, transforms, dry_run=dry_run)
