from collections import namedtuple
from contextlib import closing
from itertools import groupby
import logging
from operator import attrgetter
import os.path
import re

import simplejson as json

import etl
import etl.config
import etl.design.load
from etl.errors import MissingMappingError
import etl.pg
import etl.file_sets
import etl.s3


class Attribute(namedtuple("_ColumnInfo", ["name", "sql_type", "not_null"])):
    __slots__ = ()


class ColumnDefinition(namedtuple("_ColumnDefinition",
                                  ["name",  # always
                                   "type", "sql_type",  # always for tables
                                   "source_sql_type", "expression", "not_null", "references"  # optional
                                   ])):
    """
    Wrapper for column attributes ... describes columns by name, type (similar to Avro), and sql_type.
    """
    __slots__ = ()


def fetch_tables(cx, source, selector):
    """
    Retrieve all tables for this source (and matching the selector) and return them as a list of TableName instances.

    The :source configuration contains a "whitelist" (which tables to include) and a
    "blacklist" (which tables to exclude). Note that "exclude" always overrides "include."
    The list of tables matching the whitelist but not the blacklist can be further narrowed
    down by the pattern in :selector.
    """
    logger = logging.getLogger(__name__)
    # Look for 'r'elations (ordinary tables), 'm'aterialized views, and 'v'iews in the catalog.
    result = etl.pg.query(cx, """
        SELECT nsp.nspname AS "schema"
             , cls.relname AS "table"
          FROM pg_catalog.pg_class cls
          JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
         WHERE cls.relname NOT LIKE 'tmp%%'
               AND cls.relname NOT LIKE 'pg_%%'
               AND cls.relkind IN ('r', 'm', 'v')
         ORDER BY nsp.nspname, cls.relname
         """)
    found = []
    for row in result:
        source_table_name = etl.TableName(row['schema'], row['table'])
        target_table_name = etl.TableName(source.name, row['table'])
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
    logging.getLogger(__name__).info("Found %d table(s) matching patterns; whitelist=%s, blacklist=%s, subset='%s'",
                                     len(found), source.include_tables, source.exclude_tables, selector)
    return found


def fetch_attributes(cx, table_name):
    """
    Retrieve table definition (column names and types).
    """
    # TODO Multiple indices lead to multiple rows per attribute when using join with pg_index
    # Make sure to turn on "User Parameters" in the Database settings of PyCharm so that `%s` works in the editor.
    attributes = etl.pg.query(cx, """
        SELECT a.attname AS attribute_name
             , pg_catalog.format_type(t.oid, a.atttypmod) AS attribute_type
             , a.attnotnull AS not_null_constraint
               -- , COALESCE(i.indisunique, FALSE) AS is_unique_constraint
               -- , COALESCE(i.indisprimary, FALSE) AS is_primary_constraint
          FROM pg_catalog.pg_attribute AS a
          JOIN pg_catalog.pg_class AS cls ON a.attrelid = cls.oid
          JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
          JOIN pg_catalog.pg_type AS t ON a.atttypid = t.oid
               -- LEFT JOIN pg_catalog.pg_index AS i ON a.attrelid = i.indrelid
               --                                   AND a.attnum = ANY(i.indkey)
         WHERE a.attnum > 0  -- skip system columns
               AND NOT a.attisdropped
               AND ns.nspname = %s
               AND cls.relname = %s
         ORDER BY a.attnum
         """, (table_name.schema, table_name.table))
    logging.getLogger(__name__).info("Found %d column(s) in relation '%s'", len(attributes), table_name.identifier)
    return [Attribute(name=att["attribute_name"], sql_type=att["attribute_type"], not_null=att["not_null_constraint"])
            for att in attributes]


def map_types_in_ddl(table_name, attributes, as_is_att_type, cast_needed_att_type):
    """"
    Replace unsupported column types by supported ones and determine casting
    spell (for a single table).

    Result for every table is a "column definition", which is basically a list
    of tuples with name, old type, new type, expression information (where
    the expression within a SELECT will return the value of the attribute with
    the "new" type), serialization type, and not null constraint (boolean).
    """
    new_columns = []
    for attribute in attributes:
        for re_att_type, generic_type in as_is_att_type.items():
            if re.match('^' + re_att_type + '$', attribute.sql_type):
                # Keep the type, use no expression, and pick generic type from map.
                mapping_sql_type, mapping_expression, mapping_type = attribute.sql_type, None, generic_type
                break
        else:
            for re_att_type, (mapping_sql_type, mapping_expression, mapping_type) in cast_needed_att_type.items():
                if re.match(re_att_type, attribute.sql_type):
                    # Found tuple with new SQL type, expression and generic type.  Rejoice.
                    break
            else:
                raise MissingMappingError("Unknown type '{}' of {}.{}.{}".format(attribute.sql_type,
                                                                                 table_name.schema,
                                                                                 table_name.table,
                                                                                 attribute.name))
        delimited_name = '"{}"'.format(attribute.name)
        new_columns.append(ColumnDefinition(name=attribute.name,
                                            source_sql_type=attribute.sql_type,
                                            sql_type=mapping_sql_type,
                                            # Replace %s in the column expression by the column name.
                                            expression=(mapping_expression % delimited_name
                                                        if mapping_expression else None),
                                            type=mapping_type,
                                            not_null=attribute.not_null,
                                            references=None))
    return new_columns


def fetch_dependencies(cx, table_name):
    """
    Lookup dependencies (other tables)
    """
    # based on example query in AWS docs; *_p is for parent, *_c is for child
    dependencies = etl.pg.query(cx, """
        SELECT DISTINCT
               n_c.nspname AS dependency_schema
             , c_c.relname AS dependency_table
          FROM pg_class c_p
          JOIN pg_depend d_p ON c_p.relfilenode = d_p.refobjid
          JOIN pg_depend d_c ON d_p.objid = d_c.objid
          -- the following OR statement covers the case where a COPY has issued a new OID for an upstream table
          JOIN pg_class c_c ON d_c.refobjid = c_c.relfilenode OR d_c.refobjid = c_c.oid
          LEFT JOIN pg_namespace n_p ON c_p.relnamespace = n_p.oid
          LEFT JOIN pg_namespace n_c ON c_c.relnamespace = n_c.oid
         WHERE n_p.nspname = %s AND c_p.relname = %s
            -- do not include the table itself in its dependency list
           AND c_p.oid != c_c.oid
         ORDER BY dependency_schema, dependency_table
        """, (table_name.schema, table_name.table))
    return [etl.TableName(schema=row['dependency_schema'], table=row['dependency_table']).identifier
            for row in dependencies]


def create_table_design(conn, source_table_name, target_table_name, type_maps):
    """
    Create (and return) new table design
    """
    source_columns = fetch_attributes(conn, source_table_name)
    target_columns = map_types_in_ddl(source_table_name,
                                      source_columns,
                                      type_maps["as_is_att_type"],
                                      type_maps["cast_needed_att_type"])
    table_design = {
        "name": "%s" % target_table_name.identifier,
        "source_name": "%s.%s" % (target_table_name.schema, source_table_name.identifier),
        "columns": [column._asdict() for column in target_columns]
    }
    # FIXME Extract actual primary keys from pg_catalog, add unique constraints
    if any(column.name == "id" for column in target_columns):
        table_design["constraints"] = {"primary_key": ["id"]}
    # Remove default settings as well as empty expressions (when columns can be selected by name)
    for column in table_design["columns"]:
        if column["expression"] is None:
            del column["expression"]
        if column["source_sql_type"] == column["sql_type"]:
            del column["source_sql_type"]
        if not column["not_null"]:
            del column["not_null"]
        if not column["references"]:
            del column["references"]
    # Make sure schema and code to create table design files is in sync.
    return etl.design.load.validate_table_design(table_design, target_table_name)


def create_table_design_for_view(conn, table_name):
    """
    Create (and return) new table design suited for a view
    """
    columns = fetch_attributes(conn, table_name)
    table_design = {
        "name": "%s" % table_name.identifier,
        "source_name": "VIEW",
        "columns": [{"name": column.name} for column in columns]
    }
    return etl.design.load.validate_table_design(table_design, table_name)


def save_table_design(local_dir, source_name, source_table_name, table_design, dry_run=False):
    """
    Write new table design file to disk.
    """
    logger = logging.getLogger(__name__)
    table = table_design["name"]
    # FIXME Move this logic into file sets (note that "source_name" is in table_design)
    filename = os.path.join(local_dir, source_name, "{}-{}.yaml".format(source_table_name.schema,
                                                                        source_table_name.table))
    if dry_run:
        logger.info("Dry-run: Skipping writing new table design file for '%s'", table)
    elif os.path.exists(filename):
        logger.warning("Skipping writing new table design for '%s' since '%s' already exists", table, filename)
    else:
        logger.info("Writing new table design file for '%s' to '%s'", table, filename)
        with open(filename, 'w') as o:
            # JSON pretty printing is prettier than YAML printing.
            json.dump(table_design, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.debug("Completed writing '%s'", filename)


def normalize_and_create(directory: str, dry_run=False) -> str:
    """
    Make sure the directory exists and return normalized path to it.

    This will create all intermediate directories as needed.
    """
    logger = logging.getLogger(__name__)
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
    logger = logging.getLogger(__name__)
    source_files = {file_set.source_table_name: file_set
                    for file_set in local_files if file_set.source_name == source.name}
    try:
        logger.info("Connecting to database source '%s' to look for tables", source.name)
        with closing(etl.pg.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            source_tables = fetch_tables(conn, source, selector)
            for source_table_name in sorted(source_tables):
                target_table_name = etl.TableName(source.name, source_table_name.table)
                table_design = create_table_design(conn, source_table_name, target_table_name, type_maps)
                source_file_set = source_files.get(source_table_name)
                if source_file_set and source_file_set.design_file_name:
                    # Replace bootstrapped table design with one from file but check whether set of columns changed.
                    design_file = source_file_set.design_file_name
                    existing_table_design = etl.design.load.load_table_design_from_localfile(design_file,
                                                                                             target_table_name)
                    compare_columns(table_design, existing_table_design)
                else:
                    save_table_design(local_dir, source.name, source_table_name, table_design, dry_run=dry_run)
    except Exception:
        logger.critical("Error while processing source '%s'", source.name)
        raise
    logger.info("Done with %d table(s) from source '%s'", len(source_tables), source.name)

    existent = frozenset(source_files)
    upstream = frozenset(source_tables)
    not_found = upstream.difference(existent)
    if not_found:
        logger.warning("New table(s) which had no local design: %s", etl.TableName.join_with_quotes(not_found))
    too_many = existent.difference(upstream)
    if too_many:
        logger.warning("Old table(s) which no longer had upstream table: %s", etl.TableName.join_with_quotes(too_many))

    return len(source_tables)


def create_views(dsn_etl, local_files, dry_run=False):
    """
    Create views for queries that do not already have a table design
    """
    logger = logging.getLogger(__name__)
    with closing(etl.pg.connection(dsn_etl, autocommit=True)) as conn:
        for file_set in local_files:
            # We cannot use the query_stmt from RelationDescription because we'd have a circular dependency.
            with open(file_set.sql_file_name) as f:
                query_stmt = f.read()
            ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(file_set.target_table_name, query_stmt)
            if dry_run:
                logger.info("Dry-run: Skipping creation of view '%s'", file_set.target_table_name.identifier)
            else:
                logger.info("Creating view for '%s' which has no design file",
                            file_set.target_table_name.identifier)
                etl.pg.execute(conn, ddl_stmt)


def cleanup_views(dsn_etl, local_files, dry_run=False):
    """
    Delete views that were created at the beginning of bootstrap
    """
    logger = logging.getLogger(__name__)
    with closing(etl.pg.connection(dsn_etl, autocommit=True)) as conn:
        for file_set in local_files:
            ddl_stmt = """DROP VIEW IF EXISTS {}""".format(file_set.target_table_name)
            if dry_run:
                logger.info("Dry-run: Skipping deletion of view '%s'", file_set.target_table_name.identifier)
            else:
                logger.info("Dropping view for '%s'", file_set.target_table_name.identifier)
                etl.pg.execute(conn, ddl_stmt)


def compare_columns(live_design, file_design):
    """
    Compare columns between what is actually present in a table vs. what is described in a table design
    """
    logger = logging.getLogger(__name__)
    logger.info("Checking design for '%s'", live_design["name"])
    live_columns = {column["name"] for column in live_design["columns"]}
    file_columns = {column["name"] for column in file_design["columns"]}
    # TODO define policy to declare columns "ETL-only" Or remove this "feature"?
    etl_columns = {name for name in file_columns if name.startswith("etl__")}
    logger.debug("Number of columns of '%s' in database: %d vs. in design: %d (ETL: %d)",
                 file_design["name"], len(live_columns), len(file_columns), len(etl_columns))
    not_accounted_for_on_file = live_columns.difference(file_columns)
    described_but_not_live = file_columns.difference(live_columns).difference(etl_columns)
    if not_accounted_for_on_file:
        logger.warning("New columns in '%s' that are not in existing table design: %s",
                       file_design["name"], sorted(not_accounted_for_on_file))
        indices = dict((name, i) for i, name in enumerate(column["name"] for column in live_design["columns"]))
        for name in not_accounted_for_on_file:
            logger.debug("New column %s.%s: %s", live_design["name"], name,
                         json.dumps(live_design["columns"][indices[name]], indent="    ", sort_keys=True))
    if described_but_not_live:
        logger.warning("Columns that have disappeared in '%s': %s", file_design["name"], sorted(described_but_not_live))


def bootstrap_sources(schemas, selector, table_design_dir, local_files, type_maps, dry_run=False):
    """
    Download schemas from database tables and compare against local design files (if available).
    This will create new design files locally if they don't already exist for any relations tied
    to upstream database sources.
    """
    logger = logging.getLogger(__name__)
    total = 0
    for schema in schemas:
        if not schema.is_database_source:
            logger.debug("Skipping schema which is not an upstream database source: '%s'", schema.name)
        elif selector.match_schema(schema.name):
            normalize_and_create(os.path.join(table_design_dir, schema.name), dry_run=dry_run)
            total += create_table_designs_from_source(schema, selector, table_design_dir, local_files,
                                                      type_maps, dry_run=dry_run)
    if not total:
        logger.warning("Found no matching tables in any upstream source for '%s'", selector)


def bootstrap_transformations(dsn_etl, schemas, local_dir, local_files, type_maps, as_view, dry_run=False):
    """
    Download design information for transformations.
    """
    logger = logging.getLogger(__name__)
    is_upstream = {schema.name for schema in schemas if schema.is_upstream_source}
    transforms = [file_set for file_set in local_files
                  if not file_set.design_file_name and file_set.source_name not in is_upstream]
    if not transforms:
        logger.info("Found no queries without matching design files")
        return

    logger.info("Found %d transformation(s) with a query but no design file", len(transforms))
    try:
        create_views(dsn_etl, transforms, dry_run=dry_run)
        with closing(etl.pg.connection(dsn_etl, autocommit=True)) as conn:
            for file_set in transforms:
                table_name = file_set.target_table_name
                if as_view:
                    table_design = create_table_design_for_view(conn, table_name)
                else:
                    table_design = create_table_design(conn, table_name, table_name, type_maps)
                    table_design["source_name"] = "CTAS"
                table_design["depends_on"] = fetch_dependencies(conn, table_name)
                save_table_design(local_dir, table_name.schema, table_name, table_design, dry_run=dry_run)
    finally:
        # FIXME If the view already existed before ... should we really delete it?
        cleanup_views(dsn_etl, transforms, dry_run=True)
