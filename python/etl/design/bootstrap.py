import logging
import os.path
import re
from contextlib import closing
from datetime import datetime
from typing import List, Mapping, Union

import simplejson as json
from psycopg2.extensions import connection  # only for type annotation

import etl.config
import etl.design.load
import etl.file_sets
import etl.pg
import etl.relation
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.design import Attribute, ColumnDefinition
from etl.names import TableName, TableSelector, join_with_quotes
from etl.relation import RelationDescription

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
    stmt = """
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
         ORDER BY a.attnum"""
    attributes = etl.pg.query(cx, stmt, (table_name.schema, table_name.table))
    return [Attribute(**att) for att in attributes]


def fetch_constraints(cx: connection, table_name: TableName) -> List[Mapping[str, List[str]]]:
    """
    Retrieve table constraints from database by looking up indices.

    We will only check primary key constraints and unique constraints. Also, if constraints
    use functions we'll probably miss them.

    (To recreate the constraint, we could use `pg_get_indexdef`.)
    """
    # We need to make two trips to the database because Redshift doesn't support functions on int2vector types.
    # So we find out which indices exist (with unique constraints) and how many attributes are related to each,
    # then we look up the attribute names with our exquisitely hand-rolled "where" clause.
    # See http://www.postgresql.org/message-id/10279.1124395722@sss.pgh.pa.us for further explanations.
    stmt_index = """
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
         ORDER BY "constraint_type", ic.relname"""
    indices = etl.pg.query(cx, stmt_index, (table_name.schema, table_name.table))

    stmt_att = """
        SELECT a.attname AS "name"
          FROM pg_catalog.pg_attribute AS a
          JOIN pg_catalog.pg_index AS i ON a.attrelid = i.indrelid
          WHERE i.indexrelid = %%s
            AND (%s)
          ORDER BY a.attname"""
    found = []
    for index_id, index_name, constraint_type, nr_atts in indices:
        cond = ' OR '.join("a.attnum = i.indkey[%d]" % i for i in range(nr_atts))
        attributes = etl.pg.query(cx, stmt_att % cond, (index_id,))
        if attributes:
            columns = list(att["name"] for att in attributes)
            constraint = {constraint_type: columns}  # type: Mapping[str, List[str]]
            logger.info("Index '%s' of '%s' adds constraint %s",
                        index_name, table_name.identifier, json.dumps(constraint))
            found.append(constraint)
    return found


def fetch_dependencies(cx: connection, table_name: TableName) -> List[TableName]:
    """
    Lookup dependencies (other tables)
    """
    # See https://github.com/awslabs/amazon-redshift-utils/blob/master/src/AdminViews/v_constraint_dependency.sql
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
         ORDER BY "schema", "table"
        """
    dependencies = etl.pg.query(cx, stmt, (table_name.schema, table_name.table))
    return [TableName(**row).identifier for row in dependencies]


def create_partial_table_design(conn: connection, source_table_name: TableName, target_table_name: TableName):
    """
    Return partial table design that contains
        - the name (identifier of our target table)
        - full column information (extracted from database source or data warehouse)
        - a description (with a timestamp)
    What is missing then to make it a valid table design is at least the "source_name".
    """
    type_maps = etl.config.get_dw_config().type_maps
    as_is_attribute_type = type_maps["as_is_att_type"]  # source tables and CTAS
    cast_needed_attribute_type = type_maps["cast_needed_att_type"]  # only source tables

    source_attributes = fetch_attributes(conn, source_table_name)
    target_columns = [ColumnDefinition.from_attribute(attribute,
                                                      as_is_attribute_type,
                                                      cast_needed_attribute_type) for attribute in source_attributes]
    table_design = {
        "name": "%s" % target_table_name.identifier,
        "description": "Automatically generated on {0:%Y-%m-%d} at {0:%H:%M:%S}".format(datetime.utcnow()),
        "columns": [column.to_dict() for column in target_columns]
    }
    return table_design


def create_table_design_for_source(conn: connection, source_table_name: TableName, target_table_name: TableName):
    """
    Create new table design for a table in an upstream source. If present, we gather the constraints from the source.

    (Note that only upstream tables can have constraints derived from inspecting the database.)
    """
    table_design = create_partial_table_design(conn, source_table_name, target_table_name)
    table_design["source_name"] = "%s.%s" % (target_table_name.schema, source_table_name.identifier)
    constraints = fetch_constraints(conn, source_table_name)
    if constraints:
        table_design["constraints"] = constraints
    return table_design


def create_partial_table_design_for_transformation(conn: connection, tmp_view_name: TableName,
                                                   relation: RelationDescription, update_keys: Union[List, None]=None):
    """
    Create a partial design that's applicable to transformations, which
        - cleans up the column information (dropping accidental expressions)
        - adds dependencies (which only transformations can have)
        - and optionally updates from the existing table design
    """
    table_design = create_partial_table_design(conn, tmp_view_name, relation.target_table_name)
    # TODO When working with CTAS or VIEW, the type casting doesn't make sense but sometimes sneaks in.
    for column in table_design["columns"]:
        if "expression" in column:
            del column["expression"]
            del column["source_sql_type"]  # expression and source_sql_type always travel together

    dependencies = fetch_dependencies(conn, tmp_view_name)
    if dependencies:
        table_design["depends_on"] = dependencies

    if update_keys is not None and relation.design_file_name:
        logger.info("Experimental update of existing table design file in progress...")
        existing_table_design = relation.table_design
        if "columns" in update_keys:
            # If the old design had added an identity column, we carry it forward here (and always as the first column).
            identity = [column for column in existing_table_design["columns"] if column.get("identity")]
            if identity:
                table_design["columns"][:0] = identity
                table_design["columns"][0]["encoding"] = "raw"
            column_lookup = {column["name"]: column for column in existing_table_design["columns"]}
            for column in table_design["columns"]:
                update_column_definition(column, column_lookup.get(column["name"], {}))
        # In case we're updating from an auto-designed file, fix the description to reflect the update.
        table_design["description"] = table_design["description"].replace("generated", "updated", 1)
        if "description" in update_keys and "description" in existing_table_design:
            old_description = existing_table_design["description"]
            if not old_description.startswith(("Automatically generated on", "Automatically updated on")):
                table_design["description"] = old_description
        for copy_key in update_keys:
            if copy_key in existing_table_design and copy_key not in ("columns", "description"):
                # TODO may have to cleanup columns in constraints and attributes!
                table_design[copy_key] = existing_table_design[copy_key]
    return table_design


def update_column_definition(new_column: dict, old_column: dict):
    """
    Update column definition derived from inspecting the view created for the transformation
    with information from the existing table design.

    Copied directly: "description", "encoding", "references", and "not_null"
    Updated carefully: "sql_type", "type" (always together)
    """
    ok_to_copy = frozenset(["description", "encoding", "references", "not_null"])
    for key in ok_to_copy.intersection(old_column):
        new_column[key] = old_column[key]
    if "sql_type" in old_column:
        old_sql_type = old_column["sql_type"]
        new_sql_type = new_column["sql_type"]
        # Upgrade to "larger" type, mostly for keys
        if old_sql_type == "bigint" and new_sql_type == "integer":
            new_column["sql_type"] = "bigint"
            new_column["type"] = "long"
        varchar_re = re.compile("(?:varchar|character varying)\((?P<size>\d+)\)")
        old_text = varchar_re.search(old_sql_type)
        new_text = varchar_re.search(new_sql_type)
        if old_text and new_text:
            new_column["sql_type"] = "character varying({})".format(old_text.group('size'))
        numeric_re = re.compile("(?:numeric|decimal)\((?P<precision>\d+),(?P<scale>\d+)\)")
        old_numeric = numeric_re.search(old_sql_type)
        new_numeric = numeric_re.search(new_sql_type)
        if old_numeric and new_numeric:
            new_column["sql_type"] = "numeric({},{})".format(old_numeric.group('precision'), old_numeric.group('scale'))


def create_table_design_for_ctas(conn: connection, tmp_view_name: TableName, relation: RelationDescription,
                                 update: bool):
    """
    Create new table design for a CTAS.
    If tables are referenced, they are added in the dependencies list.

    If :update is True, try to merge additional information from any existing table design file.
    """
    update_keys = ["description", "unload_target", "columns", "constraints", "attributes"] if update else None
    table_design = create_partial_table_design_for_transformation(conn, tmp_view_name, relation, update_keys)
    table_design["source_name"] = "CTAS"
    return table_design


def create_table_design_for_view(conn: connection, tmp_view_name: TableName, relation: RelationDescription,
                                 update: bool):
    """
    Create (and return) new table design suited for a view. Views are expected to always depend on some
    other relations. (Also, we only keep the names for columns.)

    If :update is True, try to merge additional information from any existing table design file.
    """
    update_keys = ["description", "unload_target"] if update else None
    # This creates a full column description with types (testing the type-maps), then drop all of it but their names.
    table_design = create_partial_table_design_for_transformation(conn, tmp_view_name, relation, update_keys)
    table_design["source_name"] = "VIEW"
    table_design["columns"] = [{"name": column["name"]} for column in table_design["columns"]]
    return table_design


def make_item_sorter():
    """
    Return some value that allows sorting keys that appear in any "object" (JSON-speak for dict)
    so that the resulting order of keys is easier to digest by humans.

    Input to the sorter is a tuple of (key, value) from turning a dict into a list of items.
    Output (return value) of the sorter is a tuple of (preferred order, key name).
    If a key is not known, it's sorted alphabetically (ignoring case) after all known ones.
    """
    preferred_order = [
        "name", "description",  # always (tables, columns, etc.)
        "source_name", "unload_target", "depends_on", "columns", "constraints", "attributes",  # only tables
        "sql_type", "type", "expression", "source_sql_type", "not_null", "identity"  # only columns
    ]
    order_lookup = {key: (i, key) for i, key in enumerate(preferred_order)}
    max_index = len(preferred_order)

    def sort_key(item):
        key, value = item
        return order_lookup.get(key, (max_index, key))

    return sort_key


def save_table_design(source_dir: str, source_table_name: TableName, target_table_name: TableName,
                      table_design: dict, overwrite=False, dry_run=False) -> None:
    """
    Write new table design file to disk.

    Although these files are generated by computers, they get read by humans. So we try to be nice
    here and write them out with a specific order of the keys, like have name and description towards
    the top.
    """
    # Validate before writing to make sure we don't drift between bootstrap and JSON schema.
    etl.design.load.validate_table_design(table_design, target_table_name)

    # FIXME Move this logic into file sets (note that "source_name" is in table_design)
    filename = os.path.join(source_dir, "{}-{}.yaml".format(source_table_name.schema, source_table_name.table))
    this_table = target_table_name.identifier
    if dry_run:
        logger.info("Dry-run: Skipping writing new table design file for '%s'", this_table)
    elif os.path.exists(filename) and not overwrite:
        logger.warning("Skipping writing new table design for '%s' since '%s' already exists", this_table, filename)
    else:
        logger.info("Writing new table design file for '%s' to '%s'", this_table, filename)
        # We use JSON pretty printing because it is prettier than YAML printing.
        with open(filename, 'w') as o:
            json.dump(table_design, o, indent="    ", item_sort_key=make_item_sorter())
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


def create_table_designs_from_source(source, selector, local_dir, local_files, dry_run=False):
    """
    Create table design files for tables from a single source to local directory.
    Whenever some table designs already exist locally, validate them against the information found from upstream.
    """
    source_dir = os.path.join(local_dir, source.name)
    normalize_and_create(source_dir, dry_run=dry_run)

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
                    table_design = create_table_design_for_source(conn, source_table_name, target_table_name)
                    save_table_design(source_dir, source_table_name, target_table_name, table_design, dry_run=dry_run)
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


def bootstrap_sources(schemas, selector, table_design_dir, local_files, dry_run=False):
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
                total += create_table_designs_from_source(schema, selector, table_design_dir, local_files,
                                                          dry_run=dry_run)
    if not total:
        logger.warning("Found no matching tables in any upstream source for '%s'", selector)


def bootstrap_transformations(dsn_etl, schemas, local_dir, local_files, as_view,
                              update=False, replace=False, dry_run=False):
    """
    Download design information for transformations by test-running them in the data warehouse.
    """
    transformation_schema = {schema.name for schema in schemas if schema.has_transformations}
    transforms = [file_set for file_set in local_files if file_set.source_name in transformation_schema]
    if not (update or replace):
        transforms = [file_set for file_set in transforms if not file_set.design_file_name]
    if not transforms:
        logger.warning("Found no new queries without matching design files")
        return
    relations = [RelationDescription(file_set) for file_set in transforms]
    if update:
        # Unfortunately, this adds warnings about any of the upstream sources being unknown.
        relations = etl.relation.order_by_dependencies(relations)

    if as_view:
        create_func = create_table_design_for_view
    else:
        create_func = create_table_design_for_ctas

    with closing(etl.pg.connection(dsn_etl, autocommit=True)) as conn:
        for relation in relations:
            with relation.matching_temporary_view(conn) as tmp_view_name:
                table_design = create_func(conn, tmp_view_name, relation, update)
                source_dir = os.path.join(local_dir, relation.source_name)
                save_table_design(source_dir, relation.target_table_name, relation.target_table_name, table_design,
                                  overwrite=update or replace, dry_run=dry_run)
