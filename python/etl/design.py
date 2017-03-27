"""
Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.

Table designs are dictionaries of dictionaries or lists etc.
"""

from collections import namedtuple
from contextlib import closing
import logging
import os.path
import re

import jsonschema
import simplejson as json
import yaml
import yaml.parser

import etl
import etl.config
import etl.pg
import etl.file_sets
import etl.s3
from etl.relation import RelationDescription


class MissingMappingError(etl.ETLError):
    """Exception when an attribute type's target type is unknown"""
    pass


class TableDesignError(etl.ETLError):
    """Exception when a table design file is incorrect"""
    pass


class TableDesignParseError(TableDesignError):
    """Exception when a table design file cannot be parsed"""
    pass


class TableDesignValidationError(TableDesignError):
    """Exception when a table design file does not pass schema validation"""
    pass


class TableDesignSemanticError(TableDesignError):
    """Exception when a table design file does not pass logic checks"""
    pass


class ColumnDefinition(namedtuple("_ColumnDefinition",
                                  ["name",  # always
                                   "type", "sql_type",  # always for tables
                                   "source_sql_type", "expression", "not_null", "references"  # optional
                                   ])):
    """
    Wrapper for column attributes ... describes columns by name, type (e.g. for Avro), sql_type.
    """
    __slots__ = ()


def fetch_tables(cx, source, selector):
    """
    Retrieve all tables that match the given list of tables (which look like
    schema.name or schema.*) and return them as a list of TableName instances.

    The first list of patterns defines all tables ever accessible, the
    second list allows to exclude lists from consideration and finally the
    table pattern allows to select specific tables (probably via command line args).
    """
    logger = logging.getLogger(__name__)
    # Look for 'r'elations (ordinary tables), 'm'aterialized views, and 'v'iews in the catalog.
    result = etl.pg.query(cx, """SELECT nsp.nspname AS "schema"
                                      , cls.relname AS "table"
                                   FROM pg_catalog.pg_class cls
                                   JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                                  WHERE cls.relname NOT LIKE 'tmp%%'
                                        AND cls.relname NOT LIKE 'pg_%%'
                                        AND cls.relkind IN ('r', 'm', 'v')
                                  ORDER BY nsp.nspname, cls.relname""")
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


def fetch_columns(cx, table_name):
    """
    Retrieve table definition (column names and types).
    """
    # TODO Multiple indices lead to multiple rows per attribute when using join with pg_index
    ddl = etl.pg.query(cx, """SELECT a.attname AS attribute
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
                               ORDER BY a.attnum""",
                       (table_name.schema, table_name.table))
    logging.getLogger(__name__).info("Found %d column(s) in table '%s'", len(ddl), table_name.identifier)
    return ddl


def map_types_in_ddl(table_name, columns, as_is_att_type, cast_needed_att_type):
    """"
    Replace unsupported column types by supported ones and determine casting
    spell (for a single table).

    Result for every table is a "column definition", which is basically a list
    of tuples with name, old type, new type, expression information (where
    the expression within a SELECT will return the value of the attribute with
    the "new" type), serialization type, and not null constraint (boolean).
    """
    new_columns = []
    for column in columns:
        attribute_name = column["attribute"]
        attribute_type = column["attribute_type"]
        for re_att_type, generic_type in as_is_att_type.items():
            if re.match('^' + re_att_type + '$', attribute_type):
                # Keep the type, use no expression, and pick generic type from map.
                mapping_sql_type, mapping_expression, mapping_type = attribute_type, None, generic_type
                break
        else:
            for re_att_type, (mapping_sql_type, mapping_expression, mapping_type) in cast_needed_att_type.items():
                if re.match(re_att_type, attribute_type):
                    # Found tuple with new SQL type, expression and generic type.  Rejoice.
                    break
            else:
                raise MissingMappingError("Unknown type '{}' of {}.{}.{}".format(attribute_type,
                                                                                 table_name.schema,
                                                                                 table_name.table,
                                                                                 attribute_name))
        delimited_name = '"{}"'.format(attribute_name)
        new_columns.append(ColumnDefinition(name=attribute_name,
                                            source_sql_type=attribute_type,
                                            sql_type=mapping_sql_type,
                                            # Replace %s in the column expression by the column name.
                                            expression=(mapping_expression % delimited_name
                                                        if mapping_expression else None),
                                            type=mapping_type,
                                            not_null=column["not_null_constraint"],
                                            references=None))
    return new_columns


def create_table_design(source_table_name, target_table_name, columns):
    """
    Create (and return) new table design from column definitions.
    """
    table_design = {
        "name": "%s" % target_table_name.identifier,
        "source_name": "%s.%s" % (target_table_name.schema, source_table_name.identifier),
        "columns": [column._asdict() for column in columns]
    }
    # FIXME Extract actual primary keys from pg_catalog, add unique constraints
    if any(column.name == "id" for column in columns):
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
    return validate_table_design(table_design, target_table_name)


def validate_table_design_from_file(local_filename, table_name):
    logger = logging.getLogger(__name__)
    logger.debug("Loading local table design from '%s'", local_filename)
    try:
        with open(local_filename) as f:
            table_design = load_table_design(f, table_name)
    except:
        logger.error("Failed to load table design from '%s'", local_filename)
        raise
    return table_design


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not right.

    Phase 1 of validation is based on a schema and json-schema validation.
    Phase 2 is built on specific rules that I couldn't figure out how
    to run inside json-schema.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Trying to validate table design for '%s'", table_name.identifier)
    validate_table_design_syntax(table_design, table_name)
    validate_table_design_semantics(table_design, table_name)
    logger.debug("Validated table design for '%s'", table_name.identifier)
    return table_design


def validate_table_design_syntax(table_design, table_name):
    """
    Validate table design based on the (JSON) schema (which can only check syntax but not values).
    Raise an exception if anything is amiss.
    """
    logger = logging.getLogger(__name__)
    validation_internal_errors = (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
        json.scanner.JSONDecodeError)
    # Two things can break here: reading the schema, which is validated, and then reading the table design.
    try:
        table_design_schema = etl.config.load_json("table_design.schema")
    except validation_internal_errors:
        logger.critical("Internal Error: Schema in 'table_design.schema' is not valid")
        raise
    try:
        jsonschema.validate(table_design, table_design_schema)
    except validation_internal_errors as exc:
        logger.error("Failed to validate table design for '%s'", table_name.identifier)
        raise TableDesignValidationError() from exc


def validate_table_design_semantics(table_design, table_name):
    """
    Validate table design against rule set based on values (e.g. name of columns).
    Raise an exception if anything is amiss.
    """
    if table_design["name"] != table_name.identifier:
        raise TableDesignSemanticError("Name of table (%s) must match target (%s)" % (table_design["name"],
                                                                                      table_name.identifier))

    # VIEW validation only requires check that columns have nothing more than the name information
    if table_design["source_name"] == "VIEW":
        for column in table_design["columns"]:
            if len(column) != 1:
                raise TableDesignSemanticError("Too much information for column of a VIEW: %s" % list(column))
        return

    # Designs for physical tables need further validation for columns:
    for column in table_design["columns"]:
        if column.get("skipped", False):
            continue
        if column.get("identity", False) and not column.get("not_null", False):
            # NULL columns may not be primary key (identity)
            raise TableDesignSemanticError("identity column must be set to not null")

    identity_columns = [column["name"] for column in table_design["columns"] if column.get("identity", False)]
    if len(identity_columns) > 1:
        raise TableDesignSemanticError("only one column should have identity")
    surrogate_keys = table_design.get("constraints", {}).get("surrogate_key", [])
    if len(surrogate_keys) and not surrogate_keys == identity_columns:
        raise TableDesignSemanticError("surrogate key must be identity")

    # Make sure that whenever we reference a column that that column is actually part of the table's columns
    column_set = frozenset(column["name"] for column in table_design["columns"])
    column_list_references = [
        ('constraints', 'primary_key'),
        ('constraints', 'natural_key'),
        ('constraints', 'surrogate_key'),
        ('constraints', 'foreign_key'),
        ('constraints', 'unique'),
        ('attributes', 'interleaved_sort'),
        ('attributes', 'compound_sort')
    ]
    invalid_col_list_template = "{obj}'s {key} list should only contain named columns but it does not"
    for obj, key in column_list_references:
        if not column_list_has_columns(column_set, table_design.get(obj, {}).get(key)):
            raise TableDesignSemanticError(invalid_col_list_template.format(obj=obj, key=key))


def column_list_has_columns(valid_columns, candidate_columns):
    """
    Accepts a set of known columns and a list of strings that may be columns (or a string that should be a column)
    Returns True if the possible column list is found within column_set and False otherwise

    >>> column_list_has_columns({'a'}, 'a')
    True
    >>> column_list_has_columns({'fish'}, 'a')
    False
    >>> column_list_has_columns({'a', 'b'}, ['b', 'a'])
    True
    >>> column_list_has_columns({'a', 'b'}, ['b', 'c'])
    False
    """
    if not candidate_columns:
        return True
    if not isinstance(candidate_columns, list):
        candidate_columns = [candidate_columns]
    for column in candidate_columns:
        if column not in valid_columns:
            return False
    return True


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file). The table design is
    validated before being returned.
    """
    try:
        table_design = yaml.safe_load(stream)
    except yaml.parser.ParserError as exc:
        raise TableDesignParseError() from exc
    return validate_table_design(table_design, table_name)


def download_table_design(bucket_name, design_file, table_name):
    """
    Download table design from file in S3.
    """
    with closing(etl.s3.get_s3_object_content(bucket_name, design_file)) as content:
        table_design = load_table_design(content, table_name)
    return table_design


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


def create_or_update_table_designs_from_source(source, selector, local_dir, local_files, type_maps, dry_run=False):
    """
    Dump schemas (table design files) for tables from a single source to local directory

    Whenever some table designs already exist locally, validate them against the information found from upstream.
    """
    logger = logging.getLogger(__name__)
    source_files = {file_set.source_table_name: file_set
                    for file_set in local_files if file_set.source_name == source.name}
    try:
        logger.info("Connecting to database '%s' to look for tables", source.name)
        with closing(etl.pg.connection(source.dsn, autocommit=True, readonly=True)) as conn:
            source_tables = fetch_tables(conn, source, selector)
            for source_table_name in sorted(source_tables):
                source_columns = fetch_columns(conn, source_table_name)
                target_columns = map_types_in_ddl(source_table_name,
                                                  source_columns,
                                                  type_maps["as_is_att_type"],
                                                  type_maps["cast_needed_att_type"])
                target_table_name = etl.TableName(source.name, source_table_name.table)
                table_design = create_table_design(source_table_name, target_table_name, target_columns)

                source_file_set = source_files.get(source_table_name)
                if source_file_set and source_file_set.design_file_name:
                    # Replace bootstrapped table design with one from file but check whether set of columns changed.
                    design_file = source_file_set.design_file_name
                    existing_table_design = validate_table_design_from_file(design_file, target_table_name)
                    compare_columns(table_design, existing_table_design)
                else:
                    # TODO In case there's a local SQL file, that name should be used as basis for new table design
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


def bootstrap_views(local_files, schemas, dry_run=False):
    """
    When running design --auto, newly authored transformations need to be run as views to auto-generate design files
    """
    logger = logging.getLogger(__name__)
    created = []
    for schema in schemas:
        for file in local_files:
            if file.source_name != schema.name or file.design_file_name:
                continue
            # TODO Pull out the connection so that we don't open it per table but per schema
            with closing(etl.pg.connection(schema.dsn, autocommit=True)) as conn:
                description = RelationDescription(file)
                logger.info("Creating view for '%s' which has no design file", file.target_table_name.identifier)
                ddl_stmt = """CREATE OR REPLACE VIEW {} AS\n{}""".format(file.target_table_name, description.query_stmt)
                if dry_run:
                    logger.info('Dry-run: skipping view creation')
                else:
                    etl.pg.execute(conn, ddl_stmt)
                created.append(file)
    return created


def cleanup_views(created, schemas, dry_run=False):
    """
    When running design --auto, views created for inspection should be dropped afterwards
    """
    logger = logging.getLogger(__name__)
    for schema in schemas:
        for file in created:
            if file.source_name != schema.name:
                continue
            with closing(etl.pg.connection(schema.dsn, autocommit=True)) as conn:
                ddl_stmt = """DROP VIEW IF EXISTS {}""".format(file.target_table_name)
                logger.info("Dropping view for '%s'", file.target_table_name.identifier)
                if dry_run:
                    logger.info('Dry-run: skipping view deletion')
                else:
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


def download_schemas(schemas, selector, table_design_dir, local_files, type_maps, auto=False, dry_run=False):
    """
    Download schemas from database tables and compare against local design files (if available).
    Unless in auto mode, ignore non-source schemas.
    """
    logger = logging.getLogger(__name__)
    total = 0
    for schema in schemas:
        if not schema.has_dsn:
            logger.info("Skipping static source schema or unload target in S3: '%s'", schema.name)
        elif not auto and not schema.is_database_source:
            logger.info("Skipping non-source database schema: '%s'", schema.name)
        elif selector.match_schema(schema.name):
            normalize_and_create(os.path.join(table_design_dir, schema.name), dry_run=dry_run)
            total += create_or_update_table_designs_from_source(schema, selector, table_design_dir, local_files,
                                                                type_maps, dry_run=dry_run)
    if not local_files:
        logger.warning("Found no matching files in '%s' for '%s'", table_design_dir, selector)
    if not total:
        logger.warning("Found no matching table in any upstream source for '%s'", selector)
