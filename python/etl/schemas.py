"""
Deal with schemas, or more generally, table designs.

Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.
"""

from contextlib import closing
import logging
import os.path
import re

import jsonschema
import simplejson as json
import yaml

import etl
import etl.commands
import etl.config
import etl.dump
import etl.pg
import etl.s3


class MissingMappingError(ValueError):
    """Exception when an attribute type's target type is unknown"""
    pass


class TableDesignError(ValueError):
    """Exception when a table design file is incorrect"""
    pass


def fetch_tables(cx, table_whitelist, table_blacklist, pattern):
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
        table_name = etl.TableName(row['schema'], row['table'])
        for reject_pattern in table_blacklist:
            if table_name.match(reject_pattern):
                logger.debug("Table '%s' matches blacklist", table_name.identifier)
                break
        else:
            for accept_pattern in table_whitelist:
                if table_name.match(accept_pattern):
                    if pattern.match_table(table_name.table):
                        found.append(table_name)
                        logger.debug("Table '%s' is included in result set", table_name.identifier)
                        break
                    else:
                        logger.debug("Table '%s' matches whitelist but is not selected", table_name.identifier)
    logging.getLogger(__name__).info("Found %d table(s) matching patterns; whitelist=%s, blacklist=%s, subset='%s'",
                                     len(found), table_whitelist, table_blacklist, pattern)
    return found


def fetch_columns(cx, tables):
    """
    Retrieve table definitions (column names and types).
    """
    columns = {}
    for table_name in tables:
        ddl = etl.pg.query(cx, """SELECT ca.attname AS attribute,
                                         pg_catalog.format_type(ct.oid, ca.atttypmod) AS attribute_type,
                                         ct.typelem <> 0 AS is_array_type,
                                         pg_catalog.format_type(ce.oid, ca.atttypmod) AS element_type,
                                         ca.attnotnull AS not_null_constraint
                                    FROM pg_catalog.pg_attribute AS ca
                                    JOIN pg_catalog.pg_class AS cls ON ca.attrelid = cls.oid
                                    JOIN pg_catalog.pg_namespace AS ns ON cls.relnamespace = ns.oid
                                    JOIN pg_catalog.pg_type AS ct ON ca.atttypid = ct.oid
                                    LEFT JOIN pg_catalog.pg_type AS ce ON ct.typelem = ce.oid
                                   WHERE ca.attnum > 0  -- skip system columns
                                         AND NOT ca.attisdropped
                                         AND ns.nspname = %s
                                         AND cls.relname = %s
                                   ORDER BY ca.attnum""",
                           (table_name.schema, table_name.table))
        columns[table_name] = ddl
        logging.getLogger(__name__).info("Found %d column(s) in table '%s'", len(ddl), table_name.identifier)
    return columns


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
        for re_att_type, avro_type in as_is_att_type.items():
            if re.match('^' + re_att_type + '$', attribute_type):
                # Keep the type, use no expression, and pick Avro type from map.
                mapping = (attribute_type, None, avro_type)
                break
        else:
            for re_att_type, mapping in cast_needed_att_type.items():
                if re.match(re_att_type, attribute_type):
                    # Found tuple with new SQL type, expression and Avro type.  Rejoice.
                    break
            else:
                raise MissingMappingError("Unknown type '{}' of {}.{}.{}".format(attribute_type,
                                                                                 table_name.schema,
                                                                                 table_name.table,
                                                                                 attribute_name))
        delimited_name = '"%s"' % attribute_name
        new_columns.append(etl.ColumnDefinition(name=attribute_name,
                                                source_sql_type=attribute_type,
                                                sql_type=mapping[0],
                                                # Replace %s in the column expression by the column name.
                                                expression=(mapping[1] % delimited_name if mapping[1] else None),
                                                # Add "null" to Avro type if column may have nulls.
                                                type=(mapping[2] if column["not_null_constraint"]
                                                      else ["null", mapping[2]]),
                                                not_null=column["not_null_constraint"],
                                                references=None))
    return new_columns


def create_table_design(source_name, source_table_name, table_name, columns):
    """
    Create (and return) new table design from column definitions.
    """
    table_design = {
        "name": "%s" % table_name.identifier,
        "source_name": "%s.%s" % (source_name, source_table_name.identifier),
        "columns": [column._asdict() for column in columns]
    }
    # TODO Extract actual primary keys from pg_catalog
    if any(column.name == "id" for column in columns):
        table_design["constraints"] = {"primary_key": ["id"]}
    # Remove empty expressions (since columns can be selected by name) and default settings
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
    return validate_table_design(table_design, table_name)


def validate_table_design(table_design, table_name):
    """
    Validate table design against schema.  Raise exception if anything is not
    right.

    Phase 1 of validation is based on a schema and json-schema validation.
    Phase 2 is built on specific rules that I couldn't figure out how
    to run inside json-schema.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Trying to validate table design for '%s' from stream", table_name.identifier)
    try:
        table_design_schema = etl.config.load_json("table_design.schema")
    except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError, json.scanner.JSONDecodeError):
        logger.critical("Internal Error: Schema in 'table_design.schema' is not valid")
        raise
    try:
        jsonschema.validate(table_design, table_design_schema)
    except (jsonschema.exceptions.ValidationError, jsonschema.exceptions.SchemaError, json.scanner.JSONDecodeError):
        logger.error("Failed to validate table design for '%s'!", table_name.identifier)
        raise
    # TODO Need more rules?
    if table_design["name"] != table_name.identifier:
        raise TableDesignError("Name of table (%s) must match target (%s)" % (table_design["name"], table_name.identifier))
    if table_design["source_name"] == "VIEW":
        for column in table_design["columns"]:
            if column.get("skipped", False):
                raise TableDesignError("columns may not be skipped in views")
    else:
        for column in table_design["columns"]:
            if column.get("skipped", False):
                continue
            if column.get("not_null", False):
                # NOT NULL columns -- may not have "null" as type
                if isinstance(column["type"], list) or column["type"] == "null":
                    raise TableDesignError('"not null" column may not have null type')
            else:
                # NULL columns -- must have "null" as type and may not be primary key (identity)
                if not (isinstance(column["type"], list) and "null" in column["type"]):
                    raise TableDesignError('"null" missing as type for null-able column')
                if column.get("identity", False):
                    raise TableDesignError("identity column must be set to not null")
        identity_columns = [column["name"] for column in table_design["columns"] if column.get("identity", False)]
        if len(identity_columns) > 1:
            raise TableDesignError("only one column should have identity")
        surrogate_keys = table_design.get("constraints", {}).get("surrogate_key", [])
        if len(surrogate_keys) and not surrogate_keys == identity_columns:
            raise TableDesignError("surrogate key must be identity")
    return table_design


def load_table_design(stream, table_name):
    """
    Load table design from stream (usually, an open file). The table design is
    validated before being returned.
    """
    table_design = yaml.safe_load(stream)
    return validate_table_design(table_design, table_name)


def save_table_design(table_design, source_table_name, source_output_dir, dry_run=False):
    """
    Write new table design file to etc disk.
    """
    table = table_design["name"]
    filename = os.path.join(source_output_dir, "{}-{}.yaml".format(source_table_name.schema, source_table_name.table))
    logger = logging.getLogger(__name__)
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
        logger.info("Completed writing '%s'", filename)
    return filename


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


def compare_columns(live_design, file_design):
    logger = logging.getLogger(__name__)
    logger.info("Checking design for %s", live_design["name"])
    live_columns = {column["name"] for column in live_design["columns"]}
    file_columns = {column["name"] for column in file_design["columns"]}
    # TODO define policy to declare columns "ETL-only"
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


def create_or_update_table_designs(source, table_design_files, type_maps, design_dir, selection, dry_run=False):
    """
    Dump schemas (table design files) for tables from a single source to local directory
    """
    logger = logging.getLogger(__name__)
    source_name = source["name"]
    found = set()
    try:
        design_dir = normalize_and_create(os.path.join(design_dir, source_name), dry_run=dry_run)
        logger.info("Connecting to source database '%s'", source_name)
        dsn = etl.config.env_value(source["read_access"])
        with closing(etl.pg.connection(dsn, autocommit=True, readonly=True)) as conn:
            tables = fetch_tables(conn, source["include_tables"], source.get("exclude_tables", []), selection)
            columns_by_table = fetch_columns(conn, tables)
            for source_table_name in sorted(columns_by_table):
                target_table_name = etl.TableName(source_name, source_table_name.table)
                found.add(source_table_name)
                columns = map_types_in_ddl(source_table_name,
                                           columns_by_table[source_table_name],
                                           type_maps["as_is_att_type"],
                                           type_maps["cast_needed_att_type"])
                table_design = create_table_design(source_name, source_table_name, target_table_name, columns)

                if source_table_name in table_design_files:
                    # Replace bootstrapped table design with one from file but check whether set of columns changed.
                    design_file = table_design_files[source_table_name]
                    with open(design_file) as f:
                        existing_table_design = load_table_design(f, target_table_name)
                    compare_columns(table_design, existing_table_design)
                else:
                    save_table_design(table_design, source_table_name, design_dir, dry_run=dry_run)
    except Exception:
        logger.exception("Error while processing source '%s':", source_name)
        raise
    not_found = found.difference(set(table_design_files))
    if len(not_found):
        logger.warning("New table(s) in '%s' which had no design: %s", source_name,
                       sorted(table.identifier for table in not_found))
    too_many = set(table_design_files).difference(found)
    if len(too_many):
        logger.warning("Table design files without tables: %s", sorted(table.identifier for table in too_many))
    logger.info("Done with %d table(s) from source '%s'", len(found), source_name)


def download_schemas(settings, table, table_design_dir, dry_run):
    logger = logging.getLogger(__name__)
    selection = etl.TableNamePatterns.from_list(table)
    sources = [dict(source) for source in settings("sources") if selection.match_schema(source["name"])]
    schemas = [source["name"] for source in sources]
    local_files = etl.s3.find_local_files(table_design_dir, schemas, selection)

    # Check that all env vars are set--it's annoying to have this fail for the last source without upfront warning.
    for source in sources:
        if "read_access" in source and source["read_access"] not in os.environ:
                raise KeyError("Environment variable not set: %s" % source["read_access"])

    for source, source_name in zip(sources, schemas):
        if "read_access" not in source:
            logger.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
            continue
        table_design_files = {assoc_files.source_table_name: assoc_files.design_file
                              for assoc_files in local_files.get(source_name, {})}
        create_or_update_table_designs(source, table_design_files, settings("type_maps"),
                                       table_design_dir, selection, dry_run=dry_run)


def copy_to_s3(settings, table, table_design_dir, prefix, force=False, dry_run=True):
    """
    Copy table design and SQL files from local directory to S3 bucket.

    Essentially "publishes" data-warehouse code.
    """
    logger = logging.getLogger(__name__)
    selection = etl.TableNamePatterns.from_list(table)
    sources = selection.match_field(settings("sources"), "name")
    schemas = [source["name"] for source in sources]

    local_files = etl.s3.find_local_files(table_design_dir, schemas, selection)
    if not local_files:
        logger.error("No applicable files found in '%s' for '%s'", table_design_dir, selection)
        return

    bucket_name = settings("s3", "bucket_name")
    if force:
        etl.s3.delete_files_in_bucket(bucket_name, prefix, schemas, selection, dry_run=dry_run)

    for source_name in local_files:
        for assoc_table_files in local_files[source_name]:
            target_table_name = assoc_table_files.target_table_name
            with open(assoc_table_files.design_file, 'r') as design_file:
                # Always validate before uploading table design
                load_table_design(design_file, target_table_name)
            for local_filename in (assoc_table_files.design_file, assoc_table_files.sql_file):
                if local_filename is not None:
                    source_prefix = "{}/schemas/{}".format(prefix, source_name)
                    etl.s3.upload_to_s3(local_filename, bucket_name, source_prefix, dry_run=dry_run)
    if not dry_run:
        logger.info("Uploaded all files to 's3://%s/%s/'", bucket_name, prefix)


def validate_designs(settings, target, table_design_dir):
    """
    Make sure that all (local) table design files pass the validation checks.
    """
    logger = logging.getLogger(__name__)
    selection = etl.TableNamePatterns.from_list(target)
    sources = selection.match_field(settings("sources"), "name")
    schemas = [source["name"] for source in sources]

    found = etl.s3.find_local_files(table_design_dir, schemas, selection)
    if not found:
        logger.error("No applicable files found in '%s' for '%s'", table_design_dir, selection)
        return

    for source in found:
        for info in found[source]:
            logger.info("Checking file '%s'", info.design_file)
            with open(info.design_file, 'r') as design_file:
                table_design = load_table_design(design_file, info.target_table_name)
                logger.debug("Validated table design for '%s'", table_design["name"])
