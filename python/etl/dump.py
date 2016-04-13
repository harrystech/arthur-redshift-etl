"""
Functions to deal with dumping data from PostgreSQL databases to CSV along with "table designs".

Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.
"""

from datetime import datetime
from fnmatch import fnmatch
import gzip
import logging
import os
import os.path
import re

import psycopg2
from psycopg2 import errorcodes
import simplejson as json

import etl
import etl.load
import etl.pg
import etl.s3


# List of options for COPY statement that dictates CSV format
CSV_WRITE_FORMAT = "FORMAT CSV, HEADER true, NULL '\\N'"

# How to create an "id" column that may be a primary key if none exists in the table
ID_EXPRESSION = "row_number() OVER()"

# Total number of header lines written
N_HEADER_LINES = 3


class MissingMappingError(ValueError):
    """Exception when an attribute type's target type is unknown"""
    pass


def fetch_tables(cx, table_whitelist, table_blacklist, pattern):
    """
    Retrieve all tables that match the given list of tables (which look like
    schema.name or schema.*) and return them as a list of (schema, table) tuples.

    The first list of patterns defines all tables ever accessible, the
    second list allows to exclude lists from consideration and finally the
    table pattern allows to select specific tables (via command line args).
    """
    # Look for 'r'elations and 'm'aterialized views in the catalog.
    found = etl.pg.query(cx, """SELECT nsp.nspname AS "schema"
                                     , cls.relname AS "table"
                                     , nsp.nspname || '.' || cls.relname AS "table_name"
                                  FROM pg_catalog.pg_class cls
                                  JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                                 WHERE cls.relname NOT LIKE 'tmp%%'
                                   AND cls.relkind IN ('r', 'm')
                                 ORDER BY nsp.nspname, cls.relname""")
    tables = []
    for row in found:
        for reject_pattern in table_blacklist:
            if fnmatch(row['table_name'], reject_pattern):
                break
        else:
            for accept_pattern in table_whitelist:
                if fnmatch(row['table_name'], accept_pattern):
                    if pattern.match_table(row['table']):
                        tables.append(etl.TableName(row['schema'], row['table']))
                        break
    logging.getLogger(__name__).info("Found %d table(s) matching patterns; whitelist=%s, blacklist=%s, subset='%s'",
                                     len(tables), table_whitelist, table_blacklist, pattern)
    return tables


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
    spell.

    Result for every table is a list of tuples with name, old type, new type,
    expression information (where the expression within a SELECT will return
    the value of the attribute with the "new" type), serialization type, and
    not null constraint (boolean).

    If the original table definition is missing an "id" column, then one is
    added in the target definition.  The type of the original column is set
    to "none" in this case.
    """
    new_columns = []
    found_id = False
    for column in columns:
        attribute_name = column["attribute"]
        attribute_type = column["attribute_type"]
        if attribute_name == "id":
            found_id = True
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
                                                not_null=column["not_null_constraint"]))
    if not found_id:
        new_columns.insert(0, etl.ColumnDefinition(name="id",
                                                   source_sql_type="none",
                                                   sql_type="bigint",
                                                   expression=ID_EXPRESSION,
                                                   type="long",
                                                   not_null=True))
    return new_columns


def create_table_design(source_name, source_table_name, table_name, columns):
    """
    Create (and return) new table design from column definitions.
    """
    table_design = {
        "name": "%s" % table_name.identifier,
        "source_name": "%s.%s" % (source_name, source_table_name.identifier),
        "columns": [column._asdict() for column in columns],
        "constraints": {"primary_key": ["id"]},
        "attributes": {
            "distribution": "even",
            "compound_sort": ["id"]
        }
    }
    # Remove empty expressions (since columns can be selected by name) and default settings
    for column in table_design["columns"]:
        if column["expression"] is None:
            del column["expression"]
        if column["source_sql_type"] == column["sql_type"]:
            del column["source_sql_type"]
        if not column["not_null"]:
            del column["not_null"]
    # Make sure schema and code to create table design files is in sync.
    return etl.load.validate_table_design(table_design, table_name)


def save_table_design(table_design, table_name, output_dir, dry_run=False):
    """
    Write new table design file to local disk.
    """
    filename = os.path.join(output_dir, "{}-{}.yaml".format(table_name.schema, table_name.table))
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping writing new table design file for '%s'", table_name.identifier)
    elif os.path.exists(filename):
        logger.warning("Skipping new table design for '%s' since '%s' already exists", table_name.identifier, filename)
    else:
        logger.info("Writing new table design file for '%s' to '%s'", table_name.identifier, filename)
        with open(filename, 'w') as o:
            # JSON pretty printing is prettier than YAML printing.
            json.dump(table_design, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.info("Completed writing '%s'", filename)
    return filename


def create_copy_statement(table_design, source_table_name, row_limit=None):
    """
    Assemble COPY statement that will extract attributes with their new types
    """
    select_column = []
    for column in table_design["columns"]:
        # This is either an expression with cast or function or an as-is. Either way, make sure name is delimited.
        if column.get("expression"):
            select_column.append(column["expression"] + ' AS "%s"' % column["name"])
        else:
            select_column.append('"%s"' % column["name"])
    if row_limit:
        limit = "LIMIT {:d}".format(row_limit)
    else:
        limit = ""
    return "COPY (SELECT {}\n    FROM {}\n{}) TO STDOUT WITH ({})".format(",\n    ".join(select_column),
                                                                          source_table_name,
                                                                          limit,
                                                                          CSV_WRITE_FORMAT)


def download_table_data(cx, table_design, source_table_name, table_name, output_dir,
                        limit=None, overwrite=False, dry_run=False):
    """
    Download data (with casts for columns as needed) and compress output files.
    Return filename (if file was successfully created).

    This will skip writing files if they already exist, allowing easy re-starts.

    There are three header lines (timestamp, copy options, column names).  They
    must be skipped when reading the CSV file into Redshift. See HEADER_LINES constant.
    """
    filename = os.path.join(output_dir, "{}-{}.csv.gz".format(source_table_name.schema, source_table_name.table))
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping writing CSV file for table '%s'", table_name.identifier)
    elif not overwrite and os.path.exists(filename):
        logger.warning("Skipping copy for table '%s' since '%s' already exists", table_name.identifier, filename)
    else:
        logger.info("Writing CSV data for table '%s' to '%s'", table_name.identifier, filename)
        try:
            with open(filename, 'wb') as f:
                with gzip.open(f, 'wt') as o:
                    o.write("# Timestamp: {:%Y-%m-%d %H:%M:%S}\n".format(datetime.now()))
                    if limit:
                        o.write("# Copy options with LIMIT {:d}: {}\n".format(limit, CSV_WRITE_FORMAT))
                    else:
                        o.write("# Copy options: {}\n".format(CSV_WRITE_FORMAT))
                    sql = create_copy_statement(table_design, source_table_name, limit)
                    logger.debug("Copy statement for '%s': %s", source_table_name.identifier, sql)
                    with cx.cursor() as cursor:
                        cursor.copy_expert(sql, o)
                    o.flush()
            logger.debug("Done writing CSV data to '%s'", filename)
        except (Exception, KeyboardInterrupt) as exc:
            logger.warning("Deleting '%s' because writing was interrupted", filename)
            os.remove(filename)
            if isinstance(exc, psycopg2.Error):
                etl.pg.log_sql_error(exc)
                if exc.pgcode == errorcodes.INSUFFICIENT_PRIVILEGE:
                    logger.warning("Ignoring denied access for table '%s'", source_table_name.identifier)
                    # Signal to S3 uploader that there isn't a file coming but that we handled the exception.
                    return None
            raise
    return filename


def download_table_data_bounded(semaphore, cx, table_design, source_table_name, table_name, output_dir,
                                limit=None, overwrite=False, dry_run=False):
    """
    "Bounded" version of a table download -- psycopg2 cannot run more than one
    copy operation at a time.  So we need a semaphore to switch between threads that
    want to use copy.
    """
    with semaphore:
        return download_table_data(cx, table_design, source_table_name, table_name, output_dir,
                                   limit=limit, overwrite=overwrite, dry_run=dry_run)
