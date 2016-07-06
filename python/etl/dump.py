"""
Functions to deal with dumping data from PostgreSQL databases to CSV along with "table designs".

Table designs describe the columns, like their name and type, as well as how the data
should be organized once loaded into Redshift, like the distribution style or sort key.
"""

import concurrent.futures
from datetime import datetime
from fnmatch import fnmatch
import gzip
import logging
import os
import os.path
import re

from pyspark import SparkConf, SparkContext, SQLContext

import psycopg2
from psycopg2 import errorcodes
import simplejson as json

import etl
import etl.config
import etl.load
import etl.pg
import etl.s3


# List of options for COPY statement that dictates CSV format
CSV_WRITE_FORMAT = "FORMAT CSV, HEADER true, NULL '\\N'"

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
    logger = logging.getLogger(__name__)
    # Look for 'r'elations (ordinary tables), 'm'aterialized views, and 'v'iews in the catalog.
    found = etl.pg.query(cx, """SELECT nsp.nspname AS "schema"
                                     , cls.relname AS "table"
                                     , nsp.nspname || '.' || cls.relname AS "table_name"
                                  FROM pg_catalog.pg_class cls
                                  JOIN pg_catalog.pg_namespace nsp ON cls.relnamespace = nsp.oid
                                 WHERE cls.relname NOT LIKE 'tmp%%'
                                   AND cls.relkind IN ('r', 'm', 'v')
                                 ORDER BY nsp.nspname, cls.relname""")
    tables = []
    for row in found:
        for reject_pattern in table_blacklist:
            if fnmatch(row['table_name'], reject_pattern):
                logger.debug("Table '%s' matches blacklist", row['table_name'])
                break
        else:
            for accept_pattern in table_whitelist:
                if fnmatch(row['table_name'], accept_pattern):
                    if pattern.match_table(row['table']):
                        tables.append(etl.TableName(row['schema'], row['table']))
                        logger.debug("Table '%s' is included in result set", row['table_name'])
                        break
                    else:
                        logger.debug("Table '%s' matches whitelist but is not selected", row['table_name'])
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
    logging.getLogger(__name__).debug("Trying to validate new table design for '%s'", table_name.identifier)
    return etl.load.validate_table_design(table_design, table_name)


def save_table_design(table_design, source_table_name, output_dir, dry_run=False):
    """
    Write new table design file to etc disk.
    """
    table = table_design["name"]
    filename = os.path.join(output_dir, "{}-{}.yaml".format(source_table_name.schema, source_table_name.table))
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping writing new table design file for '%s'", table)
    elif os.path.exists(filename):
        logger.warning("Skipping new table design for '%s' since '%s' already exists", table, filename)
    else:
        logger.info("Writing new table design file for '%s' to '%s'", table, filename)
        with open(filename, 'w') as o:
            # JSON pretty printing is prettier than YAML printing.
            json.dump(table_design, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.info("Completed writing '%s'", filename)
    return filename


def assemble_selected_columns(table_design):
    """
    Pick columns and decide how they are selected (as-is or with an expression).
    """
    selected_columns = []
    for column in table_design["columns"]:
        if column.get("skipped", False):
            continue
        elif column.get("expression"):
            selected_columns.append(column["expression"] + ' AS "%s"' % column["name"])
        else:
            selected_columns.append('"%s"' % column["name"])
    return selected_columns


def create_copy_statement(table_design, source_table_name, row_limit=None):
    """
    Assemble COPY statement that will extract attributes with their new types.

    If there's an expression, then it needs to cast it to the correct column
    type. Whether there's an expression or just a name the resulting column is always
    called out delimited.
    """
    selected_columns = assemble_selected_columns(table_design)
    if row_limit:
        limit = "LIMIT {:d}".format(row_limit)
    else:
        limit = ""
    return "COPY (SELECT {}\n    FROM {}\n{}) TO STDOUT WITH ({})".format(",\n    ".join(selected_columns),
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


def dump_source_to_s3(sqlContext, source, tables_in_s3, size_map, bucket_name, prefix, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    source_name = source["name"]
    read_access = etl.config.env_value(source["read_access"])
    dsn_properties = etl.pg.parse_connection_string(read_access)
    dsn_properties.update({
        "ApplicationName": "DataWarehouseETL",
        "readOnly": "true",
        "driver": "org.postgresql.Driver"  # necessary, weirdly enough
    })
    jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(**dsn_properties)
    base_path = "s3n://{}/{}/{}".format(bucket_name, prefix, "data")

    copied = set()
    for assoc_table_files in tables_in_s3:
        source_table_name = assoc_table_files.source_table_name
        target_table_name = assoc_table_files.target_table_name
        design_file = etl.s3.get_file_content(bucket_name, assoc_table_files.design_file)
        table_design = etl.load.load_table_design(design_file, target_table_name)

        num_partitions = 1
        selected_columns = assemble_selected_columns(table_design)
        select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
        logging.debug("Table query: SELECT * FROM %s", select_statement)

        # Search the primary key (look also for distribution by key!)
        if "constraints" in table_design and "primary_key" in table_design["constraints"]:
            primary_key = table_design["constraints"]["primary_key"][0]
            logging.debug("Primary key = %s", primary_key)
            partition_df = sqlContext.read.jdbc(url=jdbc_url,
                                                properties=dsn_properties,
                                                table="""(SELECT min({}) AS lower_bound
                                                               , max({}) AS upper_bound
                                                               , count(*) AS row_count
                                                            FROM {}) AS t""".format(primary_key,
                                                                                    primary_key,
                                                                                    source_table_name))
            rows = partition_df.collect()
            assert len(rows) == 1
            lower_bound, upper_bound, row_count = rows[0]

            for low in sorted(size_map):
                if row_count >= low:
                    num_partitions = size_map[low]

            logging.info("Picked %d partitions for table '%s' (lower: %d, upper: %d, count: %d)",
                         num_partitions,
                         source_table_name.identifier,
                         lower_bound,
                         upper_bound,
                         row_count)

            df = sqlContext.read.jdbc(url=jdbc_url,
                                      properties=dsn_properties,
                                      table=select_statement,
                                      column=primary_key,
                                      lowerBound=lower_bound,
                                      upperBound=upper_bound,
                                      numPartitions=num_partitions)

        #                          predicates=[
        #                              "id >= 7253011 AND id < 7683410",
        #                              "id >= 8113722 AND id < 8544102",
        #                              "id >= 8544133 AND id < 8974448",
        #                              "id >= 8974444 AND id < 9404794",
        #                          ]

        else:
            df = sqlContext.read.jdbc(url=jdbc_url,
                                      properties=dsn_properties,
                                      table=select_statement)

        path = os.path.join(base_path, assoc_table_files.source_path_name, 'csv')
        if dry_run:
            logging.info("Dry-run: Skipping upload to '%s'", path)
        else:
            logging.info("Writing table %s to '%s'", target_table_name.identifier, path)
            # etl.?.write_dataframe
            df.write \
                .format(source='com.databricks.spark.csv') \
                .option("header", "true") \
                .option("nullValue", r"\N") \
                .option("codec", "gzip") \
                .mode('overwrite') \
                .save(path)

        copied.add(target_table_name)
    logging.info("Done with %d table(s) from source '%s'", len(copied), source_name)
    return


def dump_to_s3(args, settings):
    bucket_name = settings("s3", "bucket_name")
    selector = etl.TableNamePatterns.from_list(args.table)
    schemas = selector.match_names(settings("sources"))
    tables_in_s3 = etl.s3.find_files_in_bucket(bucket_name, args.prefix, schemas, selector)

    # Check that all env vars are set--it's annoying to have this fail for the last source without upfront warning.
    for source in settings("sources"):
        source_name = source["name"]
        if source_name in schemas and "read_access" in source:
            if source["read_access"] not in os.environ:
                raise KeyError("Environment variable to access '%s' not set: %s" % (source_name, source["read_access"]))

    conf = SparkConf()
    conf.setAppName("DataWarehouseETL")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    for source in settings("sources"):
        source_name = source["name"]
        if source_name not in schemas:
            continue
        if "read_access" not in source:
            logging.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
            continue
        if not tables_in_s3.get(source_name):
            logging.warning("No information found for source '%s' in s3://%s/%s", source_name, bucket_name, args.prefix)
            continue
        dump_source_to_s3(sqlContext, source, tables_in_s3[source_name], settings("dataframe", "partition_sizes"),
                          bucket_name, args.prefix, dry_run=args.dry_run)


def normalize_and_create(directory: str, dry_run=False) -> str:
    """
    Make sure the directory exists and return normalized path to it.

    This will create all intermediate directories as needed.
    """
    name = os.path.normpath(directory)
    if not os.path.exists(name):
        if dry_run:
            logging.debug("Skipping creation of directory '%s'", name)
        else:
            logging.debug("Creating directory '%s'", name)
            os.makedirs(name)
    return name


def dump_schema_to_s3(source, table_design_files, type_maps, design_dir, bucket_name, prefix, selection, dry_run=False):
    source_name = source["name"]
    read_access = source.get("read_access")
    design_dir = normalize_and_create(os.path.join(design_dir, source_name), dry_run=dry_run)
    source_prefix = "{}/schemas/{}".format(prefix, source_name)
    found = set()
    try:
        logging.info("Connecting to source database '%s'", source_name)
        with closing(etl.pg.connection(etl.config.env_value(read_access), autocommit=True, readonly=True)) as conn:
            tables = fetch_tables(conn, source["include_tables"], source.get("exclude_tables", []), selection)
            columns_by_table = fetch_columns(conn, tables)
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                for source_table_name in sorted(columns_by_table):
                    table_name = etl.TableName(source_name, source_table_name.table)
                    found.add(source_table_name)
                    columns = map_types_in_ddl(source_table_name,
                                               columns_by_table[source_table_name],
                                               type_maps["as_is_att_type"],
                                               type_maps["cast_needed_att_type"])
                    table_design = create_table_design(source_name, source_table_name, table_name, columns)
                    if table_name in table_design_files:
                        # Replace bootstrapped table design with one from file but check whether set of columns changed.
                        design_file = table_design_files[table_name]
                        with open(design_file) as f:
                            existing_table_design = etl.load.load_table_design(f, table_name)
                        etl.load.compare_columns(table_design, existing_table_design)
                    else:
                        design_file = executor.submit(save_table_design,
                                                      table_design, source_table_name, design_dir, dry_run=dry_run)
                    executor.submit(etl.s3.upload_to_s3, design_file, bucket_name, source_prefix, dry_run=dry_run)
    except Exception:
        logging.exception("Error while processing source '%s'", source_name)
        raise
    not_found = found.difference(set(table_design_files))
    if len(not_found):
        logging.warning("New tables which had no design: %s", sorted(table.identifier for table in not_found))
    too_many = set(table_design_files).difference(found)
    if len(too_many):
        logging.warning("Table design files without tables: %s", sorted(table.identifier for table in too_many))
    logging.info("Done with %d table(s) from source '%s'", len(found), source_name)


def dump_schemas_to_s3(args, settings):
    bucket_name = settings("s3", "bucket_name")
    selection = etl.TableNamePatterns.from_list(args.table)
    schemas = [source["name"] for source in settings("sources") if selection.match_schema(source["name"])]
    local_files = etl.s3.find_local_files([args.table_design_dir], schemas, selection)

    # Check that all env vars are set--it's annoying to have this fail for the last source without upfront warning.
    for source in settings("sources"):
        if source["name"] in schemas and "read_access" in source:
            if source["read_access"] not in os.environ:
                raise KeyError("Environment variable not set: %s" % source["read_access"])

    with concurrent.futures.ProcessPoolExecutor(max_workers=args.jobs) as pool:
        for source in settings("sources"):
            source_name = source["name"]
            if source_name not in schemas:
                continue
            if "read_access" not in source:
                logging.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
                continue
            table_design_files = {assoc_files.source_table_name: assoc_files.design_file
                                  for assoc_files in local_files[source_name]}
            logging.debug("Submitting job to download from '%s'", source_name)
            pool.submit(dump_schema_to_s3, source, table_design_files, settings("type_maps"),
                        args.table_design_dir, bucket_name, args.prefix, selection,
                        dry_run=args.dry_run)
