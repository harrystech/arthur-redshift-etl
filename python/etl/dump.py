"""
Functions to deal with dumping data from PostgreSQL databases to CSV.
"""

from contextlib import closing
import logging
import os
import os.path
from tempfile import NamedTemporaryFile

from pyspark import SparkConf, SparkContext, SQLContext
import simplejson as json

import etl
import etl.config
import etl.pg
import etl.s3
import etl.schemas

APPLICATION_NAME = "DataWarehouseETL"


class UnknownTableSizeException(etl.ETLException):
    pass


class MissingCsvFilesException(etl.ETLException):
    pass


def assemble_selected_columns(table_design):
    """
    Pick columns and decide how they are selected (as-is or with an expression).

    Whether there's an expression or just a name the resulting column is always
    called out delimited.
    """
    selected_columns = []
    for column in table_design["columns"]:
        if not column.get("skipped", False):
            if column.get("expression"):
                selected_columns.append(column["expression"] + ' AS "%s"' % column["name"])
            else:
                selected_columns.append('"%s"' % column["name"])
    return selected_columns


def extract_dsn(dsn_string):
    """
    Break the connection string into a JDBC URL and connection properties.

    This is necessary since a JDBC URL may not contain all the properties needed
    to successfully connect, e.g. username, password.  These properties must
    be passed in separately.
    """
    dsn_properties = etl.pg.parse_connection_string(dsn_string)
    dsn_properties.update({
        "ApplicationName": APPLICATION_NAME,
        "readOnly": "true",
        "driver": "org.postgresql.Driver"  # necessary, weirdly enough
    })
    jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(**dsn_properties)
    return jdbc_url, dsn_properties


def suggest_best_partition_number(table_size):
    """
    Suggest number of partitions based on the table size (in bytes).  Number of partitions is always
    a factor of 2.

    The number of partitions is based on:
      Small tables (<= 10M): Use partitions around 1MB.
      Medium tables (<= 1G): Use partitions around 10MB.
      Huge tables (> 1G): Use partitions around 20MB.

    >>> suggest_best_partition_number(100)
    1
    >>> suggest_best_partition_number(1048576)
    1
    >>> suggest_best_partition_number(3 * 1048576)
    2
    >>> suggest_best_partition_number(10 * 1048576)
    8
    >>> suggest_best_partition_number(100 * 1048576)
    8
    >>> suggest_best_partition_number(2000 * 1048576)
    16
    """
    meg = 1024 * 1024
    if table_size <= 10 * meg:
        target = 1 * meg
    elif table_size <= 1024 * meg:
        target = 10 * meg
    else:
        # TODO Should be closer to 100 meg?
        target = 20 * meg

    num_partitions = 1
    partition_size = table_size
    # Keep the partition sizes above the target value:
    while partition_size >= target * 2:
        num_partitions *= 2
        partition_size //= 2

    return num_partitions


def determine_partitioning(source_table_name, table_design, read_access):
    """
    Guesstimate number of partitions based on actual table size and create list of predicates to split
    up table into that number of partitions.

    This requires for one column to be marked as the primary key.  If there's no primary
    key in the table, the number of partitions is always one.
    (This requirement doesn't come from the table size but the need to split the table
    when reading it in.)
    """
    logger = logging.getLogger(__name__)

    if "primary_key" not in table_design.get("constraints", {}):
        logger.info("No primary key defined for '%s', skipping partitioning", source_table_name.identifier)
        return []

    # Note that column constraints such as primary key are stored as one-element lists, hence:
    primary_key = table_design["constraints"]["primary_key"][0]
    logger.debug("Primary key for table '%s' is %s", source_table_name.identifier, primary_key)

    predicates = []
    with closing(etl.pg.connection(read_access, readonly=True)) as conn:
        size = etl.pg.query(conn, """SELECT pg_catalog.pg_table_size('{}') AS table_size
                                  """.format(source_table_name))
        if len(size) != 1:
            raise UnknownTableSizeException("Failed to determine size of source table")
        table_size = size[0]["table_size"]
        num_partitions = suggest_best_partition_number(table_size)

        if num_partitions > 1:
            rows = etl.pg.query(conn, """
                                      SELECT MIN(pkey) AS pkey_bound
                                        FROM (
                                            SELECT "{}" AS pkey
                                                 , NTILE({}) OVER (ORDER BY "{}") AS part
                                              FROM {}
                                              ) t
                                       GROUP BY part
                                       ORDER BY part
                                      """.format(primary_key, num_partitions, primary_key, source_table_name))
            pkey_bounds = list(row["pkey_bound"] for row in rows)
            for low, high in zip(pkey_bounds[:-1], pkey_bounds[1:]):
                predicates.append('({} <= "{}" AND "{}" < {})'.format(low, primary_key, primary_key, high))
            predicates.append('{} <= "{}"'.format(pkey_bounds[-1], primary_key))
            logger.debug("Predicates to split %s:\n  %s", source_table_name.identifier,
                         "\n  ".join("{:3d}: {}".format(i, p) for i, p in enumerate(predicates)))

        logger.info("Picked %d partition(s) for table '%s' (primary key: %s, table size: %d)",
                    num_partitions,
                    source_table_name.identifier,
                    primary_key,
                    table_size)
    return predicates


def read_table_as_dataframe(sql_context, source, source_table_name, table_design):
    """
    Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
    """
    logger = logging.getLogger(__name__)
    read_access = etl.config.env_value(source["read_access"])
    jdbc_url, dsn_properties = extract_dsn(read_access)

    selected_columns = assemble_selected_columns(table_design)
    select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
    logger.debug("Table query: SELECT * FROM %s", select_statement)

    predicates = determine_partitioning(source_table_name, table_design, read_access)
    if predicates:
        df = sql_context.read.jdbc(url=jdbc_url,
                                   properties=dsn_properties,
                                   table=select_statement,
                                   predicates=predicates)
    else:
        df = sql_context.read.jdbc(url=jdbc_url,
                                   properties=dsn_properties,
                                   table=select_statement)
    return df


def write_dataframe_as_csv(df, source_path_name, bucket_name, prefix, dry_run=False):
    """
    Write (partitioned) dataframe to CSV file(s)
    """
    logger = logging.getLogger(__name__)
    csv_path = os.path.join(prefix, "data", source_path_name, "csv")
    full_s3_path = "s3a://{}/{}".format(bucket_name, csv_path)
    if dry_run:
        logger.info("Dry-run: Skipping upload to '%s'", full_s3_path)
    else:
        logger.info("Writing table from '%s' to '%s'", source_path_name, full_s3_path)
        # TODO Share this with load to construct the correct COPY command?
        write_options = {
            "header": "true",
            "nullValue": r"\N",
            "codec": "gzip",
        }
        df.write \
            .format(source='com.databricks.spark.csv') \
            .options(**write_options) \
            .mode('overwrite') \
            .save(full_s3_path)
    write_manifest_file(bucket_name, csv_path, dry_run)


def dump_source_to_s3(sql_context, source, tables_in_s3, bucket_name, prefix, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    logger = logging.getLogger(__name__)
    source_name = source["name"]
    copied = set()
    for assoc_table_files in tables_in_s3:
        source_table_name = assoc_table_files.source_table_name
        target_table_name = assoc_table_files.target_table_name
        with closing(etl.s3.get_file_content(bucket_name, assoc_table_files.design_file)) as content:
            table_design = etl.schemas.load_table_design(content, target_table_name)

        logger.debug("Starting work on %s", source_table_name.identifier)
        df = read_table_as_dataframe(sql_context, source, source_table_name, table_design)
        write_dataframe_as_csv(df, assoc_table_files.source_path_name, bucket_name, prefix, dry_run)

        copied.add(target_table_name)
    logger.info("Done with %d table(s) from source '%s'", len(copied), source_name)


def write_manifest_file(bucket_name, csv_path, dry_run=False):
    """
    Create manifest file to load all the CSV files from the given folder.
    The manifest file will be created in the folder ABOVE the CSV files.
    """
    logger = logging.getLogger(__name__)
    csv_files = etl.s3.list_files_in_folder(bucket_name, csv_path + "/part-")
    if len(csv_files) == 0:
        raise MissingCsvFilesException("Found no CSV files")
    remote_files = ["s3://{}/{}".format(bucket_name, filename) for filename in csv_files]

    manifest_filename = os.path.dirname(csv_path) + ".manifest"
    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}

    if dry_run:
        logger.info("Dry-run: Skipping writing manifest file '%s'", manifest_filename)
    else:
        with NamedTemporaryFile(mode="w+") as local_file:
            logger.debug("Writing manifest file locally to '%s'", local_file.name)
            json.dump(manifest, local_file, indent="    ", sort_keys=True)
            local_file.write('\n')
            local_file.flush()
            logger.debug("Done writing '%s'", local_file.name)
            etl.s3.upload_to_s3(local_file.name, bucket_name, object_key=manifest_filename)


def dump_to_s3(settings, table, prefix, dry_run=False):
    """
    Dump data from multiple upstream sources to S3
    """
    logger = logging.getLogger(__name__)
    selection = etl.TableNamePatterns.from_list(table)
    sources = selection.match_field(settings("sources"), "name")
    schemas = [source["name"] for source in sources]

    bucket_name = settings("s3", "bucket_name")
    tables_in_s3 = etl.s3.find_files_for_schemas(bucket_name, prefix, schemas, selection)
    if not tables_in_s3:
        logger.error("No applicable files found in 's3://%s/%s' for '%s'", bucket_name, prefix, selection)
        return

    # Check that all env vars are set--it's annoying to have this fail for the last source without upfront warning.
    for source, source_name in zip(sources, schemas):
        if "read_access" in source:
            if source["read_access"] not in os.environ:
                raise KeyError("Environment variable to access '%s' not set: %s" % (source_name, source["read_access"]))

    logger.info("Starting SparkSQL context for %s", APPLICATION_NAME)
    conf = SparkConf()
    conf.setAppName(APPLICATION_NAME)
    sc = SparkContext(conf=conf)
    sql_context = SQLContext(sc)

    for source, source_name in zip(sources, schemas):
        if "read_access" not in source:
            logger.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
        elif source_name not in tables_in_s3:
            logger.warning("No information found for source '%s' in s3://%s/%s", source_name, bucket_name, prefix)
        else:
            # TODO Need to parallelize across sources
            dump_source_to_s3(sql_context, source, tables_in_s3[source_name], bucket_name, prefix, dry_run=dry_run)
