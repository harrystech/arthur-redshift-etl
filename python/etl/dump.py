"""
Functions to deal with dumping data from PostgreSQL databases to CSV.
"""

import logging
import os
import os.path

from pyspark import SparkConf, SparkContext, SQLContext
import simplejson as json

import etl
import etl.config
import etl.pg
import etl.s3
import etl.schemas

APPLICATION_NAME = "DataWarehouseETL"


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


def read_table_as_dataframe(sql_context, source, source_table_name, table_design, size_map):
    """
    Read (partitioned) dataframe by contacting upstream JDBC-reachable source.
    """
    logger = logging.getLogger(__name__)
    read_access = etl.config.env_value(source["read_access"])
    jdbc_url, dsn_properties = extract_dsn(read_access)

    selected_columns = assemble_selected_columns(table_design)
    select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
    logger.debug("Table query: SELECT * FROM %s", select_statement)

    # Search the primary key (look also for distribution by key!)
    if "primary_key" in table_design.get("constraints", {}):
        # Note that column constraints such as primary key are stored as one-element lists, hence:
        primary_key = table_design["constraints"]["primary_key"][0]
        logger.debug("Primary key for table '%s' = %s", source_table_name.identifier, primary_key)
        partition_df = sql_context.read.jdbc(url=jdbc_url,
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

        num_partitions = 1
        for low in sorted(size_map):
            if row_count >= low:
                num_partitions = size_map[low]

        logger.info("Picked %d partitions for table '%s' (lower: %d, upper: %d, count: %d)",
                    num_partitions,
                    source_table_name.identifier,
                    lower_bound,
                    upper_bound,
                    row_count)

        df = sql_context.read.jdbc(url=jdbc_url,
                                   properties=dsn_properties,
                                   table=select_statement,
                                   column=primary_key,
                                   lowerBound=lower_bound,
                                   upperBound=upper_bound,
                                   numPartitions=num_partitions)

        # Instead of lower and upper bounds, we could more cleverly create partitions and use the predicates list:
        #                          predicates=[
        #                              "id >= 7253011 AND id < 7683410",
        #                              "id >= 8113722 AND id < 8544102",
        #                              "id >= 8544133 AND id < 8974448",
        #                              "id >= 8974444 AND id < 9404794",
        #                          ]

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
        df.write \
            .format(source='com.databricks.spark.csv') \
            .option("header", "true") \
            .option("nullValue", r"\N") \
            .option("codec", "gzip") \
            .mode('overwrite') \
            .save(full_s3_path)
    write_manifest_file(bucket_name, csv_path, dry_run)


def dump_source_to_s3(sql_context, source, tables_in_s3, size_map, bucket_name, prefix, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    logger = logging.getLogger(__name__)
    source_name = source["name"]

    copied = set()
    for assoc_table_files in tables_in_s3:

        # XXX Split into _read_data and _write_data

        source_table_name = assoc_table_files.source_table_name
        target_table_name = assoc_table_files.target_table_name
        design_file = etl.s3.get_file_content(bucket_name, assoc_table_files.design_file)
        table_design = etl.schemas.load_table_design(design_file, target_table_name)

        df = read_table_as_dataframe(sql_context, source, source_table_name, table_design, size_map)
        write_dataframe_as_csv(df, assoc_table_files.source_path_name, bucket_name, prefix, dry_run)

        copied.add(target_table_name)
    logger.info("Done with %d table(s) from source '%s'", len(copied), source_name)


def write_manifest_file(bucket_name, csv_path, dry_run=False):
    """
    Create manifest file to load all the CSV files from the given folder.
    The manifest file will be created in the folder ABOVE the CSV files.
    """
    logger = logging.getLogger(__name__)
    csv_files = etl.s3.find_files_in_folder(bucket_name, csv_path + "/part-")
    if len(csv_files) == 0:
        raise ValueError("Found no CSV files")
    remote_files = ["s3://{}/{}".format(bucket_name, filename) for filename in csv_files]

    manifest_filename = os.path.dirname(csv_path) + ".manifest"
    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}

    # XXX Move file to dedicated temp directory
    local_filename = os.path.join("/tmp", os.path.basename(manifest_filename))

    if dry_run:
        logger.info("Dry-run: Skipping writing manifest file '%s'", manifest_filename)
    else:
        logger.debug("Writing manifest file locally to '%s'", local_filename)
        with open(local_filename, 'wt') as o:
            json.dump(manifest, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.debug("Done writing '%s'", local_filename)
        etl.s3.upload_to_s3(local_filename, bucket_name, os.path.dirname(manifest_filename))


def dump_to_s3(settings, table, prefix, dry_run):
    logger = logging.getLogger(__name__)
    bucket_name = settings("s3", "bucket_name")
    selection = etl.TableNamePatterns.from_list(table)
    sources = [dict(source) for source in settings("sources") if selection.match_schema(source["name"])]
    schemas = [source["name"] for source in sources]
    tables_in_s3 = etl.s3.find_files_in_bucket(bucket_name, prefix, schemas, selection)

    # Check that all env vars are set--it's annoying to have this fail for the last source without upfront warning.
    for source, source_name in zip(sources, schemas):
        if "read_access" in source:
            if source["read_access"] not in os.environ:
                raise KeyError("Environment variable to access '%s' not set: %s" % (source_name, source["read_access"]))

    conf = SparkConf()
    conf.setAppName(APPLICATION_NAME)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    for source, source_name in zip(sources, schemas):
        if "read_access" not in source:
            logger.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
        elif source_name not in tables_in_s3:
            logger.warning("No information found for source '%s' in s3://%s/%s", source_name, bucket_name, prefix)
        else:
            # TODO Need to parallelize across sources
            dump_source_to_s3(sqlContext, source, tables_in_s3[source_name], settings("dataframe", "partition_sizes"),
                              bucket_name, prefix, dry_run=dry_run)
