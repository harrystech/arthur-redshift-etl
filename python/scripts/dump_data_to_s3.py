#!  /usr/bin/env python3

"""
Connect to source databases and download data using JDBC connection,
then write out CSV files (in multiple partitions).

The final file layout will look something like this:
s3://<bucket_name>/<prefix>/data/<source name>/<old schema name>-<table name>/csv/
"""

import logging
import os
import os.path

from pyspark import SparkConf, SparkContext, SQLContext

import etl
import etl.arguments
import etl.config
import etl.dump
import etl.load
import etl.pg
import etl.s3


def dump_source_to_s3(sqlContext, source, tables_in_s3, size_map, bucket_name, prefix, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    source_name = source["name"]
    read_access = etl.env_value(source["read_access"])
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
        selected_columns = etl.dump.assemble_selected_columns(table_design)
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


def build_argument_parser():
    return etl.arguments.argument_parser(["config", "prefix", "prefix_env", "dry-run", "table"], description=__doc__)


if __name__ == "__main__":
    main_args = build_argument_parser().parse_args()
    etl.config.configure_logging(main_args.log_level)
    main_settings = etl.config.load_settings(main_args.config)
    with etl.measure_elapsed_time():
        dump_to_s3(main_args, main_settings)
