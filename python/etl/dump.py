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
import etl.load
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


def dump_source_to_s3(sqlContext, source, tables_in_s3, size_map, bucket_name, prefix, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    source_name = source["name"]
    read_access = etl.config.env_value(source["read_access"])
    dsn_properties = etl.pg.parse_connection_string(read_access)
    dsn_properties.update({
        "ApplicationName": APPLICATION_NAME,
        "readOnly": "true",
        "sslmode": "require",  # XXX make configurable?
        "driver": "org.postgresql.Driver"  # necessary, weirdly enough
    })
    jdbc_url = "jdbc:postgresql://{host}:{port}/{database}".format(**dsn_properties)
    base_path = "s3a://{}/{}/{}".format(bucket_name, prefix, "data")

    copied = set()
    for assoc_table_files in tables_in_s3:
        source_table_name = assoc_table_files.source_table_name
        target_table_name = assoc_table_files.target_table_name
        design_file = etl.s3.get_file_content(bucket_name, assoc_table_files.design_file)
        table_design = etl.schemas.load_table_design(design_file, target_table_name)

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


def dump_to_s3(settings, table, prefix, dry_run):
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
            logging.info("Skipping empty source '%s' (no environment variable to use for connection)", source_name)
        elif source_name not in tables_in_s3:
            logging.warning("No information found for source '%s' in s3://%s/%s", source_name, bucket_name, prefix)
        else:
            # TODO Need to parallelize across sources
            dump_source_to_s3(sqlContext, source, tables_in_s3[source_name], settings("dataframe", "partition_sizes"),
                              bucket_name, prefix, dry_run=dry_run)


def write_manifest_file(local_files, bucket_name, prefix, dry_run=False):
    """
    Create manifest file to load all the given files (after upload
    to S3) and return name of new manifest file.
    """
    logger = logging.getLogger(__name__)
    data_files = [filename for filename in local_files if not filename.endswith(".manifest")]
    if len(data_files) == 0:
        raise ValueError("List of files must include at least one CSV file")
    elif len(data_files) > 1:
        parts = os.path.commonprefix(data_files)
        filename = parts[:parts.rfind(".part_")] + ".manifest"
    else:
        csv_file = data_files[0]
        filename = csv_file[:csv_file.rfind(".csv")] + ".csv.manifest"
    remote_files = ["s3://{}/{}/{}".format(bucket_name, prefix, os.path.basename(name)) for name in data_files]
    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}
    if dry_run:
        logger.info("Dry-run: Skipping writing new manifest file to '%s'", filename)
    else:
        logger.info("Writing new manifest file for %d file(s) to '%s'", len(data_files), filename)
        with open(filename, 'wt') as o:
            json.dump(manifest, o, indent="    ", sort_keys=True)
            o.write('\n')
        logger.debug("Done writing '%s'", filename)
    return filename


def write_manifest_file_eventually(file_futures, bucket_name, prefix, dry_run=False):
    return write_manifest_file([future.result() for future in file_futures], bucket_name, prefix, dry_run=dry_run)


