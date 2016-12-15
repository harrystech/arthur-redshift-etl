"""
Unload data from Redshift to S3.

A "unload" refers to the wholesale dumping of data from any schema or table involved into
some number of gzipped CSV files to a given S3 destination.

Details for unload (1):

CSV files must have fields delimited by commas, quotes around fields if they
contain a comma, and have doubled-up quotes if there's a quote within the field.

Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP

Details for unload (2):

Every unload must be accompanied by a manifest containing a complete list of CSVs

Details for unload (3):

Every unload must contain a YAML file containing a completely list of all columns from the
schema or table from which the data was dumped to CSV.
"""
from contextlib import closing
from itertools import chain
import logging
import typing
import os

import psycopg2
import boto3

import etl
import etl.config
import etl.design
import etl.dw
import etl.file_sets
import etl.monitor
import etl.pg
import etl.relation
import etl.dump


def unload_data(conn: psycopg2.extensions.connection, description: etl.relation.RelationDescription,
                aws_iam_role: str, prefix, dry_run=False):
    """
    Unload data from table in the data warehouse using the UNLOAD command.
    A manifest for the CSV files must be provided.
    """
    logger = logging.getLogger(__name__)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    s3_key_prefix = os.path.join(prefix, 'data', description.source_table_name.schema,
                                 description.source_table_name.table, 'unload')

    unload_path = "s3://{}/{}/".format(description.bucket_name, s3_key_prefix)

    table_design = etl.design.download_table_design(description.bucket_name, description.design_file_name,
                                                    description.target_table_name)
    columns = etl.dump.assemble_selected_columns(table_design)
    select_statement = """SELECT {} FROM {}""".format(", ".join(columns), description.source_table_name)

    if dry_run:
        logger.info("Dry-run: Skipping for for '%s' to '%s'", description.identifier, unload_path)
    else:
        logger.info("Unloading data from '%s' to '%s'", description.identifier, unload_path)
        try:
            etl.pg.execute(conn, """UNLOAD ('{}')
                                    TO %s
                                    CREDENTIALS %s MANIFEST
                                    DELIMITER ',' ESCAPE GZIP
                                    ALLOWOVERWRITE
                                 """.format(select_statement), (unload_path, credentials))
            logger.info("Unloaded data from '%s' into '%s'", description.identifier, unload_path)

            logger.info("Copying design file from '%s' to '%s'", description.design_file_name, unload_path)
            design_file_name = "{}-{}.yaml".format(description.target_table_name.schema,
                                                   description.target_table_name.table)
            copied_design_file_key = os.path.join(s3_key_prefix, design_file_name)
            session = boto3.session.Session()
            s3 = session.resource("s3")
            copy_source = {
                'Bucket': description.bucket_name,
                'Key': description.design_file_name
            }
            s3.Object(description.bucket_name, copied_design_file_key).copy_from(CopySource=copy_source)
            logger.info("Successfully copied design file from '%s' to '%s'", description.design_file_name, unload_path)
        except psycopg2.Error as exc:
            conn.rollback()
            if "stl_load_errors" in exc.pgerror:
                logger.debug("Trying to get error message from stl_log_errors table")
                info = etl.pg.query(conn, """SELECT query, starttime, filename, colname, type, col_length,
                                                    line_number, position, err_code, err_reason
                                               FROM stl_load_errors
                                              WHERE session = pg_backend_pid()
                                              ORDER BY starttime DESC
                                              LIMIT 1""")
                values = "  \n".join(["{}: {}".format(k, row[k]) for row in info for k in row.keys()])
                logger.info("Information from stl_load_errors:\n  %s", values)
            raise


def unload_to_s3(config: etl.config.DataWarehouseConfig, file_sets: typing.List[etl.file_sets.TableFileSet],
                 prefix: str, dry_run: bool):
    logger = logging.getLogger(__name__)
    logger.info("Processing data to unload.")
    descriptions = etl.relation.RelationDescription.from_file_sets(file_sets)
    conn = etl.pg.connection(config.dsn_etl)
    with closing(conn) as conn, conn as conn:
        try:
            for description in descriptions:
                try:
                    unload_data(conn, description, config.iam_role, prefix, dry_run=dry_run)
                except Exception as e:
                    logger.warning("Unload failure for %s;"
                                   " continuing to UNLOAD remaining tables"
                                   "Failure error was: %s",
                                   description.identifier, e)
        except Exception:
            raise
