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

import psycopg2

import etl
from etl import TableName
import etl.config
import etl.design
import etl.dw
import etl.file_sets
import etl.monitor
import etl.pg
import etl.relation


def unload_data(conn, description: etl.relation.RelationDescription, aws_iam_role, dry_run=False):
    """
    Unload data from table in the data warehouse using the UNLOAD command.
    A manifest for the CSV files must be provided.
    """
    # TODO Need to figure out what 'description' is here. Might need to change object.
    logger = logging.getLogger(__name__)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    s3_path = "s3://{}/{}".format(description.bucket_name, description.manifest_file_name)
    table_name = description.target_table_name
    if dry_run:
        logger.info("Dry-run: Skipping copy for '%s' from '%s'", table_name.identifier, s3_path)
    elif skip_copy:
        logger.info("Skipping copy for '%s' from '%s'", table_name.identifier, s3_path)
    else:
        logger.info("Copying data into '%s' from '%s'", table_name.identifier, s3_path)
        try:
            # FIXME Given that we're always running as the owner now, could we truncate?
            # The connection should not be open with autocommit at this point or we may have empty random tables.
            etl.pg.execute(conn, """DELETE FROM {}""".format(table_name))
            # N.B. If you change the COPY options, make sure to change the documentation at the top of the file.
            etl.pg.execute(conn, """COPY {}
                                    FROM %s
                                    CREDENTIALS %s MANIFEST
                                    DELIMITER ',' ESCAPE REMOVEQUOTES GZIP
                                    TIMEFORMAT AS 'auto' DATEFORMAT AS 'auto'
                                    TRUNCATECOLUMNS
                                 """.format(table_name), (s3_path, credentials))
            # FIXME Retrieve list of files that were actually loaded
            row_count = etl.pg.query(conn, "SELECT pg_last_copy_count()")
            logger.info("Copied %d rows into '%s'", row_count[0][0], table_name.identifier)
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
                 pattern: etl.TableSelector, dry_run: bool):
    logger = logging.getLogger(__name__)
    logger.info("Processing data to unload.")
    descriptions = etl.relation.RelationDescription.from_file_sets(file_sets)
    conn = etl.pg.connection(data_warehouse.dsn_etl, autocommit=whole_schemas)
    with closing(conn) as conn, conn as conn:
        try:
            for description in descriptions:
                try:
                    unload_data(conn, description, data_warehouse.iam_role, dry_run=dry_run)
                except Exception as e:
                    logger.warning("Unload failure for %s;"
                                   " continuing to UNLOAD remaining tables"
                                   "Failure error was: %s",
                                   description.identifier, e)
        except Exception:
            raise

    # import ipdb;ipdb.set_trace()
