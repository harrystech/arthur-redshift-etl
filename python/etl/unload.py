"""
Unload data from Redshift to S3.

A "unload" refers to the wholesale dumping of data from any schema or table involved into
some number of gzipped CSV files to a given S3 destination.

Details for unload (1):

CSV files must have fields delimited by commas, quotes around fields if they
contain a comma, and have doubled-up quotes if there's a quote within the field.

Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP ALLOWOVERWRITE

Details for unload (2):

Every unload must be accompanied by a manifest containing a complete list of CSVs

Details for unload (3):

Every unload must contain a YAML file containing a completely list of all columns from the
schema or table from which the data was dumped to CSV.
"""
from contextlib import closing
from typing import List
import logging
import os
import tempfile

from psycopg2.extensions import connection
import psycopg2
import boto3
import botocore.exceptions

from etl.file_sets import TableFileSet
from etl.config import DataWarehouseConfig
from etl.relation import RelationDescription
from etl.thyme import Thyme
import etl
import etl.config
import etl.design
import etl.dw
import etl.file_sets
import etl.monitor
import etl.pg
import etl.relation
import etl.dump


def generate_select_statement(bucket_name: str, design_file_name: str, table_name: str) -> str:
    """
    We want to generate specific select statements to be run as part of the Redshift UNLOAD
    command. To do this we'll map over the columns specified in the design file. This is a
    better practice to 'SELECT *'
    """
    table_design = etl.design.download_table_design(bucket_name, design_file_name, table_name)
    columns = etl.dump.assemble_selected_columns(table_design)
    select_statement = """SELECT {} FROM {}""".format(", ".join(columns), table_name)
    return select_statement


def run_redshift_unload(conn: connection, select_statement: str, unload_path: str, aws_iam_role: str,
                        identifier: str, allow_overwrite=False) -> None:
    """
    Execute the UNLOAD command for the given select statement. Optionally allow users to overwrite
    previously unloaded data within the same key space
    """
    logger = logging.getLogger(__name__)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    unload_statement = """UNLOAD ('{}')
                          TO '{}'
                          CREDENTIALS '{}' MANIFEST
                          DELIMITER ',' ESCAPE GZIP
                       """.format(select_statement, unload_path, credentials)
    if allow_overwrite:
        unload_statement += "ALLOWOVERWRITE"

    with etl.pg.log_error():
        etl.pg.execute(conn, unload_statement)
        logger.info("Unloaded data from '%s' into '%s'", identifier, unload_path)


def copy_design_file_to_unload_keyspace(design_file_path: str, unload_path: str, schema: str, table_name: str,
                                        prefix: str, bucket_name: str) -> None:
    """
    As the final part of the UNLOAD command we need to copy the design YAML file from the schema location
    into the unload keyspace in S3. This is so whatever process or application cares about this unloaded
    data has the option to map out a list of columns and types of the data found in each GZipped CSV
    """
    logger = logging.getLogger(__name__)
    logger.info("Copying design file from '%s' to '%s'", design_file_path, unload_path)
    design_file_name = "{}-{}.yaml".format(schema, table_name)
    copied_design_file_key = os.path.join(prefix, design_file_name)
    session = boto3.session.Session()
    s3 = session.resource("s3")
    copy_source = {
        'Bucket': bucket_name,
        'Key': design_file_path
    }
    try:
        s3.Object(bucket_name, copied_design_file_key).copy_from(CopySource=copy_source)
        logger.info("Successfully copied design file from '%s' to '%s'", design_file_path, unload_path)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == "AccessDenied":
            logger.warning("Access Denied for Object 's3://%s/%s'", bucket_name, design_file_path)
            raise
        else:
            logger.warning("THIS IS THE ERROR CODE: %s", error_code)
            raise


def write_success_file(bucket_name: str, prefix: str) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Writing _SUCCESS file to 's3://%s/%s/'", bucket_name, prefix)
    obj_key = os.path.join(prefix, "_SUCCESS")
    session = boto3.session.Session()
    s3 = session.resource("s3")
    bucket = s3.Bucket(bucket_name)
    object = bucket.Object(obj_key)
    try:
        object.put()
        logger.info("Successfully wrote _SUCCESS file to 's3://%s/%s/'", bucket_name, prefix)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == "AccessDenied":
            logger.warning("Access Denied for Object 's3://%s/%s'", bucket_name, prefix)
            raise
        else:
            logger.warning("THIS IS THE ERROR CODE: %s", error_code)
            raise


def build_key_prefix(user: str, schema_table_name: str, today=False) -> str:
    t = Thyme.today()
    return os.path.join(user, "data", "unload", schema_table_name, t.year, t.month, t.day, "csv")


def unload_data(conn: connection, description: RelationDescription, aws_iam_role: str, prefix: str,
                allow_overwrite=False, today=False, dry_run=False) -> None:
    """
    Unload data from table in the data warehouse using the UNLOAD command.
    A manifest for the CSV files must be provided.
    """
    logger = logging.getLogger(__name__)
    schema_table_key = "{}-{}".format(description.target_table_name.schema, description.target_table_name.table)
    s3_key_prefix = build_key_prefix(prefix, schema_table_key, today=today)
    unload_path = "s3://{}/{}/".format(description.bucket_name, s3_key_prefix)
    select_statement = generate_select_statement(description.bucket_name, description.design_file_name,
                                                 description.target_table_name)

    if dry_run:
        logger.info("Dry-run: Skipping for for '%s' to '%s'", description.identifier, unload_path)
    else:
        logger.info("Unloading data from '%s' to '%s'", description.identifier, unload_path)
        run_redshift_unload(conn, select_statement, unload_path, aws_iam_role, description.identifier,
                            allow_overwrite=allow_overwrite)

        copy_design_file_to_unload_keyspace(description.design_file_name, unload_path,
                                            description.target_table_name.schema, description.target_table_name.table,
                                            s3_key_prefix, description.bucket_name)
        write_success_file(description.bucket_name, s3_key_prefix)


def unload_to_s3(config: DataWarehouseConfig, file_sets: List[TableFileSet], prefix: str, allow_overwrite: bool,
                 dry_run: bool) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Processing data to unload.")
    descriptions = etl.relation.RelationDescription.from_file_sets(file_sets)
    conn = etl.pg.connection(config.dsn_etl, readonly=True)
    with closing(conn) as conn:
        for description in descriptions:
            try:
                unload_data(conn, description, config.iam_role, prefix, allow_overwrite=allow_overwrite,
                            dry_run=dry_run)
            except Exception as e:
                logger.warning("Unload failure for %s;"
                               " continuing to UNLOAD remaining tables"
                               "Failure error was: %s",
                               description.identifier, e)
