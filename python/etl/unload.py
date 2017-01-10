"""
Unload data from Redshift to S3.

A "unload" refers to the wholesale dumping of data from any schema or table involved into
some number of gzipped CSV files to a given S3 destination.

(1) CSV files must have fields delimited by commas, quotes around fields if they
contain a comma, and have doubled-up quotes if there's a quote within the field.

Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP ALLOWOVERWRITE

(2) Every unload must be accompanied by a manifest containing a complete list of CSVs

(3) Every unload must contain a YAML file containing a completely list of all columns from the
schema or table from which the data was dumped to CSV.
"""

from contextlib import closing
from typing import List
from string import Template
import logging
import os

from psycopg2.extensions import connection
import boto3
import botocore.exceptions

from etl.file_sets import TableFileSet
from etl.config import DataWarehouseConfig
from etl.config import DataWarehouseSchema
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


def run_redshift_unload(conn: connection, description: RelationDescription, unload_path: str, aws_iam_role: str,
                        allow_overwrite=False) -> None:
    """
    Execute the UNLOAD command for the given select statement. Optionally allow users to overwrite
    previously unloaded data within the same key space
    """
    logger = logging.getLogger(__name__)

    columns = etl.design.get_columns(description.table_design)
    select_statement = """SELECT {} FROM {}""".format(", ".join(columns), description.table_design["name"])

    credentials = "aws_iam_role={}".format(aws_iam_role)
    unload_statement = """UNLOAD ('{}')
                          TO '{}'
                          CREDENTIALS '{}' MANIFEST
                          DELIMITER ',' ESCAPE ADDQUOTES GZIP
                       """.format(select_statement, unload_path, credentials)
    if allow_overwrite:
        unload_statement += "ALLOWOVERWRITE"

    with etl.pg.log_error():
        etl.pg.execute(conn, unload_statement)
        logger.info("Unloaded data from '%s' into '%s'", description.identifier, unload_path)


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
        logger.info("Successfully wrote file to 's3://%s/%s/_SUCCESS'", bucket_name, prefix)
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        logger.error("Error code %s for Object 's3://%s/%s'", error_code, bucket_name, prefix)
        raise


def build_s3_key(prefix: str, description: RelationDescription, schema: DataWarehouseSchema) -> str:
    schema_table_name = "{}-{}".format(description.target_table_name.schema, description.target_table_name.table)
    rendered_template = render_unload_template(schema.s3_unload_path_template, prefix)
    return os.path.join(rendered_template, "data", schema.name, schema_table_name, "csv")


def render_unload_template(template: str, prefix) -> str:
    t = Thyme.today()
    str_template = Template(template)
    today = os.path.join(t.year, t.month, t.day)
    data = dict(prefix=prefix, today=today)
    return str_template.substitute(data)


def unload_redshift_relation(conn: connection, description: RelationDescription, schema: DataWarehouseSchema,
                             aws_iam_role: str, prefix: str, allow_overwrite=False, dry_run=False) -> None:
    """
    Unload data from table in the data warehouse using the UNLOAD command.
    A manifest for the CSV files must be provided.
    """
    logger = logging.getLogger(__name__)
    s3_key_prefix = build_s3_key(prefix, description, schema)
    unload_path = "s3://{}/{}/".format(schema.s3_bucket, s3_key_prefix)

    if dry_run:
        logger.info("Dry-run: Skipping for for '%s' to '%s'", description.identifier, unload_path)
    else:
        logger.info("Unloading data from '%s' to '%s'", description.identifier, unload_path)
        run_redshift_unload(conn, description, unload_path, aws_iam_role, allow_overwrite=allow_overwrite)
        write_success_file(schema.s3_bucket, s3_key_prefix)


def unload_to_s3(config: DataWarehouseConfig, file_sets: List[TableFileSet], prefix: str, allow_overwrite: bool,
                 keep_going: bool, dry_run: bool) -> None:
    """
    Create CSV files for selected tables based on the S3 path in a "unload" source.
    """
    logger = logging.getLogger(__name__)
    logger.info("Processing data to unload")
    descriptions = RelationDescription.from_file_sets(file_sets)
    unloadable_relations = [d for d in descriptions if d.is_unloadable]
    if not unloadable_relations:
        logger.warning("Found no relations that are unloadable.")
        return

    conn = etl.pg.connection(config.dsn_etl, readonly=True)
    with closing(conn) as conn:
        for relation in unloadable_relations:
            try:
                [unload_schema] = [schema for schema in config.schemas if schema.name == relation.unload_target]
                unload_redshift_relation(conn, relation, unload_schema, config.iam_role, prefix,
                                         allow_overwrite=allow_overwrite, dry_run=dry_run)
            except Exception:
                if keep_going:
                    logger.warning("Unload failure for '%s'", relation.identifier)
                    logger.exception("Ignoring this exception and proceeding as requested:")
                else:
                    raise
