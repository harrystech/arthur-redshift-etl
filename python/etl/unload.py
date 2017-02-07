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
import logging
import os

from psycopg2.extensions import connection  # For type annotation

from etl.config import DataWarehouseConfig, DataWarehouseSchema
from etl.file_sets import TableFileSet
from etl.relation import RelationDescription
from etl.thyme import Thyme
import etl
import etl.file_sets
import etl.monitor
import etl.pg
import etl.s3


class DataUnloadError(etl.ETLError):
    pass


class UnloadTargetNotFoundError(DataUnloadError):
    pass


def run_redshift_unload(conn: connection, description: RelationDescription, unload_path: str, aws_iam_role: str,
                        allow_overwrite=False) -> None:
    """
    Execute the UNLOAD command for the given select statement.
    Optionally allow users to overwrite previously unloaded data within the same key space.
    """
    select_statement = """
        SELECT {}
        FROM {}
        """.format(", ".join(description.columns), description.target_table_name)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    null_string = "'\\\\N'"
    unload_statement = """
        UNLOAD ('{}')
        TO '{}'
        CREDENTIALS '{}' MANIFEST
        DELIMITER ',' ESCAPE ADDQUOTES GZIP NULL AS {}
        """.format(select_statement, unload_path, credentials, null_string)
    if allow_overwrite:
        unload_statement += "ALLOWOVERWRITE"

    print(unload_statement)
    with etl.pg.log_error():
        etl.pg.execute(conn, unload_statement)


def write_columns_file(description: RelationDescription, bucket_name: str, prefix: str, dry_run: bool) -> None:
    """
    Write out a YAML file into the same folder as the CSV files to document the columns of the relation
    """
    logger = logging.getLogger(__name__)

    data = {"columns": description.unquoted_columns}
    object_key = os.path.join(prefix, "columns.yaml")

    if dry_run:
        logger.info("Dry-run: Skipping writing columns file to 's3://%s/%s'", bucket_name, object_key)
    else:
        logger.info("Writing columns file to 's3://%s/%s'", bucket_name, object_key)
        etl.s3.upload_data_to_s3(data, bucket_name, object_key)


def write_success_file(bucket_name: str, prefix: str, dry_run: bool=False) -> None:
    """
    Write out a "_SUCCESS" file into the same folder as the CSV files to mark
    the unload as complete.  The dump insists on this file before writing a manifest for load.
    """
    logger = logging.getLogger(__name__)
    object_key = os.path.join(prefix, "_SUCCESS")
    if dry_run:
        logger.info("Dry-run: Skipping creation of 's3://%s/%s'", bucket_name, object_key)
    else:
        logger.info("Creating 's3://%s/%s'", bucket_name, object_key)
        etl.s3.upload_empty_object(bucket_name, object_key)


def unload_redshift_relation(conn: connection, description: RelationDescription, schema: DataWarehouseSchema,
                             aws_iam_role: str, prefix: str, allow_overwrite=False, dry_run: bool=False) -> None:
    """
    Unload data from table in the data warehouse using the UNLOAD command of Redshift.
    """
    logger = logging.getLogger(__name__)

    # TODO Move the "{}-{}" logic into the RelationDescription? Or all of this for "csv_folder()"?
    schema_table_name = "{}-{}".format(description.target_table_name.schema, description.target_table_name.table)
    rendered_template = Thyme.render_template(schema.s3_unload_path_template, {"prefix": prefix})
    s3_key_prefix = os.path.join(rendered_template, "data", schema.name, schema_table_name, "csv")
    unload_path = "s3://{}/{}/".format(schema.s3_bucket, s3_key_prefix)

    if dry_run:
        logger.info("Dry-run: Skipping unload for '%s' to '%s'", description.identifier, unload_path)
    else:
        try:
            logger.info("Unloading data from '%s' to '%s'", description.identifier, unload_path)

            # FIXME Review the target/source/destination values
            with etl.monitor.Monitor(description.identifier, 'unload', dry_run=dry_run,
                                     source={'schema': description.target_table_name.schema,
                                             'table': description.target_table_name.table},
                                     destination={'bucket_name': schema.s3_bucket,
                                                  'prefix': s3_key_prefix}):
                run_redshift_unload(conn, description, unload_path, aws_iam_role, allow_overwrite=allow_overwrite)
        except Exception as exc:
            raise DataUnloadError(exc) from exc
    write_columns_file(description, schema.s3_bucket, s3_key_prefix, dry_run=dry_run)
    write_success_file(schema.s3_bucket, s3_key_prefix, dry_run=dry_run)


def unload_to_s3(config: DataWarehouseConfig, descriptions: List[RelationDescription], prefix: str,
                 allow_overwrite: bool, keep_going: bool, dry_run: bool) -> None:
    """
    Create CSV files for selected tables based on the S3 path in an "unload" source.
    """
    logger = logging.getLogger(__name__)
    logger.info("Collecting all table information (from S3) before unload")
    unloadable_relations = [d for d in descriptions if d.is_unloadable]
    if not unloadable_relations:
        logger.warning("Found no relations that are unloadable.")
        return

    target_lookup = {schema.name: schema for schema in config.schemas if schema.is_an_unload_target}
    relation_target_tuples = []
    for relation in unloadable_relations:
        if relation.unload_target not in target_lookup:
            raise UnloadTargetNotFoundError("Unload target specified, but not defined: '%s'" %
                                            relation.unload_target)
        relation_target_tuples.append((relation, target_lookup[relation.unload_target]))

    conn = etl.pg.connection(config.dsn_etl, readonly=True)
    with closing(conn) as conn:
        for relation, unload_schema in relation_target_tuples:
            try:
                unload_redshift_relation(conn, relation, unload_schema, config.iam_role, prefix,
                                         allow_overwrite=allow_overwrite, dry_run=dry_run)
            except DataUnloadError:
                if keep_going:
                    logger.warning("Unload failure for '%s'", relation.identifier)
                    logger.exception("Ignoring this exception and proceeding as requested:")
                else:
                    raise
