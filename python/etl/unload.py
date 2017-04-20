"""
Unload data from data warehouse to S3.

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

from psycopg2.extensions import connection  # only for type annotation

import etl
import etl.monitor
import etl.pg
import etl.s3
from etl.config.dw import DataWarehouseConfig, DataWarehouseSchema
from etl.errors import DataUnloadError, ETLDelayedExit, TableDesignSemanticError
from etl.relation import RelationDescription
from etl.thyme import Thyme

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def run_redshift_unload(conn: connection, relation: RelationDescription, unload_path: str, aws_iam_role: str,
                        allow_overwrite=False) -> None:
    """
    Execute the UNLOAD command for the given :relation (via a select statement).
    Optionally allow users to overwrite previously unloaded data within the same keyspace.
    """
    select_statement = """
        SELECT {}
        FROM {}
    """.format(", ".join(relation.columns), relation)
    credentials = "aws_iam_role={}".format(aws_iam_role)
    # TODO Need to review why we can't use r"\N"
    null_string = "\\\\N"
    unload_statement = """
        UNLOAD ('{}')
        TO '{}'
        CREDENTIALS '{}' MANIFEST
        DELIMITER ',' ESCAPE ADDQUOTES GZIP NULL AS '{}'
        """.format(select_statement, unload_path, credentials, null_string)
    if allow_overwrite:
        unload_statement += "ALLOWOVERWRITE"

    logger.info("Unloading data from '%s' to '%s'", relation.identifier, unload_path)
    with etl.pg.log_error():
        etl.pg.execute(conn, unload_statement)


def write_columns_file(relation: RelationDescription, bucket_name: str, prefix: str, dry_run: bool) -> None:
    """
    Write out a YAML file into the same folder as the CSV files to document the columns of the relation
    """
    data = {"columns": relation.unquoted_columns}
    object_key = os.path.join(prefix, "columns.yaml")
    if dry_run:
        logger.info("Dry-run: Skipping writing columns file to 's3://%s/%s'", bucket_name, object_key)
    else:
        logger.info("Writing columns file to 's3://%s/%s'", bucket_name, object_key)
        etl.s3.upload_data_to_s3(data, bucket_name, object_key)


def write_success_file(bucket_name: str, prefix: str, dry_run=False) -> None:
    """
    Write out a "_SUCCESS" file into the same folder as the CSV files to mark
    the unload as complete. The dump insists on this file before writing a manifest for load.
    """
    object_key = os.path.join(prefix, "_SUCCESS")
    if dry_run:
        logger.info("Dry-run: Skipping creation of 's3://%s/%s'", bucket_name, object_key)
    else:
        logger.info("Creating 's3://%s/%s'", bucket_name, object_key)
        etl.s3.upload_empty_object(bucket_name, object_key)


def unload_relation(conn: connection, relation: RelationDescription, schema: DataWarehouseSchema,
                    aws_iam_role: str, prefix: str, index: dict,
                    allow_overwrite=False, dry_run=False) -> None:
    """
    Unload data from table in the data warehouse using the UNLOAD command of Redshift.
    """
    # TODO Move the "{}-{}" logic into the TableFileSet
    rendered_prefix = Thyme.render_template(schema.s3_unload_path_template, {"prefix": prefix})
    schema_table_name = "{}-{}".format(relation.target_table_name.schema, relation.target_table_name.table)
    s3_key_prefix = os.path.join(rendered_prefix, "data", schema.name, schema_table_name, "csv")
    unload_path = "s3://{}/{}/".format(schema.s3_bucket, s3_key_prefix)

    with etl.monitor.Monitor(relation.identifier,
                             "unload",
                             source={'schema': relation.target_table_name.schema,
                                     'table': relation.target_table_name.table},
                             destination={'name': schema.name,
                                          'bucket_name': schema.s3_bucket,
                                          'prefix': s3_key_prefix},
                             index=index,
                             dry_run=dry_run):
        if dry_run:
            logger.info("Dry-run: Skipping unload of '%s' to '%s'", relation.identifier, unload_path)
        else:
            run_redshift_unload(conn, relation, unload_path, aws_iam_role, allow_overwrite=allow_overwrite)
            write_columns_file(relation, schema.s3_bucket, s3_key_prefix, dry_run=dry_run)
            write_success_file(schema.s3_bucket, s3_key_prefix, dry_run=dry_run)


def unload_to_s3(config: DataWarehouseConfig, relations: List[RelationDescription], prefix: str,
                 allow_overwrite: bool, keep_going: bool, dry_run: bool) -> None:
    """
    Create CSV files for selected tables based on the S3 path in an "unload" source.
    """
    etl.relation.RelationDescription.load_in_parallel(relations)

    unloadable_relations = [d for d in relations if d.is_unloadable]
    if not unloadable_relations:
        logger.warning("Found no relations that are unloadable.")
        return

    target_lookup = {schema.name: schema for schema in config.schemas if schema.is_an_unload_target}
    relation_target_tuples = []
    for relation in unloadable_relations:
        if relation.unload_target not in target_lookup:
            raise TableDesignSemanticError("Unload target specified, but not defined: '%s'" % relation.unload_target)
        relation_target_tuples.append((relation, target_lookup[relation.unload_target]))

    error_occurred = False
    conn = etl.pg.connection(config.dsn_etl, autocommit=True, readonly=True)
    with closing(conn) as conn:
        for i, (relation, unload_schema) in enumerate(relation_target_tuples):
            try:
                index = {"current": i+1, "final": len(relation_target_tuples)}
                unload_relation(conn, relation, unload_schema, config.iam_role, prefix, index,
                                allow_overwrite=allow_overwrite, dry_run=dry_run)
            except Exception as exc:
                if keep_going:
                    error_occurred = True
                    logger.warning("Unload failure for '%s'", relation.identifier)
                    logger.exception("Ignoring this exception and proceeding as requested:")
                else:
                    raise DataUnloadError(exc) from exc

    if error_occurred:
        raise ETLDelayedExit("At least one error occurred while unloading with 'keep going' option")
