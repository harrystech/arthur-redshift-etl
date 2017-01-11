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

from etl.file_sets import TableFileSet
from etl.config import DataWarehouseConfig, DataWarehouseSchema
from etl.relation import RelationDescription
from etl.thyme import Thyme
import etl
import etl.file_sets
import etl.monitor
import etl.pg


class DataUnloadError(etl.ETLError):
    pass


def run_redshift_unload(conn: connection, description: RelationDescription, unload_path: str, aws_iam_role: str,
                        allow_overwrite=False) -> None:
    """
    Execute the UNLOAD command for the given select statement.
    Optionally allow users to overwrite previously unloaded data within the same key space.
    """
    logger = logging.getLogger(__name__)

    select_statement = """
        SELECT {}
        FROM {}
        """.format(", ".join(description.columns), description.table_design["name"])
    credentials = "aws_iam_role={}".format(aws_iam_role)
    unload_statement = """
        UNLOAD ('{}')
        TO '{}'
        CREDENTIALS '{}' MANIFEST
        DELIMITER ',' ESCAPE ADDQUOTES GZIP
        """.format(select_statement, unload_path, credentials)
    if allow_overwrite:
        unload_statement += "ALLOWOVERWRITE"

    with etl.pg.log_error():
        etl.pg.execute(conn, unload_statement)


def write_success_file(bucket_name: str, prefix: str, dry_run: bool=False) -> None:
    """
    Write out a "_SUCCESS" file into the same folder as the CSV files to mark
    the unload as complete.  The dump insists on this file before writing a manifest for load.
    """
    object_key = os.path.join(prefix, "_SUCCESS")
    etl.file_sets.create_empty_object(bucket_name, object_key, dry_run=dry_run)


def unload_redshift_relation(conn: connection, description: RelationDescription, schema: DataWarehouseSchema,
                             aws_iam_role: str, prefix: str, allow_overwrite=False, dry_run=False) -> None:
    """
    Unload data from table in the data warehouse using the UNLOAD command of Redshift.
    """
    logger = logging.getLogger(__name__)

    # TODO Move the "{}-{}" logic into the RelationDescription? Or all of this for "csv_folder()"?
    schema_table_name = "{}-{}".format(description.target_table_name.schema, description.target_table_name.table)
    rendered_template = Thyme.render_template(prefix, schema.s3_unload_path_template)
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
    write_success_file(schema.s3_bucket, s3_key_prefix, dry_run=dry_run)


def unload_to_s3(config: DataWarehouseConfig, file_sets: List[TableFileSet], prefix: str, allow_overwrite: bool,
                 keep_going: bool, dry_run: bool) -> None:
    """
    Create CSV files for selected tables based on the S3 path in an "unload" source.
    """
    logger = logging.getLogger(__name__)
    logger.info("Collecting all table information before unload")
    descriptions = RelationDescription.from_file_sets(file_sets)
    unloadable_relations = [d for d in descriptions if d.is_unloadable]
    if not unloadable_relations:
        logger.warning("Found no relations that are unloadable.")
        return

    conn = etl.pg.connection(config.dsn_etl, readonly=True)
    with closing(conn) as conn:
        for relation in unloadable_relations:
            try:
                # Find matching schema to this relation's unload target ... there should be exactly one match
                [unload_schema] = [schema for schema in config.schemas if schema.name == relation.unload_target]
                unload_redshift_relation(conn, relation, unload_schema, config.iam_role, prefix,
                                         allow_overwrite=allow_overwrite, dry_run=dry_run)
            except DataUnloadError:
                if keep_going:
                    logger.warning("Unload failure for '%s'", relation.identifier)
                    logger.exception("Ignoring this exception and proceeding as requested:")
                else:
                    raise
