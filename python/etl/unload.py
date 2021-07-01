"""
Unload data from data warehouse to S3.

A "unload" refers to the wholesale dumping of data from any schema or table involved into
some number of gzipped CSV files to a given S3 destination. The resulting data files
can be loaded back into the data warehouse using the "load" step.

(1) CSV files will have fields delimited by commas, quotes around fields if they
    contain a comma, and have doubled-up quotes if there's a quote within the field.

    Data format parameters: DELIMITER ',' ESCAPE REMOVEQUOTES GZIP ALLOWOVERWRITE

(2) Every unload will be accompanied by a manifest containing a complete list of CSVs.

(3) Every unload will write a "_SUCCESS" file to mark successful completion.

(4) Every unload will also write a YAML file containing a complete list of all columns from the
    relation from which the data was dumped to CSV.
"""

import logging
from contextlib import closing
from typing import List

from psycopg2.extensions import connection as Connection  # only for type annotation

import etl
import etl.db
import etl.dialect.redshift
import etl.monitor
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.errors import DataUnloadError, ETLDelayedExit, TableDesignSemanticError
from etl.relation import RelationDescription

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def write_columns_file(relation: RelationDescription, bucket_name: str, prefix: str, dry_run: bool) -> None:
    """Write out a YAML file into the same folder as the CSV files with a list of the columns."""
    data = {
        "columns": relation.unquoted_columns,
        "columns_with_types": relation.get_columns_with_types(),
    }
    object_key = f"{prefix}/columns.yaml"
    if dry_run:
        logger.info("Dry-run: Skipping writing columns file to 's3://%s/%s'", bucket_name, object_key)
        return

    logger.info("Writing columns file to 's3://%s/%s'", bucket_name, object_key)
    etl.s3.upload_data_to_s3(data, bucket_name, object_key)
    # This waits for the file to show up so that we don't move on before all files are ready.
    etl.s3.wait_until_object_exists(bucket_name, object_key)


def write_success_file(bucket_name: str, prefix: str, dry_run=False) -> None:
    """
    Write out a "_SUCCESS" file into the same folder as the CSV files.

    The sentinel file marks the unload as complete. The dump insists on this file before
    writing a manifest for load.
    """
    object_key = f"{prefix}/_SUCCESS"
    if dry_run:
        logger.info("Dry-run: Skipping creation of 's3://%s/%s'", bucket_name, object_key)
        return

    logger.info("Creating 's3://%s/%s'", bucket_name, object_key)
    etl.s3.upload_empty_object(bucket_name, object_key)


def unload_relation(
    conn: Connection,
    relation: RelationDescription,
    schema: DataWarehouseSchema,
    index: dict,
    allow_overwrite=False,
    dry_run=False,
) -> None:
    """Unload data from table in the data warehouse using the UNLOAD command of Redshift."""
    s3_key_prefix = "{schema.s3_unload_path_prefix}/data/{schema.name}/{source.schema}-{source.table}/csv".format(
        schema=schema,
        source=relation.target_table_name,
    )
    unload_path = f"s3://{schema.s3_bucket}/{s3_key_prefix}/"
    aws_iam_role = str(etl.config.get_config_value("object_store.iam_role"))

    with etl.monitor.Monitor(
        relation.identifier,
        "unload",
        source={"schema": relation.target_table_name.schema, "table": relation.target_table_name.table},
        destination={"name": schema.name, "bucket_name": schema.s3_bucket, "prefix": s3_key_prefix},
        index=index,
        dry_run=dry_run,
    ):
        if dry_run:
            logger.info("Dry-run: Skipping unload of '%s' to '%s'", relation.identifier, unload_path)
            return

        etl.dialect.redshift.unload(
            conn,
            relation.target_table_name,
            relation.columns,
            unload_path,
            aws_iam_role,
            allow_overwrite=allow_overwrite,
        )
        write_columns_file(relation, schema.s3_bucket, s3_key_prefix, dry_run=dry_run)
        write_success_file(schema.s3_bucket, s3_key_prefix, dry_run=dry_run)


def unload_to_s3(
    relations: List[RelationDescription],
    allow_overwrite: bool,
    keep_going: bool,
    dry_run: bool,
) -> None:
    """Create CSV files for selected tables based on the S3 path in an "unload" source."""
    logger.info("Loading table design for %d relation(s) to look for unloadable relations", len(relations))
    etl.relation.RelationDescription.load_in_parallel(relations)

    unloadable_relations = [relation for relation in relations if relation.is_unloadable]
    if not unloadable_relations:
        logger.warning("Found no relations that are unloadable.")
        return
    logger.info("Starting to unload %s relation(s)", len(unloadable_relations))

    config = etl.config.get_dw_config()
    target_lookup = {schema.name: schema for schema in config.schemas if schema.is_an_unload_target}
    relation_target_tuples = []
    for relation in unloadable_relations:
        if relation.unload_target not in target_lookup:
            raise TableDesignSemanticError(f"unload target specified, but not defined: '{relation.unload_target}'")
        relation_target_tuples.append((relation, target_lookup[relation.unload_target]))

    errors_occurred = 0
    conn = etl.db.connection(config.dsn_etl, autocommit=True, readonly=True)
    with closing(conn) as conn:
        for i, (relation, unload_schema) in enumerate(relation_target_tuples):
            try:
                index = {"current": i + 1, "final": len(relation_target_tuples)}
                unload_relation(conn, relation, unload_schema, index, allow_overwrite=allow_overwrite, dry_run=dry_run)
            except Exception as exc:
                if not keep_going:
                    raise DataUnloadError(exc) from exc
                errors_occurred += 1
                logger.warning("Unload failed for '%s'", relation.identifier)
                logger.exception("Ignoring this exception and proceeding as requested:")

    if errors_occurred > 0:
        raise ETLDelayedExit(f"unload encountered {errors_occurred} error(s) while running with 'keep going' option")
