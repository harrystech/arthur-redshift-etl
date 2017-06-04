"""
Base classes for preparing data to be loaded

Extractors leave usable (ie, COPY-ready) manifests on S3 that reference data files

DBExtractors query upstream databases and save their data on S3 before writing manifests
"""

import concurrent.futures
from contextlib import closing
from itertools import groupby
import logging
from operator import attrgetter
from typing import Dict, List, Set

from psycopg2.extensions import connection  # only for type annotation

import etl.monitor
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.errors import DataExtractError, ETLRuntimeError, MissingCsvFilesError, UnknownTableSizeError
from etl.names import TableName, join_with_quotes
from etl.relation import RelationDescription
from etl.timer import Timer


class Extractor:
    """
    The extractor base class provides the basic mechanics to
    * iterate over sources
      * iterate over tables in each source
        * call a child's class extract for a single table
    It is that method (`extract_table`) that child classes must implement.
    """

    def __init__(self, name: str, schemas: Dict[str, DataWarehouseSchema], relations: List[RelationDescription],
                 keep_going: bool, needs_to_wait: bool, dry_run: bool) -> None:
        self.name = name
        self.schemas = schemas
        self.relations = relations
        self.keep_going = keep_going
        # Decide whether we should wait for some application to finish extracting and writing a success file or
        # whether we can proceed immediately when testing for presence of that success file.
        self.needs_to_wait = needs_to_wait
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        self.failed_sources = set()  # type: Set[str]

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription):
        raise NotImplementedError("Forgot to implement extract_table in {}".format(self.__class__.__name__))

    @staticmethod
    def source_info(source: DataWarehouseSchema, relation: RelationDescription) -> Dict:
        """
        Return info for the job monitor that says from where the data is extracted.
        Defaults to the relation's idea of the source but may be overridden by child classes.
        """
        return {'name': relation.source_name,
                'schema': relation.source_table_name.schema,
                'table': relation.source_table_name.table}

    def extract_source(self, source: DataWarehouseSchema,
                       relations: List[RelationDescription]) -> List[RelationDescription]:
        """
        For a given upstream source, iterate through given relations to extract the relations' data.
        """
        self.logger.info("Extracting %d relation(s) from source '%s'", len(relations), source.name)
        failed = []

        with Timer() as timer:
            for i, relation in enumerate(relations):
                try:
                    with etl.monitor.Monitor(relation.identifier,
                                             "extract",
                                             options=["with-{0.name}-extractor".format(self)],
                                             source=self.source_info(source, relation),
                                             destination={'bucket_name': relation.bucket_name,
                                                          'object_key': relation.manifest_file_name},
                                             index={"current": i + 1, "final": len(relations), "name": source.name},
                                             dry_run=self.dry_run):
                        self.extract_table(source, relation)
                except ETLRuntimeError:
                    self.failed_sources.add(source.name)
                    failed.append(relation)
                    if not relation.is_required:
                        self.logger.exception("Extract failed for non-required relation '%s':", relation.identifier)
                    elif self.keep_going:
                        self.logger.exception("Ignoring failure of required relation '%s' and proceeding as requested:",
                                              relation.identifier)
                    else:
                        self.logger.debug("Extract failed for required relation '%s'", relation.identifier)
                        raise
            self.logger.info("Finished extract from source '%s': %d succeeded, %d failed (%s)",
                             source.name, len(relations) - len(failed), len(failed), timer)
        return failed

    def extract_sources(self) -> None:
        """
        Iterate over sources to be extracted and parallelize extraction at the source level
        """
        self.logger.info("Starting to extract %d relation(s)", len(self.relations))
        self.failed_sources.clear()
        # FIXME We need to evaluate whether extracting from multiple sources in parallel works with Spark!
        max_workers = len(self.schemas)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for source_name, relation_group in groupby(self.relations, attrgetter("source_name")):
                f = executor.submit(self.extract_source, self.schemas[source_name], list(relation_group))
                futures.append(f)
            if self.keep_going:
                done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
            else:
                done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)
        if self.failed_sources:
            self.logger.error("Failed to extract from these source(s): %s", join_with_quotes(self.failed_sources))

        # Note that iterating over result of futures may raise an exception (which surfaces exceptions from threads)
        missing_tables = []  # type: List
        for future in done:
            missing_tables.extend(future.result())
        for table_name in missing_tables:
            self.logger.warning("Failed to extract: '%s'", table_name.identifier)
        if not_done:
            raise DataExtractError("Extract failed to complete for {:d} source(s)".format(len(not_done)))

    def write_manifest_file(self, relation: RelationDescription, source_bucket: str, prefix: str) -> None:
        """
        Create manifest file to load all the CSV files for the given relation.
        The manifest file will be created in the folder ABOVE the CSV files.

        If the data files are in 'data/foo/bar/csv/part-r*', then the manifest is 'data/foo/bar.manifest'.

        Note that for static sources, we need to check the bucket of that source, not the
        bucket where the manifest will be written to.

        This will also test for the presence of the _SUCCESS file (added by map-reduce jobs).
        """
        self.logger.info("Preparing manifest file for data in 's3://%s/%s'", source_bucket, prefix)

        have_success = etl.s3.get_s3_object_last_modified(source_bucket, prefix + "/_SUCCESS",
                                                          wait=self.needs_to_wait and not self.dry_run)
        if have_success is None:
            if self.dry_run:
                self.logger.warning("No valid CSV files (_SUCCESS is missing)")
            else:
                raise MissingCsvFilesError("No valid CSV files (_SUCCESS is missing)")

        csv_files = sorted(key for key in etl.s3.list_objects_for_prefix(source_bucket, prefix)
                           if "part" in key and key.endswith(".gz"))
        remote_files = ["s3://{}/{}".format(source_bucket, filename) for filename in csv_files]
        manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}

        if self.dry_run:
            if not manifest:
                self.logger.warning("Dry-run: Found no CSV files")
            else:
                self.logger.info("Dry-run: Skipping writing manifest file 's3://%s/%s'",
                                 relation.bucket_name, relation.manifest_file_name)
        else:
            if not manifest:
                raise MissingCsvFilesError("Found no CSV files")
            else:
                self.logger.info("Writing manifest file to 's3://%s/%s'",
                                 relation.bucket_name, relation.manifest_file_name)
                etl.s3.upload_data_to_s3(manifest, relation.bucket_name, relation.manifest_file_name)


class DBExtractor(Extractor):
    """
    Extend extractors with common functionality for dealing with upstream databases
    """

    def suggest_num_partitions(self, source_table_name: TableName, read_access: Dict[str, str]) -> int:
        """
        Guesstimate number of partitions based on actual table size
        """
        self.logger.debug("Determining partitioning for table '%s'", source_table_name.identifier)

        with closing(etl.pg.connection(read_access, readonly=True)) as conn:
            table_size = self.fetch_table_size(conn, source_table_name)

        num_partitions = suggest_best_partition_number(table_size)
        return num_partitions

    def fetch_table_size(self, conn: connection, table_name: TableName) -> int:
        """
        Fetch table size in bytes.
        """
        # TODO move this into pg.py
        stmt = """
            SELECT pg_catalog.pg_table_size('{}') AS table_size
                 , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size('{}')) AS pretty_table_size
        """
        with Timer() as timer:
            rows = etl.pg.query(conn, stmt.format(table_name, table_name))
        if len(rows) != 1:
            raise UnknownTableSizeError("Failed to determine size of table")
        table_size, pretty_table_size = rows[0]["table_size"], rows[0]["pretty_table_size"]
        self.logger.info("Size of table '%s': %s (%d bytes) (%s)", table_name.identifier, pretty_table_size, table_size,
                         timer)
        return table_size


def suggest_best_partition_number(table_size: int) -> int:
    """
    Suggest number of partitions based on the table size (in bytes).  Number of partitions is always
    a factor of 2.

    The number of partitions is based on:
      Small tables (<= 10M): Use partitions around 1MB.
      Medium tables (<= 1G): Use partitions around 10MB.
      Huge tables (> 1G): Use partitions around 20MB.

    >>> suggest_best_partition_number(100)
    1
    >>> suggest_best_partition_number(1048576)
    1
    >>> suggest_best_partition_number(3 * 1048576)
    2
    >>> suggest_best_partition_number(10 * 1048576)
    8
    >>> suggest_best_partition_number(100 * 1048576)
    8
    >>> suggest_best_partition_number(200 * 1048576)
    16
    >>> suggest_best_partition_number(2000 * 1048576)
    64
    """
    meg = 1024 * 1024
    if table_size <= 10 * meg:
        target = 1 * meg
    elif table_size <= 1024 * meg:
        target = 10 * meg
    else:
        target = 20 * meg

    num_partitions = 1
    partition_size = table_size
    # Keep the partition sizes above the target value:
    while partition_size >= target * 2 and num_partitions < 1024:
        num_partitions *= 2
        partition_size //= 2

    return num_partitions
