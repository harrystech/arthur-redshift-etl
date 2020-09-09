"""
Base classes for preparing data to be loaded.

Extractors leave usable (ie, COPY-ready) manifests on S3 that reference data files.
"""
import concurrent.futures
import logging
from functools import partial
from itertools import groupby
from operator import attrgetter
from typing import Dict, List, Set

import etl.config
import etl.db
import etl.file_sets
import etl.monitor
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.errors import DataExtractError, ETLRuntimeError, MissingCsvFilesError, retry
from etl.relation import RelationDescription
from etl.text import join_with_quotes
from etl.timer import Timer


class Extractor:
    """
    The extractor base class provides the basic mechanics to
    * iterate over sources
      * iterate over tables in each source
        * call a child's class extract for a single table
    It is that method (`extract_table`) that child classes must implement.
    """

    def __init__(
        self,
        name: str,
        schemas: Dict[str, DataWarehouseSchema],
        relations: List[RelationDescription],
        keep_going: bool,
        needs_to_wait: bool,
        dry_run: bool,
    ) -> None:
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

    def options_info(self) -> List[str]:
        """
        Return list of "options" that describe the extract.

        This list will be part of the step monitor
        """
        return ["with-{0.name}-extractor".format(self)]

    @staticmethod
    def source_info(source: DataWarehouseSchema, relation: RelationDescription) -> Dict:
        """
        Return info for the job monitor that says from where the data is extracted.

        Defaults to the relation's idea of the source but may be overridden by child classes.
        """
        return {
            "name": relation.source_name,
            "schema": relation.source_table_name.schema,
            "table": relation.source_table_name.table,
        }

    def extract_source(
        self, source: DataWarehouseSchema, relations: List[RelationDescription]
    ) -> List[RelationDescription]:
        """For a given upstream source, iterate through given relations to extract the data."""
        self.logger.info("Extracting %d relation(s) from source '%s'", len(relations), source.name)
        failed = []
        extract_retries = etl.config.get_config_int("arthur_settings.extract_retries")
        with Timer() as timer:
            for i, relation in enumerate(relations):
                try:
                    extract_func = partial(self.extract_table, source, relation)
                    with etl.monitor.Monitor(
                        relation.identifier,
                        "extract",
                        options=self.options_info(),
                        source=self.source_info(source, relation),
                        destination={"bucket_name": relation.bucket_name, "object_key": relation.manifest_file_name},
                        index={"current": i + 1, "final": len(relations), "name": source.name},
                        dry_run=self.dry_run,
                    ):
                        retry(extract_retries, extract_func, self.logger)
                except ETLRuntimeError:
                    self.failed_sources.add(source.name)
                    failed.append(relation)
                    if not relation.is_required:
                        self.logger.warning(
                            "Extract failed for non-required relation '%s':", relation.identifier, exc_info=True
                        )
                    elif self.keep_going:
                        self.logger.warning(
                            "Ignoring failure of required relation '%s' and proceeding as requested:",
                            relation.identifier,
                            exc_info=True,
                        )
                    else:
                        self.logger.error("Extract failed for required relation '%s'", relation.identifier)
                        raise
            self.logger.info(
                "Finished extract from source '%s': %d succeeded, %d failed (%s)",
                source.name,
                len(relations) - len(failed),
                len(failed),
                timer,
            )
        return failed

    def extract_sources(self) -> None:
        """Iterate over sources to be extracted and parallelize extraction at the source level."""
        self.logger.info("Starting to extract %d relation(s) in %d schema(s)", len(self.relations), len(self.schemas))
        self.failed_sources.clear()
        max_workers = len(self.schemas)

        # TODO With Python 3.6, we should pass in a thread_name_prefix
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for source_name, relation_group in groupby(self.relations, attrgetter("source_name")):
                future = executor.submit(self.extract_source, self.schemas[source_name], list(relation_group))
                futures.append(future)
            if self.keep_going:
                done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
            else:
                done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)
        if self.failed_sources:
            self.logger.warning("Failed to extract from these source(s): %s", join_with_quotes(self.failed_sources))

        # Note that iterating over result of futures may raise an exception (which surfaces
        # exceptions from threads)
        missing_tables = []  # type: List
        for future in done:
            missing_tables.extend(future.result())

        if missing_tables:
            self.logger.warning(
                "Failed to extract %d relation(s): %s",
                len(missing_tables),
                join_with_quotes(table_name.identifier for table_name in missing_tables),
            )
        if not_done:
            raise DataExtractError("Extract failed to complete for {:d} source(s)".format(len(not_done)))

    def write_manifest_file(self, relation: RelationDescription, source_bucket: str, source_prefix: str) -> None:
        """
        Create manifest file to load all the data files for the given relation.

        The manifest file will be created in the folder ABOVE the data files.
        If the data files are in 'data/foo/bar/csv/part-r*',
        then the manifest is 'data/foo/bar.manifest'.

        Note that for static sources, we need to check the bucket of that source, not the
        bucket where the manifest will be written to.

        This will also test for the presence of the _SUCCESS file (added by map-reduce jobs).
        """
        self.logger.info("Preparing manifest file for data in 's3://%s/%s'", source_bucket, source_prefix)

        have_success = etl.s3.get_s3_object_last_modified(
            source_bucket, source_prefix + "/_SUCCESS", wait=self.needs_to_wait and not self.dry_run
        )
        if have_success is None:
            if self.dry_run:
                self.logger.warning("No valid data files (_SUCCESS is missing)")
            else:
                raise MissingCsvFilesError("No valid data files (_SUCCESS is missing)")

        data_files = sorted(etl.file_sets.find_data_files_in_s3(source_bucket, source_prefix))
        remote_paths = ["s3://{}/{}".format(source_bucket, filename) for filename in data_files]
        manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_paths]}

        if self.dry_run:
            if not remote_paths:
                self.logger.warning("Dry-run: Found no data files to add to manifest")
            else:
                self.logger.info(
                    "Dry-run: Skipping writing manifest file 's3://%s/%s' for %d data file(s)",
                    relation.bucket_name,
                    relation.manifest_file_name,
                    len(data_files),
                )
        else:
            if not remote_paths:
                raise MissingCsvFilesError("found no data files to add to manifest")

            self.logger.info(
                "Writing manifest file to 's3://%s/%s' for %d data file(s)",
                relation.bucket_name,
                relation.manifest_file_name,
                len(data_files),
            )
            etl.s3.upload_data_to_s3(manifest, relation.bucket_name, relation.manifest_file_name)

            # Make sure file exists before proceeding
            etl.s3.get_s3_object_last_modified(relation.bucket_name, relation.manifest_file_name, wait=True)
