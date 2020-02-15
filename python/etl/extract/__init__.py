"""
Extract data from an upstream source into S3.

An "extract" refers to the wholesale extraction of data from relation(s) in some number
of upstream sources. The data is stored in gzipped CSV form, into a specified keyspace in S3.

(1) There are two main types of upstream sources: static sources and database sources.

    (a) Static sources use S3 and have the data ready in gzipped CSV files. No
        database connection is required. With these sources, the extract
        is simply looking to verify that the data exists. An example use case
        is storing historic data in S3 which is no longer in live database sources.
    (b) Database sources are tied to tables with data that is changing frequently. Here
        we require a database connection to query, dump data from these sources
        and write them out to gzipped CSV files. There are two extract tools that can be
        used for database sources: Spark or Sqoop.

    Once the data has been extracted, the "extract" job checks for a _SUCCESS file. If
    this file is not present in the same keyspace as the data, the "extract" is considered
    to have failed, meaning a manifest file will not be written and an error will be logged.

    If the "--keep-going" flag is used, the failure of a single relation will not kill the
    entire extract job.

(2) Every successful extract writes out a manifest file to the specified S3 keyspace that
    contains a list of the locations of the extracted data.
"""

import logging
from typing import Dict, List

from etl.config.dw import DataWarehouseSchema
from etl.extract.extractor import Extractor
from etl.extract.manifest_only import ManifestOnlyExtractor
from etl.extract.spark import SparkExtractor
from etl.extract.sqoop import SqoopExtractor
from etl.extract.static import StaticExtractor
from etl.relation import RelationDescription
from etl.text import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def extract_upstream_sources(extract_type: str,
                             schemas: List[DataWarehouseSchema], relations: List[RelationDescription],
                             max_partitions: int, use_sampling=False, keep_going=False,
                             dry_run=False) -> None:
    """
    Extract data from upstream sources to S3.

    This is the entry point. Static sources are processed first, followed by database
    sources for which we determine the extraction technology here.
    """
    static_sources = {source.name: source for source in schemas if source.is_static_source}
    applicable = filter_relations_for_sources(static_sources, relations)
    total_relations = len(applicable)
    if applicable:
        static_extractor = StaticExtractor(static_sources, applicable, keep_going, dry_run)
        static_extractor.extract_sources()
    else:
        logger.info("No static sources were selected")

    database_sources = {source.name: source for source in schemas if source.is_database_source}
    applicable = filter_relations_for_sources(database_sources, relations)
    total_relations += len(applicable)
    if not applicable:
        logger.info("No database sources were selected")
        if total_relations == 0:
            logger.warning("Found no matching relations for your selection")
        return

    if extract_type == "manifest-only":
        database_extractor = ManifestOnlyExtractor(database_sources, applicable, keep_going, dry_run)  # type: Extractor
    elif extract_type == "spark":
        database_extractor = SparkExtractor(database_sources, applicable,
                                            max_partitions=max_partitions,
                                            use_sampling=use_sampling,
                                            keep_going=keep_going,
                                            dry_run=dry_run)
    else:
        database_extractor = SqoopExtractor(database_sources, applicable,
                                            max_partitions=max_partitions,
                                            use_sampling=use_sampling,
                                            keep_going=keep_going,
                                            dry_run=dry_run)
    database_extractor.extract_sources()


def filter_relations_for_sources(source_lookup: Dict[str, DataWarehouseSchema],
                                 relations: List[RelationDescription]) -> List[RelationDescription]:
    """
    Filter for the relations that a given "extract" stage cares about.
    """
    selected = [relation for relation in relations if relation.source_name in source_lookup]
    if selected:
        sources = frozenset(relation.source_name for relation in selected)
        logger.info("Selected %d relation(s) from source(s): %s", len(selected), join_with_quotes(sources))
    return selected
