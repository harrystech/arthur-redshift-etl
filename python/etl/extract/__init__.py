"""
Extract data from an upsteam source into S3.

An "extract" refers to the wholesale extraction of data from (a) given relation(s) in some number
of upstream sources. The data is stored in gzipped CSV form, into a specified keyspace in S3.

(1) There are two main types of upstream sources: static sources and database sources.

    (a) Static sources consist largely of historical data that is not subject to
        change. Static upstream source are already in gzipped CSV format, and no
        database connection is required. With static upstream sources, the extract
        is simply looking to verify the data exists.
    (b) Database sources consist of raw data that is changing frequently. Here
        we require a database connection to query and extract data from these sources
        and write them out to gzipped CSVs. There are two extract tools that can be
        used for database sources: spark or sqoop. Sqoop is the default extractor.

    Once the data has been extracted, the "extract" job checks for a _SUCCESS file. If
    this file is not present in the same keyspace as the data, the "extract" is considered
    failed, meaning a manifest file will not be written and and error will be logged. If the
    "--keep-going" flag was used, the failure of a single relation will not hard fail the
    entire extract job.

(2) Every successful extract writes out a manifest file to the specified S3 keyspace that
    contains a list of the location of the extracted data.
"""
import logging
from typing import List, Dict

import etl
from etl.config import DataWarehouseSchema
from etl.extract.errors import DataExtractError
from etl.extract.extractor import Extractor
import etl.extract.spark
from etl.extract.spark import SparkExtractor
import etl.extract.sqoop
from etl.extract.sqoop import SqoopExtractor
from etl.extract.static import StaticExtractor
import etl.pg
from etl.relation import RelationDescription


def extract_upstream_sources(extract_type: str, schemas: List[DataWarehouseSchema],
                             descriptions: List[RelationDescription], max_partitions: int,
                             keep_going: bool, dry_run: bool) -> None:
    """
    Extract data from an upstream source to S3

    This is the entry point, and which technology will be used will be determined
    by the args here. Static sources are processed first, followed by database
    sources.
    """
    logger = logging.getLogger(__name__)

    static_sources = {source.name: source for source in schemas if source.is_static_source}
    if static_sources:
        descs = filter_relations_for_sources(static_sources, descriptions)
        static_extractor = StaticExtractor(static_sources, descs, keep_going, dry_run, wait=False)
        static_extractor.extract_sources()
    else:
        logger.info("No static sources were selected")

    database_sources = {source.name: source for source in schemas if source.is_database_source}
    if not database_sources:
        logger.info("No database sources were selected")
        return

    with etl.pg.log_error():
        descs = filter_relations_for_sources(database_sources, descriptions)
        if extract_type == "spark":
            database_extractor = SparkExtractor(database_sources, descs, keep_going, dry_run)
        else:
            database_extractor = SqoopExtractor(max_partitions, database_sources, descs, keep_going, dry_run)
        database_extractor.extract_sources()


def filter_relations_for_sources(source_lookup: Dict[str, DataWarehouseSchema],
                                 descriptions: List[RelationDescription]) -> List[RelationDescription]:
    """
    Filter for the relations that a given "extract" cares about.
    """
    logger = logging.getLogger(__name__)
    descs = [d for d in descriptions if d.source_name in source_lookup]
    sources = set([d.source_name for d in descs])
    logging.info("Found %d relation(s) for %s sources", len(descs), ', '.join(sources))
    return descs
