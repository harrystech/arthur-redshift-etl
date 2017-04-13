"""
Help developers "debug" their queries using the explain command.

In addition to showing the query plan, the output will also contain warnings
regarding costly distributions or creation of temporary tables.

See http://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html
"""

from contextlib import closing
from collections import Counter
import logging
from typing import Dict, List

from etl.relation import RelationDescription
import etl.pg

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def explain_queries(dsn: dict, descriptions: List[RelationDescription]) -> None:
    """
    Print query plans by running EXPLAIN on the queries.

    This also checks for distribution styles which cause a performance penalty.
    When the query plan consists of multiple query plans (which means, there will be
    temporary tables created), warn about that as well. A query plan has multiple sub-queries
    when the query plan has empty lines separating out the sub-queries.
    """
    transforms = [description for description in descriptions if description.sql_file_name is not None]
    if not transforms:
        logger.info("No transformations were selected")
        return

    bad_distribution_styles = ["DS_DIST_INNER", "DS_BCAST_INNER", "DS_DIST_ALL_INNER", "DS_DIST_BOTH"]
    counter = Counter()  # type: Dict[str, int]
    queries_with_temps = 0

    # We can't use a read-only connection here because Redshift needs to (or wants to) create
    # temporary tables when building the query plan if temporary tables (probably from CTEs)
    # will be needed during query execution.  (Look for scans on volt_tt_* tables.)
    with closing(etl.pg.connection(dsn, autocommit=True)) as conn:
        for description in transforms:
            logger.info("Testing query for '%s'", description.identifier)
            plan = etl.pg.explain(conn, description.query_stmt)
            print("Explain plan for query of '{0.identifier}':\n | {1}".format(description, "\n | ".join(plan)))
            if any(row == "" for row in plan):
                queries_with_temps += 1
            for row in plan:
                for ds in bad_distribution_styles:
                    if ds in row:
                        counter[ds] += 1
    for ds in bad_distribution_styles:
        if counter[ds]:
            logger.warning("Found %s distribution style %d time(s)", ds, counter[ds])
    if queries_with_temps:
        logger.warning("Found creation of temporary tables %d time(s)", queries_with_temps)
