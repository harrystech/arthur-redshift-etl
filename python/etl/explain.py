"""
Help developers "debug" their queries using the explain command.

In addition to showing the query plan, the output will also contain warnings
regarding costly distributions or creation of temporary tables.

See http://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html
"""

import logging
import re
from collections import Counter
from contextlib import closing
from typing import Dict, List

import etl.db
from etl.relation import RelationDescription

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

bad_distribution_styles = ["DS_DIST_INNER", "DS_BCAST_INNER", "DS_DIST_ALL_INNER", "DS_DIST_BOTH"]

leader_only_functions = [
    # System information functions
    "CURRENT_SCHEMA",
    "CURRENT_SCHEMAS",
    "HAS_DATABASE_PRIVILEGE",
    "HAS_SCHEMA_PRIVILEGE",
    "HAS_TABLE_PRIVILEGE",
    # The following leader-node only functions are deprecated: Date functions
    "AGE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "LOCALTIME",
    "ISFINITE",
    "NOW",
    # The following leader-node only functions are deprecated: String functions
    "ASCII",
    "GET_BIT",
    "GET_BYTE",
    "OCTET_LENGTH",
    "SET_BIT",
    "SET_BYTE",
    "TO_ASCII",
]

# The pattern matches while ignoring case if the name (or whatever is in {}) is not preceded
# by '.' and not in a word.
leader_only_compiled = {
    name: re.compile(r"(?i)(?<!\.)\b{}\b".format(name)) for name in leader_only_functions
}


def explain_queries(dsn: dict, relations: List[RelationDescription]) -> None:
    """
    Print query plans by running EXPLAIN on the queries.

    This also checks for distribution styles which cause a performance penalty.
    When the query plan consists of multiple query plans (which means, there will be
    temporary tables created), warn about that as well. A query plan has multiple sub-queries
    when the query plan has empty lines separating out the sub-queries.

    The queries are tested for "leader-only" functions which may harm performance as well.
    Since it's a simple string match, there might be false positives, e.g. when the function's
    use is actually commented out.  (A query containing `-- group users by age` will be flagged.)

    See http://docs.aws.amazon.com/redshift/latest/dg/c_data_redistribution.html
    and http://docs.aws.amazon.com/redshift/latest/dg/c_SQL_functions_leader_node_only.html
    """
    transforms = [relation for relation in relations if relation.sql_file_name is not None]
    if not transforms:
        logger.info("No transformations were selected")
        return

    queries_with_temps = 0
    counter: Dict[str, int] = Counter()

    # We can't use a read-only connection here because Redshift needs to (or wants to) create
    # temporary tables when building the query plan if temporary tables (probably from CTEs)
    # will be needed during query execution.  (Look for scans on volt_tt_* tables.)
    with closing(etl.db.connection(dsn, autocommit=True)) as conn:
        for relation in transforms:
            logger.info("Retrieving query plan for '%s'", relation.identifier)
            plan = etl.db.explain(conn, relation.query_stmt)
            print(
                "Query plan for query of '{0.identifier}':\n | {1}".format(
                    relation, "\n | ".join(plan)
                )
            )
            if any(row == "" for row in plan):
                queries_with_temps += 1
            for ds in bad_distribution_styles:
                if any(ds in row for row in plan):
                    counter[ds] += 1
            # Poor man's detection of comments ... drop anything after '--' on every line.
            lines = [line.split("--", 1)[0] for line in relation.query_stmt.split("\n)")]
            for name in leader_only_compiled:
                if any(leader_only_compiled[name].search(line) for line in lines):
                    counter[name] += 1
    if queries_with_temps:
        logger.warning("Found creation of temporary tables %d time(s)", queries_with_temps)
    for ds in bad_distribution_styles:
        if counter[ds]:
            logger.warning("Found %s distribution style %d time(s)", ds, counter[ds])
    for name in leader_only_functions:
        if counter[name]:
            logger.warning("Found leader-only function '%s' %d time(s)", name, counter[name])
