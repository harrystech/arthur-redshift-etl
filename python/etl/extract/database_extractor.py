"""
DatabaseExtractors query upstream databases and save their data on S3 before writing manifests
"""
from typing import Dict, List, Optional

from psycopg2.extensions import connection  # only for type annotation

import etl.pg
from etl.extract.extractor import Extractor
from etl.config.dw import DataWarehouseSchema
from etl.relation import RelationDescription


class DatabaseExtractor(Extractor):
    """
    Special super class for database extractors that stores parameters and helps with partitioning and sampling.
    """

    def __init__(self, name: str, schemas: Dict[str, DataWarehouseSchema], relations: List[RelationDescription],
                 max_partitions: int, use_sampling: bool, keep_going: bool, dry_run: bool) -> None:
        super().__init__(name, schemas, relations, keep_going, needs_to_wait=True, dry_run=dry_run)
        self.max_partitions = max_partitions
        self.use_sampling = use_sampling

    def use_sampling_with_table(self, size: int) -> bool:
        """
        Return True iff option `--use-sampling` appeared and table is large enough (> 1MB).
        """
        return self.use_sampling and (size > 1024 ** 2)

    def select_statement(self, relation: RelationDescription, add_sampling_on_column: Optional[str]) -> str:
        """
        Return something like
            "SELECT id, name FROM table WHERE TRUE" or
            "SELECT id, name FROM table WHERE ((id % 10) = 1)"
        where the actual statement used delimited identifiers, but note the existence of the WHERE clause.
        """
        selected_columns = relation.get_columns_with_casts()
        statement = """SELECT {} FROM {}""".format(", ".join(selected_columns), relation.source_table_name)
        if add_sampling_on_column is None:
            statement += " WHERE TRUE"
        else:
            self.logger.info("Adding sampling on column '%s' while extracting '%s.%s'",
                             add_sampling_on_column, relation.source_name, relation.source_table_name.identifier)
            statement += """ WHERE (("{}" % 10) = 1)""".format(add_sampling_on_column)
        return statement

    def fetch_source_table_size(self, conn: connection, relation: RelationDescription) -> int:
        """
        Return size of source table for this relation in bytes
        """
        stmt = """
            SELECT pg_catalog.pg_table_size(%s) AS "bytes"
                 , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(%s)) AS pretty_size
            """
        table = relation.source_table_name
        rows = etl.pg.query(conn, stmt, (str(table), str(table)))
        bytes_size, pretty_size = rows[0]["bytes"], rows[0]["pretty_size"]
        self.logger.info("Size of table '%s.%s': %s (%s)",
                         relation.source_name, table.identifier, bytes_size, pretty_size)
        return bytes_size
