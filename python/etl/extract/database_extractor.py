"""
DatabaseExtractors query upstream databases and save their data on S3 before writing manifests
"""
from contextlib import closing
from typing import Dict, List, Optional

import etl.db
from etl.config.dw import DataWarehouseSchema
from etl.extract.extractor import Extractor
from etl.relation import RelationDescription


class DatabaseExtractor(Extractor):
    """
    Special super class for database extractors that stores parameters and helps with partitioning and sampling.
    """

    def __init__(
        self,
        name: str,
        schemas: Dict[str, DataWarehouseSchema],
        relations: List[RelationDescription],
        max_partitions: int,
        use_sampling: bool,
        keep_going: bool,
        dry_run: bool,
    ) -> None:
        super().__init__(name, schemas, relations, keep_going, needs_to_wait=True, dry_run=dry_run)
        self.max_partitions = max_partitions
        self.use_sampling = use_sampling

    def options_info(self) -> List[str]:
        info = super().options_info()
        info.append("max-partitions={}".format(self.max_partitions))
        info.append("use-sampling={}".format(self.use_sampling))
        return info

    def use_sampling_with_table(self, size: int) -> bool:
        """
        Return True iff option `--use-sampling` appeared and table is large enough (> 1MB).
        """
        return self.use_sampling and (size > 1024 ** 2)

    def select_min_partition_size(self, size: int) -> int:
        """
        Return min partition size to stay above when calculating the number of partitions.

        Redshift documentation suggests to stay above 1MB for data files. Assuming that the CSV
        files can be compressed 1:10 and that sampling will reduce that 1:10, then we have:
            * with sampling: 100MB
            * w/o sampling: 10MB
        """
        if self.use_sampling_with_table(size):
            return 100 * 1024 ** 2
        else:
            return 10 * 1024 ** 2

    def maximize_partitions(self, table_size: int) -> int:
        """
        Determine the maximum number of row-wise partitions a table can be divided into while respecting a minimum
        partition size, and a limit on the number of partitions.

        DatabaseExtractors often need to partition the input so that multiple smaller parts can be operated on in
        parallel.

        Given a table size (in bytes), the maximum number of partitions to divide the table, and the minimum partition
        size (in bytes), return the number of partitions.

        >>> extractor = DatabaseExtractor("test", {}, [], 64, use_sampling=False, keep_going=False, dry_run=True)
        >>> extractor.maximize_partitions(1)
        1
        >>> extractor.maximize_partitions(10485750)
        1
        >>> extractor.maximize_partitions(10485760)
        1
        >>> extractor.maximize_partitions(10485770)
        1
        >>> extractor.maximize_partitions(20971510)
        1
        >>> extractor.maximize_partitions(20971520)
        2
        >>> extractor.maximize_partitions(671088630)
        63
        >>> extractor.maximize_partitions(671088640)
        64
        >>> extractor.maximize_partitions(671088650)
        64
        >>> extractor.maximize_partitions(470958407680)
        64
        >>> extractor.maximize_partitions(0)
        1
        """
        min_partition_size = self.select_min_partition_size(table_size)
        partitions = self.max_partitions
        partition_size = table_size / partitions
        while partition_size < min_partition_size and partitions > 1:
            partitions -= 1
            partition_size = table_size / partitions

        self.logger.debug(
            "Number of partitions: %d (max: %d), partition size: %d (table size: %d, min size: %d)",
            partitions,
            self.max_partitions,
            int(partition_size),
            table_size,
            min_partition_size,
        )
        return partitions

    def select_statement(self, relation: RelationDescription, add_sampling_on_column: Optional[str]) -> str:
        """
        Return something like
            "SELECT id, name FROM table WHERE TRUE" or
            "SELECT id, name FROM table WHERE ((id % 10) = 1)"
        where the actual statement used delimited identifiers, but note the existence of the WHERE clause.
        """
        selected_columns = relation.get_columns_with_casts()
        statement = """SELECT {} FROM {}""".format(", ".join(selected_columns), relation.source_table_name)

        condition = relation.table_design.get("extract_settings", {}).get("condition", "TRUE")

        if add_sampling_on_column is None:
            statement += """ WHERE ({})""".format(condition)
        else:
            self.logger.info(
                "Adding sampling on column '%s' while extracting '%s.%s'",
                add_sampling_on_column,
                relation.source_name,
                relation.source_table_name.identifier,
            )
            statement += """ WHERE (({}) AND ("{}" % 10) = 1)""".format(condition, add_sampling_on_column)

        return statement

    def fetch_source_table_size(self, dsn_dict: Dict[str, str], relation: RelationDescription) -> int:
        """
        Return size or estimated size of source table for this relation in bytes.

        For source tables in a postgres database, fetch the actual size from pg_catalog tables.
        Otherwise, pessimistically estimate a large fixed size.
        """
        stmt = """
            SELECT pg_catalog.pg_table_size(%s) AS "bytes"
                 , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(%s)) AS pretty_size
            """
        table = relation.source_table_name
        subprotocol = dsn_dict["subprotocol"]
        if subprotocol.startswith("postgres"):
            with closing(etl.db.connection(dsn_dict, readonly=True)) as conn:
                rows = etl.db.query(conn, stmt, (str(table), str(table)))
            bytes_size, pretty_size = rows[0]["bytes"], rows[0]["pretty_size"]
            self.logger.info(
                "Size of table '%s.%s': %s (%s)", relation.source_name, table.identifier, bytes_size, pretty_size
            )
        else:
            bytes_size, pretty_size = 671088640, "671 Mb"
            self.logger.info(
                "Pessimistic size estimate for non-postgres table '%s.%s': %s (%s)",
                relation.source_name,
                table.identifier,
                bytes_size,
                pretty_size,
            )

        return bytes_size
