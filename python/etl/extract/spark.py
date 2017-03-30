import logging
import os.path
from contextlib import closing
from typing import List, Dict, Tuple

from psycopg2.extensions import connection  # For type annotation

import etl.pg
from etl import TableName
from etl.config import DataWarehouseSchema
from etl.errors import UnknownTableSizeError
from etl.extract.extractor import Extractor
from etl.timer import Timer
from etl.relation import RelationDescription


class SparkExtractor(Extractor):

    def __init__(self, schemas: Dict[str, DataWarehouseSchema], descriptions: List[RelationDescription],
                 keep_going: bool, dry_run: bool):
        super().__init__("spark", schemas, descriptions, keep_going, needs_to_wait=True, dry_run=dry_run)
        self.logger = logging.getLogger(__name__)
        self._sql_context = None

    @property
    def sql_context(self):
        if self._sql_context is None:
            self._sql_context = self._create_sql_context()
        return self._sql_context

    def _create_sql_context(self):
        """
        Create a new SQL context within a new Spark context. Import of classes from
        pyspark has to be pushed down into the method as Spark needs to be available
        in order for the libraries to be imported. Since Spark is not available upon
        booting up an EMR cluster, an ImportError is served unless the import is
        deferred into the method.

        Side-effect: Logging is configured by the time that pyspark is loaded
        so we have some better control over filters and formatting.
        """
        from pyspark import SparkConf, SparkContext, SQLContext

        if "SPARK_ENV_LOADED" not in os.environ:
            self.logger.warning("SPARK_ENV_LOADED is not set")

        self.logger.info("Starting SparkSQL context")
        conf = SparkConf()
        conf.setAppName(__name__)
        conf.set("spark.logConf", "true")
        # TODO Add spark.jars here? spark.submit.pyFiles?
        sc = SparkContext(conf=conf)
        return SQLContext(sc)

    def extract_table(self, source: DataWarehouseSchema, description: RelationDescription):
        """
        Using Spark's dataframe API, read the table in as a dataframe before writing it
        out to CSV.
        """
        df = self._read_table_as_dataframe(source, description)
        self._write_dataframe_as_csv(df, description)
        prefix = os.path.join(description.prefix, description.csv_path_name)
        self.write_manifest_file(description, description.bucket_name, prefix)

    def _read_table_as_dataframe(self, source: DataWarehouseSchema, description: RelationDescription):
        """
        Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
        """
        jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

        source_table_name = description.source_table_name
        selected_columns = description.get_columns_with_casts()
        select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
        self.logger.debug("Table query: SELECT * FROM %s", select_statement)

        predicates = self._determine_partitioning(source_table_name, description, source.dsn)
        if predicates:
            df = self.sql_context.read.jdbc(url=jdbc_url,
                                            properties=dsn_properties,
                                            table=select_statement,
                                            predicates=predicates)
        else:
            df = self.sql_context.read.jdbc(url=jdbc_url,
                                            properties=dsn_properties,
                                            table=select_statement)
        return df

    def _determine_partitioning(self, source_table_name: TableName, description: RelationDescription,
                                read_access: Dict[str, str]) -> List[str]:
        """
        Guesstimate number of partitions based on actual table size and create list of predicates to split
        up table into that number of partitions.

        This requires for one column to be marked as the primary key.  If there's no primary
        key in the table, the number of partitions is always one.
        (This requirement doesn't come from the table size but the need to split the table
        when reading it in.)
        """
        primary_key = description.find_primary_key()
        if "primary_key" is None:
            self.logger.info("No primary key defined for '%s', skipping partitioning", source_table_name.identifier)
            return []
        self.logger.debug("Primary key for table '%s' is '%s'", source_table_name.identifier, primary_key)

        predicates = []
        with closing(etl.pg.connection(read_access, readonly=True)) as conn:
            self.logger.debug("Determining predicates for table '%s'", source_table_name.identifier)

            table_size = self._fetch_table_size(conn, source_table_name)
            num_partitions = self._suggest_best_partition_number(table_size)
            self.logger.info("Picked %d partition(s) for table '%s' (primary key: '%s')",
                             num_partitions, source_table_name.identifier, primary_key)

            if num_partitions > 1:
                boundaries = self._fetch_partition_boundaries(conn, source_table_name, primary_key, num_partitions)
                for low, high in boundaries:
                    predicates.append('({} <= "{}" AND "{}" < {})'.format(low, primary_key, primary_key, high))
                self.logger.debug("Predicates to split '%s':\n    %s", source_table_name.identifier,
                                  "\n    ".join("{:3d}: {}".format(i + 1, p) for i, p in enumerate(predicates)))

        return predicates

    def _fetch_table_size(self, conn: connection, table_name: TableName) -> int:
        """
        Fetch table size in bytes.
        """
        with Timer() as timer:
            rows = etl.pg.query(conn,
                                """SELECT pg_catalog.pg_table_size('{}') AS table_size
                                        , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size('{}')) AS pretty_table_size
                                """.format(table_name, table_name))
        if len(rows) != 1:
            raise UnknownTableSizeError("Failed to determine size of table")
        table_size, pretty_table_size = rows[0]["table_size"], rows[0]["pretty_table_size"]
        self.logger.info("Size of table '%s': %s (%d bytes) (%s)", table_name.identifier, pretty_table_size, table_size,
                         timer)
        return table_size

    def _suggest_best_partition_number(self, table_size: int) -> int:
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
        >>> suggest_best_partition_number(2000 * 1048576)
        16
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
        while partition_size >= target * 2:
            num_partitions *= 2
            partition_size //= 2

        return num_partitions

    def _fetch_partition_boundaries(self, conn: connection, table_name: TableName, primary_key: str,
                                    num_partitions: int) -> List[Tuple[int, int]]:
        """
        Fetch ranges for the primary key that partitions the table nicely.
        """
        with Timer() as timer:
            rows = etl.pg.query(conn, """SELECT MIN(pkey) AS lower_bound
                                              , MAX(pkey) AS upper_bound
                                              , COUNT(pkey) AS count
                                           FROM (
                                               SELECT "{}" AS pkey
                                                    , NTILE({}) OVER (ORDER BY "{}") AS part
                                                 FROM {}
                                                 ) t
                                          GROUP BY part
                                          ORDER BY part
                                      """.format(primary_key, num_partitions, primary_key, table_name))
        row_count = sum(row["count"] for row in rows)
        self.logger.info("Calculated %d partition boundaries for '%s' (%d rows) with primary key '%s' (%s)",
                         num_partitions, table_name.identifier, row_count, primary_key, timer)
        lower_bounds = (row["lower_bound"] for row in rows)
        upper_bounds = (row["upper_bound"] for row in rows)
        return [(low, high) for low, high in zip(lower_bounds, upper_bounds)]

    def _write_dataframe_as_csv(self, df, description: RelationDescription) -> None:
        """
        Write (partitioned) dataframe to CSV file(s)
        """
        s3_uri = "s3a://{0.bucket_name}/{0.prefix}/{0.csv_path_name}".format(description)
        if self.dry_run:
            self.logger.info("Dry-run: Skipping upload to '%s'", s3_uri)
        else:
            self.logger.info("Writing dataframe for '%s' to '%s'", description.source_path_name, s3_uri)
            # N.B. This must match the Sqoop (import) and Redshift (COPY) options
            # BROKEN Uses double quotes to escape double quotes ("Hello" becomes """Hello""")
            # BROKEN Does not escape newlines ('\n' does not become '\\n' so is read as 'n' in Redshift)
            # TODO Patch the com.databricks.spark.csv format to match Sqoop output
            write_options = {
                "header": "false",
                "nullValue": r"\N",
                "quoteMode": "ALL",  # Thanks to a bug in Apache commons, this is ignored.
                "codec": "gzip"
            }
            df.write \
                .format('com.databricks.spark.csv') \
                .options(**write_options) \
                .mode('overwrite') \
                .save(s3_uri)
