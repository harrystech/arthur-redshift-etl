import logging
import os.path
from contextlib import closing
from typing import Dict, List, Tuple

import boto3
from psycopg2.extensions import connection  # only for type annotation

import etl.db
from etl.config.dw import DataWarehouseSchema
from etl.extract.database_extractor import DatabaseExtractor
from etl.names import TableName
from etl.relation import RelationDescription
from etl.timer import Timer


class SparkExtractor(DatabaseExtractor):
    """
    Use Apache Spark to download data from upstream databases.
    """

    def __init__(self, schemas: Dict[str, DataWarehouseSchema], relations: List[RelationDescription],
                 max_partitions: int, use_sampling: bool, keep_going: bool, dry_run: bool) -> None:
        super().__init__("spark", schemas, relations, max_partitions, use_sampling, keep_going, dry_run=dry_run)
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
        pyspark has to be pushed down into this method as Spark needs to be available
        in order for the libraries to be imported successfully. Since Spark is not available
        when the ETL is started initally, we delay the import until the ETL has restarted
        under Spark.

        Side-effect: Logging is configured by the time that pyspark is loaded
        so we have some better control over filters and formatting.
        """
        from pyspark import SparkConf, SparkContext, SQLContext

        if "SPARK_ENV_LOADED" not in os.environ:
            self.logger.warning("SPARK_ENV_LOADED is not set")

        self.logger.info("Starting SparkSQL context")
        conf = (SparkConf()
                .setAppName(__name__)
                .set("spark.logConf", "true"))
        sc = SparkContext(conf=conf)

        # Copy the credentials from the session into hadoop for access to S3
        session = boto3.Session()
        credentials = session.get_credentials()
        hadoopConf = sc._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", credentials.access_key)
        hadoopConf.set("fs.s3a.secret.key", credentials.secret_key)

        return SQLContext(sc)

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription):
        """
        Using Spark's dataframe API, read the table in as a dataframe before writing it out to CSV.
        """
        with etl.db.log_error():
            df = self.read_table_as_dataframe(source, relation)
            self.write_dataframe_as_csv(df, relation)
            prefix = os.path.join(relation.prefix, relation.csv_path_name)
            self.write_manifest_file(relation, relation.bucket_name, prefix)

    def read_table_as_dataframe(self, source: DataWarehouseSchema, relation: RelationDescription):
        """
        Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
        """
        partition_key = relation.find_partition_key()

        table_size = self.fetch_source_table_size(source.dsn, relation)
        num_partitions = self.maximize_partitions(table_size)

        if partition_key is None or num_partitions <= 1:
            predicates = None
        else:
            with closing(etl.db.connection(source.dsn, readonly=True)) as conn:
                predicates = self.determine_partitioning(conn, relation, partition_key, num_partitions)

        if self.use_sampling_with_table(table_size):
            inner_select = self.select_statement(relation, partition_key)
        else:
            inner_select = self.select_statement(relation, None)
        select_statement = """({}) AS t""".format(inner_select)
        self.logger.debug("Table query: SELECT * FROM %s", select_statement)

        jdbc_url, dsn_properties = etl.db.extract_dsn(source.dsn, read_only=True)
        df = self.sql_context.read.jdbc(url=jdbc_url,
                                        properties=dsn_properties,
                                        table=select_statement,
                                        predicates=predicates)
        return df

    def determine_partitioning(self, conn: connection, relation: RelationDescription,
                               partition_key: str, num_partitions: int) -> List[str]:
        """
        Create list of predicates to split up table into that number of partitions.
        This requires for one numeric column to be marked as the primary key.
        """
        self.logger.info("Decided on using %d partition(s) for table '%s.%s' with partition key: '%s'",
                         num_partitions, relation.source_name, relation.source_table_name.identifier, partition_key)
        boundaries = self.fetch_partition_boundaries(conn, relation.source_table_name, partition_key, num_partitions)
        predicates = []
        for low, high in boundaries:
            predicates.append('({} <= "{}" AND "{}" < {})'.format(low, partition_key, partition_key, high))
        self.logger.debug("Predicates to split '%s':\n    %s", relation.source_table_name.identifier,
                          "\n    ".join("{:3d}: {}".format(i + 1, p) for i, p in enumerate(predicates)))
        return predicates

    def fetch_partition_boundaries(self, conn: connection, table_name: TableName, partition_key: str,
                                   num_partitions: int) -> List[Tuple[int, int]]:
        """
        Fetch ranges for the partition key that partitions the table nicely.
        """
        stmt = """
            SELECT MIN(pkey) AS lower_bound
                 , MAX(pkey) AS upper_bound
                 , COUNT(pkey) AS count
              FROM (
                      SELECT "{partition_key}" AS pkey
                           , NTILE({num_partitions}) OVER (ORDER BY "{partition_key}") AS part
                        FROM {table_name}
                   ) t
             GROUP BY part
             ORDER BY part
        """
        with Timer() as timer:
            rows = etl.db.query(conn, stmt.format(partition_key=partition_key, num_partitions=num_partitions,
                                                  table_name=table_name))
        row_count = sum(row["count"] for row in rows)
        self.logger.info("Calculated %d partition boundaries for %d rows in '%s' using partition key '%s' (%s)",
                         num_partitions, row_count, table_name.identifier, partition_key, timer)
        lower_bounds = (row["lower_bound"] for row in rows)
        upper_bounds = (row["upper_bound"] for row in rows)
        return [(low, high) for low, high in zip(lower_bounds, upper_bounds)]

    def write_dataframe_as_csv(self, df, relation: RelationDescription) -> None:
        """
        Write (partitioned) dataframe to CSV file(s)
        """
        s3_uri = "s3a://{0.bucket_name}/{0.prefix}/{0.csv_path_name}".format(relation)
        if self.dry_run:
            self.logger.info("Dry-run: Skipping upload to '%s'", s3_uri)
        else:
            self.logger.info("Writing dataframe for '%s' to '%s'", relation.source_path_name, s3_uri)
            # N.B. This must match the Sqoop (import) and Redshift (COPY) options
            # BROKEN Uses double quotes to escape double quotes ("Hello" becomes """Hello""")
            # BROKEN Does not escape newlines ('\n' does not become '\\n' so is read as 'n' in Redshift)
            write_options = {
                "header": "false",
                "nullValue": r"\N",
                "quoteAll": "true",
                "codec": "gzip"
            }
            df.write \
                .mode('overwrite') \
                .options(**write_options) \
                .csv(s3_uri)
