import logging
import os.path
from contextlib import closing
from typing import List, Dict

import etl.pg
from etl.config.dw import DataWarehouseSchema
from etl.extract.extractor import Extractor
from etl.extract.partition import DefaultPartitioningStrategy
from etl.names import TableName
from etl.relation import RelationDescription


class SparkExtractor(Extractor):

    """
    Use Apache Spark to download data from upstream databases.
    """

    def __init__(self, schemas: Dict[str, DataWarehouseSchema], relations: List[RelationDescription],
                 keep_going: bool, dry_run: bool) -> None:
        super().__init__("spark", schemas, relations, keep_going, needs_to_wait=True, dry_run=dry_run)
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
        conf = SparkConf()
        conf.setAppName(__name__)
        conf.set("spark.logConf", "true")
        # TODO Add spark.jars here? spark.submit.pyFiles?
        sc = SparkContext(conf=conf)
        return SQLContext(sc)

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription):
        """
        Using Spark's dataframe API, read the table in as a dataframe before writing it
        out to CSV.
        """
        with etl.pg.log_error():
            df = self.read_table_as_dataframe(source, relation)
            self.write_dataframe_as_csv(df, relation)
            prefix = os.path.join(relation.prefix, relation.csv_path_name)
            self.write_manifest_file(relation, relation.bucket_name, prefix)

    def read_table_as_dataframe(self, source: DataWarehouseSchema, relation: RelationDescription):
        """
        Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
        """
        jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

        source_table_name = relation.source_table_name
        selected_columns = relation.get_columns_with_casts()
        select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
        self.logger.debug("Table query: SELECT * FROM %s", select_statement)

        predicates = self.determine_partitioning(source_table_name, relation, source.dsn)
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

    def determine_partitioning(self, source_table_name: TableName, relation: RelationDescription,
                               read_access: Dict[str, str]) -> List[str]:
        """
        Guesstimate number of partitions based on actual table size and create list of predicates to split
        up table into that number of partitions.

        This requires for one numeric column to be marked as the primary key.  If there's no primary
        key in the table, the number of partitions is always one.
        (This requirement doesn't come from the table size but the need to split the table
        when reading it in.)
        """
        partition_key = relation.find_partition_key()  # type: Optional[str]
        if partition_key is None:
            self.logger.info("No partition key found for '%s', skipping partitioning", source_table_name.identifier)
            return []
        self.logger.debug("Determining partitioning for table '%s'", source_table_name.identifier)

        predicates = []
        with closing(etl.pg.connection(read_access, readonly=True)) as conn:
            table_size = etl.pg.fetch_table_size(conn, source_table_name.identifier)
            num_partitions = DefaultPartitioningStrategy(table_size, 1024).num_partitions()
            self.logger.info("Decided on using %d partition(s) for table '%s' with partition key: '%s'",
                             num_partitions, source_table_name.identifier, partition_key)

            if num_partitions > 1:
                boundaries = etl.pg.fetch_partition_boundaries(conn, source_table_name.identifier, partition_key,
                                                               num_partitions)
                for low, high in boundaries:
                    predicates.append('({} <= "{}" AND "{}" < {})'.format(low, partition_key, partition_key, high))
                self.logger.debug("Predicates to split '%s':\n    %s", source_table_name.identifier,
                                  "\n    ".join("{:3d}: {}".format(i + 1, p) for i, p in enumerate(predicates)))

        return predicates

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
