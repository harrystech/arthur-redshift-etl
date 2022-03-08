import logging
import os.path
import shlex
import subprocess
from tempfile import NamedTemporaryFile
from typing import Dict, List, Optional

import funcy as fy
import psycopg2

import etl.config
import etl.db
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.errors import DataExtractError, SqoopExecutionError
from etl.extract.database_extractor import DatabaseExtractor
from etl.relation import RelationDescription


class SqoopExtractor(DatabaseExtractor):
    """
    This extractor manages parallel SQL database extraction via MapReduce using Sqoop.

    See http://sqoop.apache.org/ and https://attic.apache.org/projects/sqoop.html
    """

    def __init__(
        self,
        schemas: Dict[str, DataWarehouseSchema],
        relations: List[RelationDescription],
        max_partitions: int,
        use_sampling: bool,
        keep_going: bool,
        dry_run: bool,
    ) -> None:
        super().__init__(
            "sqoop", schemas, relations, max_partitions, use_sampling, keep_going, dry_run=dry_run
        )

        self.logger = logging.getLogger(__name__)
        self.sqoop_executable = "sqoop"

        # During Sqoop extraction we write out files to a temp location
        self._sqoop_options_dir = etl.config.etl_tmp_dir("sqoop")
        if not os.path.isdir(self._sqoop_options_dir) and not self.dry_run:
            self.logger.info("Creating directory '%s' (with mode 750)", self._sqoop_options_dir)
            os.makedirs(self._sqoop_options_dir, mode=0o750, exist_ok=True)

    def _temporary_options_file(self, prefix: Optional[str] = None):
        # This function is needed to avoid a type error around 'dir' which isn't defined
        # as 'Optional' in the library.
        return NamedTemporaryFile("w", dir=self._sqoop_options_dir, prefix=prefix, delete=False)

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription) -> None:
        """Run Sqoop for one table; creates the sub-process and all the pretty args for Sqoop."""
        try:
            table_size = self.fetch_source_table_size(source.dsn, relation)
        except psycopg2.OperationalError as exc:
            raise DataExtractError(
                "failed to fetch table size for '%s'" % relation.identifier
            ) from exc

        connection_params_file_path = self.write_connection_params()
        password_file_path = self.write_password_file(source.dsn["password"])
        args = self.build_sqoop_options(
            source.dsn, relation, table_size, connection_params_file_path, password_file_path
        )
        options_file = self.write_options_file(args)
        # TODO(tom): Guard against failure in S3
        self._delete_directory_before_write(relation)

        self.run_sqoop(options_file)
        self.write_manifest_file(relation, relation.bucket_name, relation.data_directory())

    def write_password_file(self, password: str) -> str:
        """Write password to a (temporary) file, return name of file created."""
        if self.dry_run:
            self.logger.info("Dry-run: Skipping writing of password file")
            password_file_path = "/only/needed/for/type/checking"
        else:
            with self._temporary_options_file("pw_") as fp:
                fp.write(password)  # type: ignore
                fp.close()
            password_file_path = fp.name
            self.logger.info("Wrote password to '%s'", password_file_path)
        return password_file_path

    def write_connection_params(self) -> str:
        """Write a (temporary) file for connection parameters, return name of file created."""
        if self.dry_run:
            self.logger.info("Dry-run: Skipping writing of connection params file")
            params_file_path = "/only/needed/for/type/checking"
        else:
            with self._temporary_options_file("cp_") as fp:
                fp.write("ssl = true\n")  # type: ignore
                fp.write("sslfactory = org.postgresql.ssl.NonValidatingFactory\n")  # type: ignore
                fp.close()
            params_file_path = fp.name
            self.logger.info("Wrote connection params to '%s'", params_file_path)
        return params_file_path

    def build_sqoop_options(
        self,
        source_dsn: Dict[str, str],
        relation: RelationDescription,
        table_size: int,
        connection_param_file_path: str,
        password_file_path: str,
    ) -> List[str]:
        """
        Create set of Sqoop options.

        Starts with the command (import), then continues with generic options,
        tool specific options, and child-process options.
        """
        jdbc_url, dsn_properties = etl.db.extract_dsn(source_dsn)

        partition_key = relation.find_partition_key()
        select_statement = self.build_sqoop_select(relation, partition_key, table_size)
        partition_options = self.build_sqoop_partition_options(relation, partition_key, table_size)

        # Only the paranoid survive ... quote arguments of options, except for --select
        def q(s):
            # E731 do not assign a lambda expression, use a def -- whatever happened to Python?
            return '"{}"'.format(s)

        args = [
            "import",
            "--connect",
            q(jdbc_url),
            "--driver",
            q(dsn_properties["driver"]),
            "--connection-param-file",
            q(connection_param_file_path),
            "--username",
            q(dsn_properties["user"]),
            "--password-file",
            '"file://{}"'.format(password_file_path),
            "--verbose",
            "--fields-terminated-by",
            q(","),
            "--lines-terminated-by",
            r"'\n'",
            "--enclosed-by",
            "'\"'",
            "--escaped-by",
            r"'\\'",
            "--null-string",
            r"'\\N'",
            "--null-non-string",
            r"'\\N'",
            # NOTE Does not work with s3n:  "--delete-target-dir",
            "--target-dir",
            '"s3a://{}/{}"'.format(relation.bucket_name, relation.data_directory()),
            # NOTE Quoting the select statement breaks the select in an unSQLy way.
            "--query",
            select_statement,
            # NOTE Embedded newlines are not escaped so we need to remove them.  WAT?
            "--hive-drop-import-delims",
            "--compress",
        ]  # The default compression codec is gzip.

        args.extend(partition_options)
        self.logger.debug("Sqoop options are:\n%s", " ".join(args))
        return args

    def build_sqoop_select(
        self, relation: RelationDescription, partition_key: Optional[str], table_size: int
    ) -> str:
        """Build custom select statement needed to implement sampling and extracting views."""
        if self.use_sampling_with_table(table_size):
            select_statement = self.select_statement(relation, partition_key)
        else:
            select_statement = self.select_statement(relation, None)

        # Note that select statement always ends in a where clause, adding $CONDITIONS per
        # sqoop documentation
        return select_statement + " AND $CONDITIONS"

    def build_sqoop_partition_options(
        self, relation: RelationDescription, partition_key: Optional[str], table_size: int
    ) -> List[str]:
        """Build the partitioning-related arguments for Sqoop."""
        # Use single mapper if either there is no partition key, or if the partitioner returns
        # only one partition.
        num_mappers = 1
        partition_options = []
        if partition_key:
            column = fy.first(fy.where(relation.table_design["columns"], name=partition_key))

            if column is not None and column["type"] in ("date", "timestamp"):
                # Turn dates and timestamps into ints that we can partition on.
                quoted_key_arg = f"""CAST(DATE_PART('epoch', "{partition_key}") AS BIGINT)"""
            else:
                quoted_key_arg = f'"{partition_key}"'

            partition_options += ["--split-by", quoted_key_arg]

            if relation.partition_boundary_query:
                partition_options += ["--boundary-query", f'"{relation.partition_boundary_query}"']

            if relation.num_partitions:
                # num_partitions explicitly set in the design file overrides dynamic determination.
                num_mappers = min(relation.num_partitions, self.max_partitions)
            else:
                num_mappers = self.maximize_partitions(table_size)

        partition_options += ["--num-mappers", str(num_mappers)]
        return partition_options

    def write_options_file(self, args: List[str]) -> str:
        """Write options to a (temporary) file, return name of file created."""
        if self.dry_run:
            self.logger.info("Dry-run: Skipping creation of Sqoop options file")
            options_file_path = "/tmp/never_used"
        else:
            with self._temporary_options_file("so_") as fp:
                fp.write("\n".join(args))  # type: ignore
                fp.write("\n")  # type: ignore
                fp.close()
            options_file_path = fp.name
            self.logger.info("Wrote Sqoop options to '%s'", options_file_path)
        return options_file_path

    def _delete_directory_before_write(self, relation: RelationDescription) -> None:
        """Need to first delete data directory since Sqoop won't overwrite (and can't delete)."""
        csv_prefix = relation.data_directory()
        deletable = sorted(etl.s3.list_objects_for_prefix(relation.bucket_name, csv_prefix))
        if not deletable:
            return
        if self.dry_run:
            self.logger.info(
                "Dry-run: Skipping deletion of %d existing CSV file(s) in 's3://%s/%s'",
                len(deletable),
                relation.bucket_name,
                csv_prefix,
            )
        else:
            etl.s3.delete_objects(relation.bucket_name, deletable, wait=True, hdfs_wait=False)

    def run_sqoop(self, options_file_path: str):
        """Run Sqoop in a sub-process with the help of the given options file."""
        args = [self.sqoop_executable, "--options-file", options_file_path]
        cmdline = " ".join(map(shlex.quote, args))
        if self.dry_run:
            self.logger.info("Dry-run: Skipping Sqoop run '%s'", cmdline)
        else:
            self.logger.debug("Starting command: %s", cmdline)
            sqoop = subprocess.Popen(
                args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            self.logger.info("Sqoop is running with pid %d", sqoop.pid)
            out, err = sqoop.communicate()
            nice_out, nice_err = ("\n" + str(out)).rstrip(), (
                "\n" + str(err)
            ).rstrip()  # using str() for type check
            self.logger.debug("Sqoop finished with return code %d", sqoop.returncode)
            self.logger.debug("Sqoop stdout:%s", nice_out)
            self.logger.debug("Sqoop stderr:%s", nice_err)
            if sqoop.returncode != 0:
                # TODO(tom): Be more intelligent about detecting whether certain Sqoop errors are
                # retryable, instead of assuming they all are.
                raise SqoopExecutionError("Sqoop failed with return code %s" % sqoop.returncode)


class DummySqoopExtractor(SqoopExtractor):
    """
    This extractor runs '/usr/bin/false', which means that extraction always fails.

    Used for testing outside EMR and to test control flow (like retries).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqoop_executable = "/usr/bin/false"
