import logging
import os.path
import shlex
import subprocess
from contextlib import closing
from tempfile import NamedTemporaryFile
from typing import Dict, List, Optional

import etl.config
import etl.monitor
import etl.db
import etl.s3
from etl.config.dw import DataWarehouseSchema
from etl.errors import SqoopExecutionError
from etl.extract.database_extractor import DatabaseExtractor
from etl.relation import RelationDescription


class SqoopExtractor(DatabaseExtractor):
    """
    This extractor manages parallel SQL database extraction via MapReduce
    using Sqoop (http://sqoop.apache.org/)
    """

    def __init__(self, schemas: Dict[str, DataWarehouseSchema], relations: List[RelationDescription],
                 max_partitions: int, use_sampling: bool, keep_going: bool, dry_run: bool) -> None:

        super().__init__("sqoop", schemas, relations, max_partitions, use_sampling, keep_going, dry_run=dry_run)

        self.logger = logging.getLogger(__name__)
        self.sqoop_executable = "sqoop"

        # During Sqoop extraction we write out files to a temp location
        self._sqoop_options_dir = etl.config.etl_tmp_dir("sqoop")
        if not os.path.isdir(self._sqoop_options_dir) and not self.dry_run:
            self.logger.info("Creating directory '%s' (with mode 750)", self._sqoop_options_dir)
            os.makedirs(self._sqoop_options_dir, mode=0o750, exist_ok=True)

    def extract_table(self, source: DataWarehouseSchema, relation: RelationDescription) -> None:
        """
        Run Sqoop for one table; creates the sub-process and all the pretty args for Sqoop.
        """
        with closing(etl.db.connection(source.dsn, readonly=True)) as conn:
            table_size = self.fetch_source_table_size(conn, relation)

        connection_params_file_path = self.write_connection_params()
        password_file_path = self.write_password_file(source.dsn["password"])
        args = self.build_sqoop_options(source.dsn, relation, table_size,
                                        connection_params_file_path, password_file_path)
        options_file = self.write_options_file(args)
        self._delete_directory_before_write(relation)

        self.run_sqoop(options_file)

        prefix = os.path.join(relation.prefix, relation.csv_path_name)
        self.write_manifest_file(relation, relation.bucket_name, prefix)

    def write_password_file(self, password: str) -> str:
        """
        Write password to a (temporary) file, return name of file created.
        """
        if self.dry_run:
            self.logger.info("Dry-run: Skipping writing of password file")
            password_file_path = "/only/needed/for/type/checking"
        else:
            with NamedTemporaryFile('w', dir=self._sqoop_options_dir, prefix="pw_", delete=False) as fp:
                fp.write(password)  # type: ignore
                fp.close()
            password_file_path = fp.name
            self.logger.info("Wrote password to '%s'", password_file_path)
        return password_file_path

    def write_connection_params(self) -> str:
        """
        Write a (temporary) file for connection parameters, return name of file created.
        """
        if self.dry_run:
            self.logger.info("Dry-run: Skipping writing of connection params file")
            params_file_path = "/only/needed/for/type/checking"
        else:
            with NamedTemporaryFile('w', dir=self._sqoop_options_dir, prefix="cp_", delete=False) as fp:
                fp.write("ssl = true\n")  # type: ignore
                fp.write("sslfactory = org.postgresql.ssl.NonValidatingFactory\n")  # type: ignore
                fp.close()
            params_file_path = fp.name
            self.logger.info("Wrote connection params to '%s'", params_file_path)
        return params_file_path

    def build_sqoop_options(self, source_dsn: Dict[str, str], relation: RelationDescription,
                            table_size: int, connection_param_file_path: str, password_file_path: str) -> List[str]:
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

        args = ["import",
                "--connect", q(jdbc_url),
                "--driver", q("org.postgresql.Driver"),  # TODO: use com.amazon.redshift.jdbc41.Driver
                "--connection-param-file", q(connection_param_file_path),
                "--username", q(dsn_properties["user"]),
                "--password-file", '"file://{}"'.format(password_file_path),
                "--verbose",
                "--fields-terminated-by", q(","),
                "--lines-terminated-by", r"'\n'",
                "--enclosed-by", "'\"'",
                "--escaped-by", r"'\\'",
                "--null-string", r"'\\N'",
                "--null-non-string", r"'\\N'",
                # NOTE Does not work with s3n:  "--delete-target-dir",
                "--target-dir", '"s3n://{}/{}/{}"'.format(relation.bucket_name,
                                                          relation.prefix,
                                                          relation.csv_path_name),
                # NOTE Quoting the select statement (e.g. with shlex.quote) breaks the select in an unSQLy way.
                "--query", select_statement,
                # NOTE Embedded newlines are not escaped so we need to remove them.  WAT?
                "--hive-drop-import-delims",
                "--compress"]  # The default compression codec is gzip.

        args.extend(partition_options)
        self.logger.debug("Sqoop options are:\n%s", " ".join(args))
        return args

    def build_sqoop_select(self, relation: RelationDescription, partition_key: Optional[str], table_size: int) -> str:
        """
        Build custom select statement needed to implement sampling and extracting views.
        """
        if self.use_sampling_with_table(table_size):
            select_statement = self.select_statement(relation, partition_key)
        else:
            select_statement = self.select_statement(relation, None)

        # Note that select statement always ends in a where clause, adding $CONDITIONS per sqoop documentation
        select_statement += " AND $CONDITIONS"
        return select_statement

    def build_sqoop_partition_options(self, relation: RelationDescription, partition_key: Optional[str],
                                      table_size: int) -> List[str]:
        """
        Build the partitioning-related arguments for Sqoop.
        """
        if partition_key:
            quoted_key_arg = '"{}"'.format(partition_key)

            if relation.num_partitions:
                # num_partitions explicitly set in the design file overrides the dynamic determination.
                num_mappers = min(relation.num_partitions, self.max_partitions)
            else:
                num_mappers = self.maximize_partitions(table_size)

            if num_mappers > 1:
                return ["--split-by", quoted_key_arg, "--num-mappers", str(num_mappers)]

        # Use 1 mapper if either there is no partition key, or if the partitioner returns only one partition
        return ["--num-mappers", "1"]

    def write_options_file(self, args: List[str]) -> str:
        """
        Write options to a (temporary) file, return name of file created.
        """
        if self.dry_run:
            self.logger.info("Dry-run: Skipping creation of Sqoop options file")
            options_file_path = "/tmp/never_used"
        else:
            with NamedTemporaryFile('w', dir=self._sqoop_options_dir, prefix="so_", delete=False) as fp:
                fp.write('\n'.join(args))  # type: ignore
                fp.write('\n')  # type: ignore
                fp.close()
            options_file_path = fp.name
            self.logger.info("Wrote Sqoop options to '%s'", options_file_path)
        return options_file_path

    def _delete_directory_before_write(self, relation: RelationDescription) -> None:
        """
        Need to first delete data directory since Sqoop won't overwrite (and can't delete).
        """
        csv_prefix = os.path.join(relation.prefix, relation.csv_path_name)
        deletable = sorted(etl.s3.list_objects_for_prefix(relation.bucket_name, csv_prefix))
        if deletable:
            if self.dry_run:
                self.logger.info("Dry-run: Skipping deletion of %d existing CSV file(s) in 's3://%s/%s'",
                                 len(deletable), relation.bucket_name, csv_prefix)
            else:
                etl.s3.delete_objects(relation.bucket_name, deletable, wait=True)

    def run_sqoop(self, options_file_path: str):
        """
        Run Sqoop in a sub-process with the help of the given options file.
        """
        args = [self.sqoop_executable, "--options-file", options_file_path]
        cmdline = " ".join(map(shlex.quote, args))
        if self.dry_run:
            self.logger.info("Dry-run: Skipping Sqoop run '%s'", cmdline)
        else:
            self.logger.debug("Starting command: %s", cmdline)
            sqoop = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     universal_newlines=True)
            self.logger.debug("Sqoop is running with pid %d", sqoop.pid)
            out, err = sqoop.communicate()
            nice_out, nice_err = ('\n' + str(out)).rstrip(), ('\n' + str(err)).rstrip()  # using str() for type check
            self.logger.debug("Sqoop finished with return code %d", sqoop.returncode)
            self.logger.debug("Sqoop stdout:%s", nice_out)
            self.logger.debug("Sqoop stderr:%s", nice_err)
            if sqoop.returncode != 0:
                # TODO: Be more intelligent about detecting whether certain Sqoop errors are retryable, instead of
                # assuming they all are.
                raise SqoopExecutionError("Sqoop failed with return code %s" % sqoop.returncode)


class FakeSqoopExtractor(SqoopExtractor):
    """
    This extractor runs '/usr/bin/false' which means that extraction fails ... used for testing outside EMR.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqoop_executable = "/usr/bin/false"
