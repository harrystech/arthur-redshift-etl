import logging
import os.path
import shlex
import subprocess
from tempfile import NamedTemporaryFile
from typing import Dict, List, Union

import etl.config
from etl.config import DataWarehouseSchema
from etl.extract.errors import SqoopExecutionError
from etl.extract.extractor import Extractor
import etl.monitor
import etl.pg
from etl.relation import RelationDescription
import etl.s3


class SqoopExtractor(Extractor):

    """
    This extractor takes care of all the idiosyncrasies of extracting data from
    upstream sources using Sqoop, http://sqoop.apache.org/
    """

    def __init__(self, schemas: Dict[str, DataWarehouseSchema], descriptions: List[RelationDescription],
                 keep_going: bool, max_partitions: int, dry_run: bool):
        super().__init__("sqoop", schemas, descriptions, keep_going, needs_to_wait=True, dry_run=dry_run)
        self.logger = logging.getLogger(__name__)
        self.max_partitions = max_partitions
        self.sqoop_executable = "sqoop"

        # During Sqoop extraction we write out files to a temp location
        self._sqoop_options_dir = etl.config.etl_tmp_dir("sqoop")
        if not os.path.isdir(self._sqoop_options_dir) and not self.dry_run:
            self.logger.info("Creating directory '%s' (with mode 750)", self._sqoop_options_dir)
            os.makedirs(self._sqoop_options_dir, mode=0o750, exist_ok=True)

    def extract_table(self, source: DataWarehouseSchema, description: RelationDescription) -> None:
        """
        Run Sqoop for one table; creates the sub-process and all the pretty args for Sqoop.
        """
        jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

        password_file_path = self._write_password_file(dsn_properties["password"])
        args = self._build_sqoop_options(jdbc_url, dsn_properties["user"], password_file_path, description)
        self.logger.info("Sqoop options are:\n%s", " ".join(args))
        options_file = self._write_options_file(args)

        self._delete_directory_before_write(description)
        self._run_sqoop(options_file)
        prefix = os.path.join(description.prefix, description.csv_path_name)
        self.write_manifest_file(description, description.bucket_name, prefix)

    def _write_password_file(self, password: str) -> Union[str, None]:
        """
        Write password to a (temporary) file, return name of file created.
        """
        if self.dry_run:
            self.logger.info("Dry-run: Skipping writing of password file")
            password_file_path = None
        else:
            with NamedTemporaryFile('w+', dir=self._sqoop_options_dir, prefix="pw_", delete=False) as fp:
                fp.write(password)
                fp.close()
            password_file_path = fp.name
            self.logger.info("Wrote password to '%s'", password_file_path)
        return password_file_path

    def _build_sqoop_options(self, jdbc_url: str, username: str, password_file_path: str,
                             description: RelationDescription) -> List[str]:
        """
        Create set of Sqoop options.

        Starts with the command (import), then continues with generic options,
        tool specific options, and child-process options.
        """
        source_table_name = description.source_table_name
        table_design = description.table_design
        columns = description.get_columns_with_casts()
        select_statement = """SELECT {} FROM {} WHERE $CONDITIONS""".format(", ".join(columns), source_table_name)
        primary_key = description.find_primary_key()

        # Only the paranoid survive ... quote arguments of options, except for --select
        def q(s):
            # E731 do not assign a lambda expression, use a def -- whatever happened to Python?
            return '"{}"'.format(s)

        args = ["import",
                "--connect", q(jdbc_url),
                "--driver", q("org.postgresql.Driver"),
                "--connection-param-file", q(os.path.join(self._sqoop_options_dir, "ssl.props")),
                "--username", q(username),
                "--password-file", '"file://{}"'.format(password_file_path),
                "--verbose",
                "--fields-terminated-by", q(","),
                "--lines-terminated-by", r"'\n'",
                "--enclosed-by", "'\"'",
                "--escaped-by", r"'\\'",
                "--null-string", r"'\\N'",
                "--null-non-string", r"'\\N'",
                # NOTE Does not work with s3n:  "--delete-target-dir",
                "--target-dir", '"s3n://{}/{}/{}"'.format(description.bucket_name,
                                                          description.prefix,
                                                          description.csv_path_name),
                # NOTE Quoting the select statement (e.g. with shlex.quote) breaks the select in an unSQLy way.
                "--query", select_statement,
                # NOTE Embedded newlines are not escaped so we need to remove them.  WAT?
                "--hive-drop-import-delims",
                "--compress"]  # The default compression codec is gzip.
        if primary_key:
            args.extend(["--split-by", q(primary_key), "--num-mappers", str(self.max_partitions)])
        else:
            # TODO use "--autoreset-to-one-mapper" ?
            args.extend(["--num-mappers", "1"])
        return args

    def _write_options_file(self, args: List[str]) -> Union[str, None]:
        """
        Write options to a (temporary) file, return name of file created.
        """
        if self.dry_run:
            self.logger.info("Dry-run: Skipping creation of Sqoop options file")
            options_file_path = None
        else:
            with NamedTemporaryFile('w+', dir=self._sqoop_options_dir, prefix="so_", delete=False) as fp:
                fp.write('\n'.join(args))
                fp.write('\n')
                fp.close()
            options_file_path = fp.name
            self.logger.info("Wrote Sqoop options to '%s'", options_file_path)
        return options_file_path

    def _delete_directory_before_write(self, description: RelationDescription) -> None:
        """
        Need to first delete data directory since Sqoop won't overwrite (and can't delete).
        """
        csv_prefix = os.path.join(description.prefix, description.csv_path_name)
        deletable = sorted(etl.s3.list_objects_for_prefix(description.bucket_name, csv_prefix))
        if deletable:
            if self.dry_run:
                self.logger.info("Dry-run: Skipping deletion of existing CSV files 's3://%s/%s'",
                                 description.bucket_name, csv_prefix)
            else:
                etl.s3.delete_objects(description.bucket_name, deletable)

    def _run_sqoop(self, options_file_path: str):
        """
        Run Sqoop in a sub-process with the help of the given options file.
        """
        args = [self.sqoop_executable, "--options-file", options_file_path]
        if self.dry_run:
            self.logger.info("Dry-run: Skipping Sqoop run")
        else:
            self.logger.debug("Starting: %s", " ".join(map(shlex.quote, args)))
            sqoop = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                     universal_newlines=True)
            self.logger.debug("Sqoop is running with pid %d", sqoop.pid)
            out, err = sqoop.communicate()
            # Thanks to universal_newlines, out and err are str not bytes (even if PyCharm thinks differently)
            nice_out, nice_err = ('\n' + out).rstrip(), ('\n' + err).rstrip()
            self.logger.debug("Sqoop finished with return code %d", sqoop.returncode)
            self.logger.debug("Sqoop stdout:%s", nice_out)
            self.logger.debug("Sqoop stderr:%s", nice_err)
            if sqoop.returncode != 0:
                raise SqoopExecutionError("Sqoop failed with return code %s" % sqoop.returncode)


class FakeSqoopExtractor(SqoopExtractor):
    """
    This extractor runs '/usr/bin/false' which means that extraction fails ... used for testing outside EMR.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sqoop_executable = "/usr/bin/false"
