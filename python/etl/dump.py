"""
Functions to deal with dumping data from PostgreSQL databases to CSV.
"""

from collections import OrderedDict
import concurrent.futures
from contextlib import closing
from itertools import groupby
import logging
from operator import attrgetter
import os
import os.path
import shlex
import subprocess
from tempfile import NamedTemporaryFile

# Note that we'll import pyspark modules only when starting a SQL context.
import simplejson as json

import etl
import etl.config
import etl.design
import etl.monitor
import etl.pg
import etl.file_sets
import etl.relation
import etl.s3
from etl.timer import Timer
from etl.thyme import Thyme


# N.B. This must match value in deploy scripts in ./bin (and should be in a config file)
REDSHIFT_ETL_HOME = "/tmp/redshift_etl"


class UnknownTableSizeError(etl.ETLError):
    pass


class MissingCsvFilesError(etl.ETLError):
    pass


class DataDumpError(etl.ETLError):
    pass


class SqoopExecutionError(DataDumpError):
    pass


def create_sql_context():
    """
    Create a new SQL context within a new Spark context.

    This method will import from pyspark -- this is delayed so
    that we can `import etl.dump` without havig a spark context running.
    (E.g. it's silly to have to spark-submit the command to figure out the version.)

    Side-effect: Logging is configured by the time that pyspark is loaded
    so we have some better control over filters and formatting.
    """
    logger = logging.getLogger(__name__)
    if "SPARK_ENV_LOADED" not in os.environ:
        logger.warning("SPARK_ENV_LOADED is not set")

    from pyspark import SparkConf, SparkContext, SQLContext

    logger.info("Starting SparkSQL context")
    conf = SparkConf()
    conf.setAppName(__name__)
    conf.set("spark.logConf", "true")
    # TODO Add spark.jars here? spark.submit.pyFiles?
    sc = SparkContext(conf=conf)
    return SQLContext(sc)


def suggest_best_partition_number(table_size):
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


def fetch_table_size(conn, table_name):
    """
    Fetch table size in bytes.
    """
    logger = logging.getLogger(__name__)
    with Timer() as timer:
        rows = etl.pg.query(conn,
                            """SELECT pg_catalog.pg_table_size('{}') AS table_size
                                    , pg_catalog.pg_size_pretty(pg_catalog.pg_table_size('{}')) AS pretty_table_size
                            """.format(table_name, table_name))
    if len(rows) != 1:
        raise UnknownTableSizeError("Failed to determine size of table")
    table_size, pretty_table_size = rows[0]["table_size"], rows[0]["pretty_table_size"]
    logger.info("Size of table '%s': %s (%d bytes) (%s)", table_name.identifier, pretty_table_size, table_size, timer)
    return table_size


def fetch_partition_boundaries(conn, table_name, primary_key, num_partitions):
    """
    Fetch ranges for the primary key that partitions the table nicely.
    """
    logger = logging.getLogger(__name__)
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
    logger.info("Calculated %d partition boundaries for '%s' (%d rows) with primary key '%s' (%s)",
                num_partitions, table_name.identifier, row_count, primary_key, timer)
    lower_bounds = (row["lower_bound"] for row in rows)
    upper_bounds = (row["upper_bound"] for row in rows)
    return [(low, high) for low, high in zip(lower_bounds, upper_bounds)]


def determine_partitioning(source_table_name, table_design, read_access):
    """
    Guesstimate number of partitions based on actual table size and create list of predicates to split
    up table into that number of partitions.

    This requires for one column to be marked as the primary key.  If there's no primary
    key in the table, the number of partitions is always one.
    (This requirement doesn't come from the table size but the need to split the table
    when reading it in.)
    """
    logger = logging.getLogger(__name__)

    primary_key = find_primary_key(table_design)
    if "primary_key" is None:
        logger.info("No primary key defined for '%s', skipping partitioning", source_table_name.identifier)
        return []
    logger.debug("Primary key for table '%s' is '%s'", source_table_name.identifier, primary_key)

    predicates = []
    with closing(etl.pg.connection(read_access, readonly=True)) as conn:
        logger.debug("Determining predicates for table '%s'", source_table_name.identifier)

        table_size = fetch_table_size(conn, source_table_name)
        num_partitions = suggest_best_partition_number(table_size)
        logger.info("Picked %d partition(s) for table '%s' (primary key: '%s')",
                    num_partitions, source_table_name.identifier, primary_key)

        if num_partitions > 1:
            boundaries = fetch_partition_boundaries(conn, source_table_name, primary_key, num_partitions)
            for low, high in boundaries:
                predicates.append('({} <= "{}" AND "{}" < {})'.format(low, primary_key, primary_key, high))
            logger.debug("Predicates to split '%s':\n    %s", source_table_name.identifier,
                         "\n    ".join("{:3d}: {}".format(i + 1, p) for i, p in enumerate(predicates)))

    return predicates


def assemble_selected_columns(table_design):
    """
    Pick columns and decide how they are selected (as-is or with an expression).

    Whether there's an expression or just a name the resulting column is always
    called out delimited.
    """
    selected_columns = []
    for column in table_design["columns"]:
        if not column.get("skipped", False):
            if column.get("expression"):
                selected_columns.append('{expression} AS "{name}"'.format(**column))
            else:
                selected_columns.append('"{name}"'.format(**column))
    return selected_columns


def find_primary_key(table_design):
    """
    Return primary key (single column) from the table design, if defined, else None.
    """
    if "primary_key" in table_design.get("constraints", {}):
        # Note that column constraints such as primary key are stored as one-element lists, hence:
        return table_design["constraints"]["primary_key"][0]
    else:
        return None


def read_table_as_dataframe(sql_context, source, source_table_name, table_design):
    """
    Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
    """
    logger = logging.getLogger(__name__)
    jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

    selected_columns = assemble_selected_columns(table_design)
    select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
    logger.debug("Table query: SELECT * FROM %s", select_statement)

    predicates = determine_partitioning(source_table_name, table_design, source.dsn)
    if predicates:
        df = sql_context.read.jdbc(url=jdbc_url,
                                   properties=dsn_properties,
                                   table=select_statement,
                                   predicates=predicates)
    else:
        df = sql_context.read.jdbc(url=jdbc_url,
                                   properties=dsn_properties,
                                   table=select_statement)
    return df


def write_dataframe_as_csv(df, source_path_name, bucket_name, prefix, dry_run=False):
    """
    Write (partitioned) dataframe to CSV file(s)
    """
    logger = logging.getLogger(__name__)
    csv_path = os.path.join(prefix, "data", source_path_name, "csv")
    full_s3_path = "s3a://{}/{}".format(bucket_name, csv_path)
    if dry_run:
        logger.info("Dry-run: Skipping upload to '%s'", full_s3_path)
    else:
        logger.info("Writing dataframe for '%s' to '%s'", source_path_name, full_s3_path)
        # N.B. This must match the Sqoop (import) and Redshift (COPY) options
        # BROKEN Uses double quotes to escape double quotes ("Hello" becomes """Hello""")
        # BROKEN Does not escape newlines ('\n' does not become '\\n' so is read as 'n' in Redshift)
        # TODO Patch the com.databricks.spark.csv format to match Sqoop output?
        write_options = {
            "header": "false",
            "nullValue": r"\N",
            "quoteMode": "NON_NUMERIC",
            "escape": "\\",
            "codec": "gzip"
        }
        df.write \
            .format(source='com.databricks.spark.csv') \
            .options(**write_options) \
            .mode('overwrite') \
            .save(full_s3_path)


def create_dir_unless_exists(name, dry_run=False):
    logger = logging.getLogger(__name__)
    if not os.path.isdir(name) and not dry_run:
        logger.info("Creating directory '%s' (with mode 750)", name)
        os.makedirs(name, mode=0o750, exist_ok=True)


def write_manifest_file(bucket_name, prefix, source_path_name, manifest_filename, static_source=None, dry_run=False):
    """
    Create manifest file to load all the CSV files from the given folder.
    The manifest file will be created in the folder ABOVE the CSV files.

    If the data files are in 'foo/bar/csv/part-r*', then the manifest is '/foo/bar.manifest'.
    (The parameter 'csv_path' itself must be 'foo/bar/csv'.)

    This will also test for the presence of the _SUCCESS file (added by map reduce jobs).
    """
    logger = logging.getLogger(__name__)

    if dry_run:
        logger.info("Dry-run: Skipping writing manifest file 's3://%s/%s'", bucket_name, manifest_filename)
        return

    if static_source:
        rendered_template = Thyme.render_template(prefix, static_source.s3_path_template)
        csv_path = os.path.join(rendered_template, "data", source_path_name, "csv")
        source_data_bucket = static_source.s3_bucket
    else:
        csv_path = os.path.join(prefix, "data", source_path_name, "csv")
        # We are dumping the data right now, so we will find the data in the bucket we're using
        source_data_bucket = bucket_name
    logger.info("Writing manifest file 's3://%s/%s' for data in 's3://%s/%s'",
                bucket_name, manifest_filename, source_data_bucket, csv_path)

    # For non-static sources, wait for data & success file to potentially finish being written
    last_success = etl.s3.get_s3_object_last_modified(source_data_bucket, csv_path + "/_SUCCESS",
                                                      wait=(static_source is None))
    if last_success is None:
        raise MissingCsvFilesError("No valid CSV files (_SUCCESS is missing)")

    csv_files = sorted([
        f for f in etl.s3.list_objects_for_prefix(source_data_bucket, csv_path)
        if "part" in f and f.endswith(".gz")
    ])

    if len(csv_files) == 0:
        raise MissingCsvFilesError("Found no CSV files")

    remote_files = ["s3://{}/{}".format(source_data_bucket, filename) for filename in csv_files]

    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}

    with NamedTemporaryFile(mode="w+", dir=REDSHIFT_ETL_HOME, prefix="mf_") as local_file:
        logger.debug("Writing manifest file locally to '%s'", local_file.name)
        json.dump(manifest, local_file, indent="    ", sort_keys=True)
        local_file.write('\n')
        local_file.flush()
        logger.debug("Done writing '%s'", local_file.name)
        etl.s3.upload_to_s3(local_file.name, bucket_name, manifest_filename)


def dump_source_to_s3_with_spark(sql_context, source, bucket_name, prefix, file_sets, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    logger = logging.getLogger(__name__)
    copied = set()
    with Timer() as timer:
        for file_set in file_sets:
            source_table_name = file_set.source_table_name
            target_table_name = file_set.target_table_name
            manifest_filename = os.path.join(prefix, "data", file_set.source_path_name + ".manifest")
            with etl.monitor.Monitor(target_table_name.identifier, 'dump', dry_run=dry_run,
                                     options=["with-spark"],
                                     source={'name': file_set.source_name,
                                             'schema': source_table_name.schema,
                                             'table': source_table_name.table},
                                     destination={'bucket_name': bucket_name,
                                                  'object_key': manifest_filename}):
                # TODO move this logic to use relation description
                table_design = etl.design.download_table_design(bucket_name, file_set.design_file, target_table_name)
                df = read_table_as_dataframe(sql_context, source, source_table_name, table_design)
                write_dataframe_as_csv(df, file_set.source_path_name, bucket_name, prefix, dry_run=dry_run)
                write_manifest_file(bucket_name, prefix, file_set.source_path_name, manifest_filename,
                                    dry_run=dry_run)
            copied.add(target_table_name)
    logger.info("Finished with %d table(s) from source '%s' (%s)", len(copied), source.name, timer)


def dump_to_s3_with_spark(sources, bucket_name, prefix, file_sets, dry_run=False):
    """
    Dump data from multiple upstream sources to S3 with Spark Dataframes
    """
    logger = logging.getLogger(__name__)
    # FIXME this is shared between spark and sqoop ... move to callback of dump command
    source_lookup = {source.name: source for source in sources if source.is_database_source}
    if not source_lookup:
        logger.warning("Nothing to do here since list of upstream sources is empty")
        return
    logger.info("Dumping from these sources: %s", etl.join_with_quotes(source_lookup))

    sql_context = create_sql_context()
    for source_name, file_group in groupby(file_sets, attrgetter("source_name")):
        dump_source_to_s3_with_spark(sql_context, source_lookup[source_name], bucket_name, prefix, file_group,
                                     dry_run=dry_run)


def build_sqoop_options(bucket_name, csv_path, jdbc_url, username, password_file, source_table_files,
                        max_mappers=4):
    """
    Create set of Sqoop options.

    Starts with the command (import), then continues with generic options,
    tool specific options, and child-process options.
    """
    source_table_name = source_table_files.source_table_name
    table_design = etl.design.download_table_design(bucket_name, source_table_files.design_file,
                                                    source_table_files.target_table_name)
    columns = assemble_selected_columns(table_design)
    select_statement = """SELECT {} FROM {} WHERE $CONDITIONS""".format(", ".join(columns), source_table_name)
    primary_key = find_primary_key(table_design)

    # Only the paranoid survive ... quote arguments of options, except for --select
    def q(s):
        # E731 do not assign a lambda expression, use a def -- whatever happened to Python?
        return '"{}"'.format(s)

    args = ["import",
            "--connect", q(jdbc_url),
            "--driver", q("org.postgresql.Driver"),
            "--connection-param-file", q(os.path.join(REDSHIFT_ETL_HOME, "sqoop", "ssl.props")),
            "--username", q(username),
            "--password-file", '"file://{}"'.format(password_file),
            "--verbose",
            "--fields-terminated-by", q(","),
            "--lines-terminated-by", r"'\n'",
            "--enclosed-by", "'\"'",
            "--escaped-by", r"'\\'",
            "--null-string", r"'\\N'",
            "--null-non-string", r"'\\N'",
            # NOTE Does not work with s3n:  "--delete-target-dir",
            "--target-dir", '"s3n://{}/{}"'.format(bucket_name, csv_path),
            # NOTE Quoting the select statement (e.g. with shlex.quote) breaks the select in an unSQLy way.
            "--query", select_statement,
            # NOTE Embedded newlines are not escaped so we need to remove them.  WAT?
            "--hive-drop-import-delims",
            "--compress"]  # The default compression codec is gzip.
    if primary_key:
        args.extend(["--split-by", q(primary_key), "--num-mappers", str(max_mappers)])
    else:
        # TODO use "--autoreset-to-one-mapper" ?
        args.extend(["--num-mappers", "1"])
    return args


def write_password_file(password, dry_run=False):
    """
    Write password to a (temporary) file, return name of file created.
    """
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping writing of password file")
        password_file = None
    else:
        with NamedTemporaryFile('w+', dir=os.path.join(REDSHIFT_ETL_HOME, "sqoop"), prefix="pw_", delete=False) as fp:
            fp.write(password)
            fp.close()
        password_file = fp.name
        logger.info("Wrote password to '%s'", password_file)
    return password_file


def write_options_file(args, dry_run=False):
    """
    Write options to a (temporary) file, return name of file created.
    """
    logger = logging.getLogger(__name__)
    if dry_run:
        logger.info("Dry-run: Skipping creation of Sqoop options file")
        options_file = None
    else:
        with NamedTemporaryFile('w+', dir=os.path.join(REDSHIFT_ETL_HOME, "sqoop"), prefix="so_", delete=False) as fp:
            fp.write('\n'.join(args))
            fp.write('\n')
            fp.close()
        options_file = fp.name
        logger.info("Wrote Sqoop options to '%s'", options_file)
    return options_file


def run_sqoop(options_file, dry_run=False):
    """
    Run Sqoop in a sub-process with the help of the given options file.
    """
    logger = logging.getLogger(__name__)
    args = ["sqoop", "--options-file", options_file]
    if dry_run:
        logger.info("Dry-run: Skipping Sqoop run")
    else:
        logger.debug("Starting: %s", " ".join(map(shlex.quote, args)))
        sqoop = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 universal_newlines=True)
        logger.debug("Sqoop is running with pid %d", sqoop.pid)
        out, err = sqoop.communicate()
        # Thanks to universal_newlines, out and err are str not bytes (even if PyCharm thinks differently)
        nice_out, nice_err = ('\n' + out).rstrip(), ('\n' + err).rstrip()
        logger.debug("Sqoop finished with return code %d", sqoop.returncode)
        logger.debug("Sqoop stdout:%s", nice_out)
        logger.debug("Sqoop stderr:%s", nice_err)
        if sqoop.returncode != 0:
            raise SqoopExecutionError("Sqoop failed with return code %s" % sqoop.returncode)


def dump_table_with_sqoop(jdbc_url, dsn_properties, source_name, source_file_set, bucket_name, prefix,
                          max_partitions, dry_run=False):
    """
    Run Sqoop for one table, creates the sub-process and all the pretty args for Sqoop.
    """
    logger = logging.getLogger(__name__)

    source_table_name = source_file_set.source_table_name
    csv_path = os.path.join(prefix, "data", source_name,
                            "{}-{}".format(source_table_name.schema, source_table_name.table), "csv")
    manifest_filename = os.path.join(prefix, "data", source_file_set.source_path_name + ".manifest")

    sqoop_files = os.path.join(REDSHIFT_ETL_HOME, 'sqoop')
    create_dir_unless_exists(sqoop_files)

    password_file = write_password_file(dsn_properties["password"], dry_run=dry_run)
    args = build_sqoop_options(bucket_name, csv_path, jdbc_url, dsn_properties["user"], password_file,
                               source_file_set, max_mappers=max_partitions)
    logger.info("Sqoop options are:\n%s", " ".join(args))
    options_file = write_options_file(args, dry_run=dry_run)

    # Need to first delete directory since sqoop won't overwrite (and can't delete)
    deletable = sorted(etl.s3.list_objects_for_prefix(bucket_name, csv_path))
    if dry_run:
        if deletable:
            logger.info("Dry-run: Skipping deletion of existing CSV files 's3://%s/%s'", bucket_name, csv_path)
    else:
        etl.s3.delete_objects(bucket_name, deletable)

    run_sqoop(options_file, dry_run=dry_run)
    write_manifest_file(bucket_name, prefix, source_file_set.source_path_name, manifest_filename, dry_run=dry_run)


def dump_source_to_s3_with_sqoop(source, file_sets, bucket_name, prefix, max_partitions,
                                 keep_going=False, dry_run=False):
    """
    Dump all (selected) tables from a single upstream source.  Return list of tables for which dump failed.
    """
    logger = logging.getLogger(__name__)
    jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

    dumped = 0
    failed = []

    with Timer() as timer:
        for file_set in file_sets:
            target_table_name = file_set.target_table_name
            source_table_name = file_set.source_table_name
            # FIXME Refactor into relation description ... calculated multiple times
            manifest_filename = os.path.join(prefix, "data", file_set.source_path_name + ".manifest")
            try:
                # FIXME The monitor should contain the number of rows that were dumped.
                with etl.monitor.Monitor(target_table_name.identifier, 'dump', dry_run=dry_run,
                                         options=["with-sqoop"],
                                         source={'name': source.name,
                                                 'schema': source_table_name.schema,
                                                 'table': source_table_name.table},
                                         destination={'bucket_name': bucket_name,
                                                      'object_key': manifest_filename}):
                    dump_table_with_sqoop(jdbc_url, dsn_properties, source.name, file_set,
                                          bucket_name, prefix, max_partitions, dry_run=dry_run)
            except DataDumpError:
                if keep_going:
                    logger.exception("Ignoring this exception and proceeding as requested:")
                    failed.append(target_table_name)
                else:
                    raise
            else:
                dumped += 1
    if failed:
        logger.warning("Finished with %d table(s) from source '%s', %d failed (%s)",
                       dumped, source.name, len(failed), timer)
    else:
        logger.info("Finished with %d table(s) from source '%s' (%s)", dumped, source.name, timer)
    return failed


def dump_to_s3_with_sqoop(sources, bucket_name, prefix, file_sets, max_partitions, keep_going=False, dry_run=False):
    """
    Dump data from upstream sources to S3 with calls to Sqoop

    It is ok to call this method with "sources" that are actually just derived schemas since
    those will be ignored.
    """
    logger = logging.getLogger(__name__)
    source_lookup = {source.name: source for source in sources if source.is_database_source}
    if not source_lookup:
        logger.warning("Nothing to do here since list of upstream sources is empty")
        return

    # TODO This will run all sources in parallel ... should this be a command line arg?
    max_workers = len(source_lookup)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for source_name, file_group in groupby(file_sets, attrgetter("source_name")):
            if source_name in source_lookup:
                logger.info("Dumping from source '%s'", source_name)
                f = executor.submit(dump_source_to_s3_with_sqoop,
                                    source_lookup[source_name], list(file_group),
                                    bucket_name, prefix, max_partitions, keep_going=keep_going, dry_run=dry_run)
                futures.append(f)
            else:
                logger.info("Skipping schema '%s' which is not an upstream source", source_name)
        if keep_going:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        else:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)

    # Note that iterating over result of futures may raise an exception (which surfaces exceptions from threads)
    missing_tables = []
    for future in done:
        missing_tables.extend(future.result())
    for table_name in missing_tables:
        logger.warning("Failed to dump: '%s'", table_name.identifier)

    if not_done:
        raise DataDumpError("Dump failed to complete for {:d} source(s)".format(len(not_done)))


def dump_static_table(source, source_file_set, bucket_name, prefix, dry_run=False):
    """
    Prepare to write manifest file for data files associated with the static data source
    """
    manifest_filename = os.path.join(prefix, "data", source_file_set.source_path_name + ".manifest")
    write_manifest_file(bucket_name, prefix, source_file_set.source_path_name, manifest_filename,
                        dry_run=dry_run, static_source=source)


def dump_static_source_to_s3(source, file_sets, bucket_name, prefix, keep_going=False, dry_run=False):
    """
    Dump all (selected) tables from a single upstream source.  Return list of tables for which dump failed.
    """
    logger = logging.getLogger(__name__)

    dumped = 0
    failed = []

    with Timer() as timer:
        for file_set in file_sets:
            target_table_name = file_set.target_table_name
            source_table_name = file_set.source_table_name
            # FIXME Refactor into relation description ... calculated multiple times
            manifest_filename = os.path.join(prefix, "data", file_set.source_path_name + ".manifest")
            try:
                # FIXME The monitor should contain the number of rows that were dumped.
                with etl.monitor.Monitor(target_table_name.identifier, 'dump', dry_run=dry_run,
                                         options=["static"],
                                         source={'name': source.name,
                                                 'schema': source_table_name.schema,
                                                 'table': source_table_name.table},
                                         destination={'bucket_name': bucket_name,
                                                      'object_key': manifest_filename}):
                    dump_static_table(source, file_set, bucket_name, prefix, dry_run=dry_run)
            except DataDumpError:
                if keep_going:
                    logger.exception("Ignoring this exception and proceeding as requested:")
                    failed.append(target_table_name)
                else:
                    raise
            else:
                dumped += 1
        if failed:
            logger.warning("Finished with %d table(s) from source '%s', %d failed (%s)",
                           dumped, source.name, len(failed), timer)
        else:
            logger.info("Finished with %d table(s) from source '%s' (%s)", dumped, source.name, timer)
        return failed


def dump_static_sources(sources, bucket_name, prefix, file_sets, keep_going=False, dry_run=False):
    logger = logging.getLogger(__name__)

    source_lookup = {source.name: source for source in sources if source.is_static_source}
    if not source_lookup:
        logger.warning("Nothing to do here since list of upstream sources is empty")
        return

    max_workers = len(source_lookup)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for source_name, file_group in groupby(file_sets, attrgetter("source_name")):

            if source_name in source_lookup:
                logger.info("Dumping from source '%s'", source_name)
                f = executor.submit(dump_static_source_to_s3, source_lookup[source_name], list(file_group),
                                    bucket_name, prefix, keep_going=keep_going, dry_run=dry_run)
                futures.append(f)
            else:
                logger.info("Skipping schema '%s' which is not an upstream source", source_name)
        if keep_going:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        else:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)

    # Note that iterating over result of futures may raise an exception (which surfaces exceptions from threads)
    missing_tables = []
    for future in done:
        missing_tables.extend(future.result())
    for table_name in missing_tables:
        logger.warning("Failed to dump: '%s'", table_name.identifier)

    if not_done:
        raise DataDumpError("Dump failed to complete for {:d} source(s)".format(len(not_done)))


def dump_to_s3(dumper, schemas, bucket_name, prefix, file_sets, max_partitions, keep_going, dry_run):
    """
    Dump data from an upstream source to S3

    This is the entry point, and which technology will be used will be determined
    by the args here. Additionally, we don't care yet if any of the sources are static.
    """
    # Make sure our temp directory exists (where manifest files are temporarily stashed)
    # TODO If this is a temp dir, should we use TempDir?
    create_dir_unless_exists(REDSHIFT_ETL_HOME)

    dump_static_sources(schemas, bucket_name, prefix, file_sets, keep_going=keep_going, dry_run=dry_run)

    if dumper == "sqoop":
        with etl.pg.log_error():
            dump_to_s3_with_sqoop(schemas, bucket_name, prefix, file_sets,
                                  max_partitions, keep_going=keep_going, dry_run=dry_run)
    else:
        with etl.pg.log_error():
            dump_to_s3_with_spark(schemas, bucket_name, prefix, file_sets, dry_run=dry_run)
