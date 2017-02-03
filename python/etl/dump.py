"""
Functions to deal with dumping data from PostgreSQL databases to CSV.
"""

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


class DataDumpError(etl.ETLError):
    pass


class SqoopExecutionError(DataDumpError):
    pass


class MissingCsvFilesError(DataDumpError):
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


def read_table_as_dataframe(sql_context, source, description):
    """
    Read dataframe (with partitions) by contacting upstream JDBC-reachable source.
    """
    logger = logging.getLogger(__name__)
    jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

    source_table_name = description.source_table_name
    selected_columns = assemble_selected_columns(description.table_design)
    select_statement = """(SELECT {} FROM {}) AS t""".format(", ".join(selected_columns), source_table_name)
    logger.debug("Table query: SELECT * FROM %s", select_statement)

    predicates = determine_partitioning(source_table_name, description.table_design, source.dsn)
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


def write_dataframe_as_csv(df, description, dry_run=False):
    """
    Write (partitioned) dataframe to CSV file(s)
    """
    logger = logging.getLogger(__name__)
    s3_uri = "s3a://{0.bucket_name}/{0.prefix}/{0.csv_path_name}".format(description)
    if dry_run:
        logger.info("Dry-run: Skipping upload to '%s'", s3_uri)
    else:
        logger.info("Writing dataframe for '%s' to '%s'", description.source_path_name, s3_uri)
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


def create_dir_unless_exists(name, dry_run=False):
    logger = logging.getLogger(__name__)
    if not os.path.isdir(name) and not dry_run:
        logger.info("Creating directory '%s' (with mode 750)", name)
        os.makedirs(name, mode=0o750, exist_ok=True)


def write_manifest_file(description, static_source=None, dry_run=False):
    """
    Create manifest file to load all the CSV files for the given relation.
    The manifest file will be created in the folder ABOVE the CSV files.

    If the data files are in 'foo/bar/csv/part-r*', then the manifest is '/foo/bar.manifest'.

    Note that for static sources, we need to check the bucket of that source, not the
    bucket where the manifest will be written to.

    This will also test for the presence of the _SUCCESS file (added by map-reduce jobs).
    """
    logger = logging.getLogger(__name__)

    if static_source:
        # Need to lookup S3 information from the static source
        rendered_template = Thyme.render_template(static_source.s3_path_template, {"prefix": description.prefix})
        csv_prefix = os.path.join(rendered_template, description.csv_path_name)
        source_data_bucket = static_source.s3_bucket
    else:
        # For database sources, the S3 information w.r.t. bucket and prefix for CSV matches the table design
        csv_prefix = os.path.join(description.prefix, description.csv_path_name)
        source_data_bucket = description.bucket_name
    logger.info("Preparing manifest file for data in 's3://%s/%s'", source_data_bucket, csv_prefix)

    # For non-static sources, wait for data & success file to potentially finish being written
    # For static sources, we go straight to failure when the success file does not exist
    last_success = etl.s3.get_s3_object_last_modified(source_data_bucket, csv_prefix + "/_SUCCESS",
                                                      wait=(static_source is None and not dry_run))
    if last_success is None and not dry_run:
        raise MissingCsvFilesError("No valid CSV files (_SUCCESS is missing)")

    csv_files = sorted(key for key in etl.s3.list_objects_for_prefix(source_data_bucket, csv_prefix)
                       if "part" in key and key.endswith(".gz"))
    if len(csv_files) == 0 and not dry_run:
        raise MissingCsvFilesError("Found no CSV files")

    remote_files = ["s3://{}/{}".format(source_data_bucket, filename) for filename in csv_files]
    manifest = {"entries": [{"url": name, "mandatory": True} for name in remote_files]}

    if dry_run:
        logger.info("Dry-run: Skipping writing manifest file 's3://%s/%s'",
                    description.bucket_name, description.manifest_file_name)
    else:
        logger.info("Writing manifest file to 's3://%s/%s'", description.bucket_name, description.manifest_file_name)
        etl.s3.upload_data_to_s3(manifest, description.bucket_name, description.manifest_file_name)


def dump_source_to_s3_with_spark(sql_context, source, descriptions, dry_run=False):
    """
    Dump all the tables from one source into CSV files in S3.  The selector may be used to pick a subset of tables.
    """
    logger = logging.getLogger(__name__)
    copied = set()
    with Timer() as timer:
        for description in descriptions:
            with etl.monitor.Monitor(description.identifier, 'dump', dry_run=dry_run,
                                     options=["with-spark"],
                                     source={'name': description.source_name,
                                             'schema': description.source_table_name.schema,
                                             'table': description.source_table_name.table},
                                     destination={'bucket_name': description.bucket_name,
                                                  'object_key': description.manifest_file_name}):
                df = read_table_as_dataframe(sql_context, source, description)
                write_dataframe_as_csv(df, description, dry_run=dry_run)
                write_manifest_file(description, dry_run=dry_run)
            copied.add(description.target_table_name)
    logger.info("Finished with %d table(s) from source '%s' (%s)", len(copied), source.name, timer)


def dump_to_s3_with_spark(source_lookup, descriptions, dry_run=False):
    """
    Dump data from multiple upstream sources to S3 with Spark Dataframes
    """
    logger = logging.getLogger(__name__)
    logger.debug("Possibly dumping from these sources with spark: %s", etl.join_with_quotes(source_lookup))

    sql_context = create_sql_context()
    for source_name, description_group in groupby(descriptions, attrgetter("source_name")):
        dump_source_to_s3_with_spark(sql_context, source_lookup[source_name], description_group, dry_run=dry_run)


def build_sqoop_options(jdbc_url, username, password_file, relation_description, max_mappers=4):
    """
    Create set of Sqoop options.

    Starts with the command (import), then continues with generic options,
    tool specific options, and child-process options.
    """
    source_table_name = relation_description.source_table_name
    table_design = relation_description.table_design
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
            "--target-dir", '"s3n://{}/{}/{}"'.format(relation_description.bucket_name,
                                                      relation_description.prefix,
                                                      relation_description.csv_path_name),
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


def dump_table_with_sqoop(jdbc_url, dsn_properties, description, max_partitions, dry_run=False):
    """
    Run Sqoop for one table, creates the sub-process and all the pretty args for Sqoop.
    """
    logger = logging.getLogger(__name__)

    # We'll write some files into this directory to make it easier to re-run sqoop for debugging.
    sqoop_files = os.path.join(REDSHIFT_ETL_HOME, 'sqoop')
    create_dir_unless_exists(sqoop_files)

    password_file = write_password_file(dsn_properties["password"], dry_run=dry_run)
    args = build_sqoop_options(jdbc_url, dsn_properties["user"], password_file, description, max_mappers=max_partitions)
    logger.info("Sqoop options are:\n%s", " ".join(args))
    options_file = write_options_file(args, dry_run=dry_run)

    # Need to first delete directory since sqoop won't overwrite (and can't delete)
    csv_prefix = os.path.join(description.prefix, description.csv_path_name)
    deletable = sorted(etl.s3.list_objects_for_prefix(description.bucket_name, csv_prefix))
    if deletable:
        if dry_run:
            logger.info("Dry-run: Skipping deletion of existing CSV files 's3://%s/%s'",
                        description.bucket_name, csv_prefix)
        else:
            etl.s3.delete_objects(description.bucket_name, deletable)

    run_sqoop(options_file, dry_run=dry_run)
    write_manifest_file(description, dry_run=dry_run)


def dump_source_to_s3_with_sqoop(source, descriptions, max_partitions, keep_going=False, dry_run=False):
    """
    Dump all (selected) tables from a single upstream source.  Return list of tables for which dump failed.
    """
    logger = logging.getLogger(__name__)
    jdbc_url, dsn_properties = etl.pg.extract_dsn(source.dsn)

    dumped = 0
    failed = []

    with Timer() as timer:
        for description in descriptions:
            try:
                # FIXME The monitor should contain the number of rows that were dumped.
                with etl.monitor.Monitor(description.identifier, 'dump', dry_run=dry_run,
                                         options=["with-sqoop"],
                                         source={'name': source.name,
                                                 'schema': description.source_table_name.schema,
                                                 'table': description.source_table_name.table},
                                         destination={'bucket_name': description.bucket_name,
                                                      'object_key': description.manifest_file_name}):
                    dump_table_with_sqoop(jdbc_url, dsn_properties, description, max_partitions, dry_run=dry_run)
            except DataDumpError:
                failed.append(description.target_table_name)
                if not description.required:
                    logger.exception("Dump failed for non-required relation '%s':", description.identifier)
                elif keep_going:
                    logger.exception("Ignoring failure of required relation and proceeding as requested:")
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


def dump_to_s3_with_sqoop(source_lookup, descriptions, max_partitions, keep_going=False, dry_run=False):
    """
    Dump data from upstream sources to S3 with calls to Sqoop.

    It is ok to call this method with "sources" that are actually just derived schemas since
    those will be ignored.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Possibly dumping from these sources with sqoop: %s", etl.join_with_quotes(source_lookup))

    # TODO This will run all sources in parallel ... should this be a command line arg?
    max_workers = len(source_lookup)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for source_name, description_group in groupby(descriptions, attrgetter("source_name")):
            if source_name in source_lookup:
                logger.info("Dumping from source '%s'", source_name)
                f = executor.submit(dump_source_to_s3_with_sqoop,
                                    source_lookup[source_name], list(description_group),
                                    max_partitions, keep_going=keep_going, dry_run=dry_run)
                futures.append(f)
            else:
                logger.info("Skipping schema '%s' which is not a database source", source_name)
        if keep_going:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        else:
            done, not_done = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)

    # Note that iterating over result of futures may raise an exception (which surfaces exceptions from threads)
    failed_tables = []
    for future in done:
        failed_tables.extend(future.result())
    for table_name in failed_tables:
        logger.warning("Failed to dump: '%s'", table_name.identifier)

    if not_done:
        raise DataDumpError("Dump failed to complete for {:d} source(s)".format(len(not_done)))


def dump_static_table(source, description, dry_run=False):
    """
    Prepare to write manifest file for data files associated with the static data source
    """
    write_manifest_file(description, static_source=source, dry_run=dry_run)


def dump_static_source_to_s3(source, descriptions, keep_going=False, dry_run=False):
    """
    Dump all (selected) tables from a single static source.
    Return list of tables for which dump failed.
    """
    logger = logging.getLogger(__name__)

    dumped = 0
    failed = []

    with Timer() as timer:
        for description in descriptions:
            try:
                with etl.monitor.Monitor(description.identifier, 'dump', dry_run=dry_run,
                                         options=["static"],
                                         source={'name': source.name,
                                                 'schema': description.source_table_name.schema,
                                                 'table': description.source_table_name.table},
                                         destination={'bucket_name': description.bucket_name,
                                                      'object_key': description.manifest_file_name}):
                    dump_static_table(source, description, dry_run=dry_run)
            except DataDumpError:
                failed.append(description.target_table_name)
                if not description.is_required:
                    logger.exception("Dump failed for non-required relation '%s':", description.identifier)
                elif keep_going:
                    logger.exception("Ignoring failure of required relation and proceeding as requested:")
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


def dump_static_sources(source_lookup, descriptions, keep_going=False, dry_run=False):
    """
    "Dump" data from static sources which simply boils down to creating manifest files so that
    Redshift's COPY can find the CSV files from the S3 bucket tied to the static source.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Possibly dumping from these static sources: %s", etl.join_with_quotes(source_lookup))

    max_workers = len(source_lookup)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for source_name, description_group in groupby(descriptions, attrgetter("source_name")):
            if source_name in source_lookup:
                logger.info("Dumping from source '%s'", source_name)
                f = executor.submit(dump_static_source_to_s3, source_lookup[source_name], list(description_group),
                                    keep_going=keep_going, dry_run=dry_run)
                futures.append(f)
            else:
                logger.info("Skipping schema '%s' which is not a static source", source_name)
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


def dump_to_s3(dumper, schemas, descriptions, max_partitions, keep_going, dry_run):
    """
    Dump data from an upstream source to S3

    This is the entry point, and which technology will be used will be determined
    by the args here. Additionally, we don't care yet if any of the sources are static.
    """
    logger = logging.getLogger(__name__)

    static_sources = {source.name: source for source in schemas if source.is_static_source}
    if static_sources:
        dump_static_sources(static_sources, descriptions, keep_going=keep_going, dry_run=dry_run)
    else:
        logger.info("No static sources were selected")

    database_sources = {source.name: source for source in schemas if source.is_database_source}
    if not database_sources:
        logger.info("No database sources were selected")
        return

    with etl.pg.log_error():
        if dumper == "spark":
            dump_to_s3_with_spark(database_sources, descriptions, dry_run=dry_run)
        else:
            dump_to_s3_with_sqoop(database_sources, descriptions, max_partitions, keep_going=keep_going,
                                  dry_run=dry_run)
