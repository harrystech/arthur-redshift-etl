"""
Driver for the ETL based on sub-commands and central place to govern command line args.

This can be the entry point for a console script.  Some functions are broken out
so that they can be leveraged by utilities in addition to the top-level script.
"""

import argparse
import getpass
import logging
import os
import sys
import traceback

import boto3
import simplejson as json

import etl
import etl.config
import etl.design
import etl.dump
import etl.dw
import etl.file_sets
import etl.json_encoder
import etl.load
import etl.monitor
import etl.pg
import etl.relation
from etl.timer import Timer


class InvalidArgumentsError(Exception):
    """Exception thrown arguments are detected to be invalid by the command callack"""
    pass


def croak(error, exit_code):
    """
    Print first line of exception and then bail out with the exit code.

    When you have a large stack trace, it's easy to miss the trigger and
    so we call it out here again, on stderr.
    """
    full_tb = "\n".join(traceback.format_exception_only(type(error), error))
    header = full_tb.split('\n')[0]
    if sys.stderr.isatty():
        message = "Bailing out: \033[01;31m{}\033[0m".format(header)
    else:
        message = "Bailing out: {}".format(header)
    print(message, file=sys.stderr)
    sys.exit(exit_code)


def run_arg_as_command(my_name="arthur.py"):
    """
    Use the sub-command's callback to actually run the sub-command.
    Also measures execution time and does some basic error handling so that commands can be chained, UNIX-style.
    """
    parser = build_full_parser(my_name)
    args = parser.parse_args()
    if not args.func:
        parser.print_usage()
    elif args.cluster_id:
        submit_step(args.cluster_id, args.sub_command)
    else:
        etl.config.configure_logging(args.prolix, args.log_level)
        logger = logging.getLogger(__name__)
        with Timer() as timer:
            try:
                settings = etl.config.load_settings(args.config)
                if hasattr(args, "prefix"):
                    # Only commands which require an environment should require a monitor.
                    etl_events = settings("etl_events")
                    etl.monitor.set_environment(args.prefix,
                                                dynamodb_settings=etl_events.get("dynamodb", {}),
                                                postgresql_settings=etl_events.get("postgresql", {}))
                args.func(args, settings)
            except InvalidArgumentsError as exc:
                logger.exception("ETL never got off the ground:")
                croak(exc, 1)
            except etl.ETLException as exc:
                logger.exception("Something bad happened in the ETL:")
                logger.info("Ran for %.2fs before this untimely end!", timer.elapsed)
                croak(exc, 1)
            except Exception as exc:
                logger.exception("Something terrible happened:")
                logger.info("Ran for %.2fs before encountering disaster!", timer.elapsed)
                croak(exc, 2)
            except BaseException as exc:
                logger.exception("Something really terrible happened:")
                logger.info("Ran for %.2fs before an exceptional termination!", timer.elapsed)
                croak(exc, 3)
            else:
                logger.info("Ran for %.2fs and finished successfully!", timer.elapsed)


def submit_step(cluster_id, sub_command):
    """
    Send the current arthur command to a cluster instead of running it locally.
    """
    # We need to remove --submit and --config to avoid an infinite loop and insert a redirect to the config directory
    partial_parser = build_basic_parser('arthur.py')
    done, remaining = partial_parser.parse_known_args()
    try:
        client = boto3.client('emr')
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Arthur command: {}".format(str(sub_command).upper()),
                    # For "interactive" steps, allow a sequence of steps to continue after failure of one
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": ["/tmp/redshift_etl/venv/bin/arthur.py",
                                 "--config", "/tmp/redshift_etl/config"] + remaining
                    }
                }
            ]
        )
        step_id = response['StepIds'][0]
        status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        json.dump(status["Step"], sys.stdout, indent="    ", sort_keys=True, cls=etl.json_encoder.FancyJsonEncoder)
        sys.stdout.write('\n')
    except Exception as exc:
        logging.getLogger(__name__).exception("Adding step to job flow failed:")
        croak(exc, 1)


def build_basic_parser(prog_name, description=None):
    """
    Build basic parser that knows about the configuration setting.

    The `--config` option is central and can be easily avoided using the environment
    variable so is always here (meaning between 'arthur.py' and the sub-command).

    Similarly, '--submit-to-cluster' is shared for all sub-commands.
    """
    parser = argparse.ArgumentParser(prog=prog_name, description=description)
    group = parser.add_mutually_exclusive_group()

    # Show different help message depending on whether user has already set the environment variable.
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG", "./config")
    group.add_argument("-c", "--config",
                       help="add configuration path or file (default: '%s')" % default_config,
                       action="append", default=[default_config])
    group.add_argument("--submit-to-cluster", help="submit this command to the cluster (EXPERIMENTAL)",
                       dest="cluster_id")

    # Set some defaults (in case no sub-command's add_to_parser is called)
    parser.set_defaults(prolix=None)
    parser.set_defaults(log_level=None)
    parser.set_defaults(func=None)
    return parser


def build_full_parser(prog_name):
    """
    Build a parser by adding sub-parsers for sub-commands.
    Other options, even if shared between sub-commands, are in the sub-parsers to avoid
    having to awkwardly insert them between program name and sub-command name.

    :param prog_name: Name that should show up as command name in help
    :return: instance of ArgumentParser that is ready to parse and run sub-commands
    """
    parser = build_basic_parser(prog_name, description="This command allows to drive the Redshift ETL.")

    package = etl.package_version()
    parser.add_argument("-V", "--version", action="version", version="%(prog)s ({})".format(package))

    # Details for sub-commands lives with sub-classes of sub-commands. Hungry? Get yourself a sub-way.
    subparsers = parser.add_subparsers(help="specify one of these sub-commands (which can all provide more help)",
                                       title="available sub-commands",
                                       dest='sub_command')

    for klass in [
            # Commands to deal with data warehouse as admin:
            InitializeSetupCommand, CreateUserCommand,
            # Commands to help with table designs and uploading them
            DownloadSchemasCommand, ValidateDesignsCommand, ExplainQueryCommand, CopyToS3Command,
            # ETL commands to extract, load/update or do both
            DumpDataToS3Command, LoadRedshiftCommand, UpdateRedshiftCommand, ExtractLoadTransformCommand,
            # Helper commands
            ListFilesCommand, PingCommand, ShowSettingsCommand, EventsQueryCommand]:
        cmd = klass()
        cmd.add_to_parser(subparsers)

    return parser


def add_standard_arguments(parser, options):
    """
    Provide "standard arguments in the sense that the name and description should be the
    same when used by multiple sub-commands.

    :param parser: should be a sub-parser
    :param options: see option strings below, like "prefix", "drop" etc.
    """
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not modify stuff", default=False, action="store_true")
    if "prefix" in options:
        parser.add_argument("-p", "--prefix", default=getpass.getuser(),
                            help="select prefix in S3 bucket (default is user name: '%(default)s')")
    if "table-design-dir" in options:
        parser.add_argument("-t", "--table-design-dir",
                            help="set path to directory with table design files (default: '%(default)s')",
                            default="./schemas")
    if "stay-local" in options:
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-l", "--local-files", help="use files available on local filesystem",
                           action="store_true", dest="stay_local", default=True)
        group.add_argument("-r", "--remote-files", help="use files in S3",
                           action="store_false", dest="stay_local")
    if "max-partitions" in options:
        parser.add_argument("-m", "--max-partitions", metavar="N",
                            help="set max number of partitions to write to N (default: %(default)s)", default=4)
    if "drop" in options:
        parser.add_argument("-d", "--drop",
                            help="first drop table or view to force update of definition", default=False,
                            action="store_true")
    if "explain" in options:
        parser.add_argument("-x", "--add-explain-plan", help="add explain plan to log", action="store_true")
    if "pattern" in options:
        parser.add_argument("pattern", help="glob pattern or identifier to select table(s) or view(s)", nargs='*')
    if "username" in options:
        parser.add_argument("username", help="name for new user")


class SubCommand:
    """
    Instances (of child classes) will setup sub-parsers and have callbacks for those.
    """
    def __init__(self, name, help_, description):
        self.name = name
        self.help = help_
        self.description = description

    def add_to_parser(self, parent_parser):
        parser = parent_parser.add_parser(self.name, help=self.help, description=self.description)
        parser.set_defaults(func=self.callback)

        # Log level and prolix setting need to be always known since `run_arg_as_command` depends on them.
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-o", "--prolix", help="send full log to console", default=False, action="store_true")
        group.add_argument("-v", "--verbose", help="increase verbosity",
                           action="store_const", const="DEBUG", dest="log_level")
        group.add_argument("-q", "--quiet", help="decrease verbosity",
                           action="store_const", const="WARNING", dest="log_level")
        group.add_argument("-s", "--silent", help="DEPRECATED use --quiet instead",
                           action="store_const", const="WARNING", dest="log_level")

        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """Override this method for sub-classes"""
        pass

    def callback(self, args, settings):
        """Override this method for sub-classes"""
        raise NotImplementedError("Instance of {} has no proper callback".format(self.__class__.__name__))

    @staticmethod
    def pick_schemas(schemas, patterns):
        """
        Create pattern-based selector and apply right away on the schemas.
        Return selected schemas and the selector.
        It is an error is there is a pattern but no match at the schemas level.
        """
        if len(patterns) == 1 and patterns[0] == "@sources":
            selector = etl.TableNamePatterns.from_list([])
            selected_schemas = [schema for schema in schemas if "read_access" in schema]
        else:
            selector = etl.TableNamePatterns.from_list(patterns)
            selected_schemas = selector.match_field(schemas, "name")
        if not selected_schemas:
            raise InvalidArgumentsError("Found no match in settings for '{}'".format(selector.str_schemas()))
        return selected_schemas, selector

    @staticmethod
    def add_dsn_for_read_access(sources):
        """
        Lookup environment variables for read access to upstream sources and add the DSN
        """
        for source in sources:
            if "read_access" in source:
                source["dsn"] = etl.config.env_value(source["read_access"])

    @staticmethod
    def find_files_in_s3(bucket_name, prefix, schemas, selector):
        """
        Return file sets from the "environment" (meaning bucket and prefix)
        It is an error if no files are found.
        """
        file_sets = etl.file_sets.find_files_for_schemas(bucket_name, prefix, schemas, selector)
        if not file_sets:
            raise InvalidArgumentsError(
                "Found no matching files in 's3://{}/{}' for '{}'".format(bucket_name, prefix, selector))
        return file_sets

    @staticmethod
    def find_files_locally(schemas_dir, schemas, selector, empty_is_ok=False, orphans=False):
        """
        Return file sets from the local directory with table designs.
        Unless it is ok to have an empty set, an exception is raised.
        If `orphans` is set to True, TableFileSets lacking Design files will also be returned.
        """
        if os.path.exists(schemas_dir):
            file_sets = etl.file_sets.find_local_files_for_schemas(schemas_dir, schemas, selector, orphans=orphans)
            if not file_sets and not empty_is_ok:
                raise InvalidArgumentsError("Found no matching files in '{}' for '{}'".format(schemas_dir, selector))
        elif empty_is_ok:
            file_sets = []
        else:
            raise FileNotFoundError("Failed to find directory: '%s'" % schemas_dir)
        return file_sets


class ShowSettingsCommand(SubCommand):

    def __init__(self):
        super().__init__("show",
                         "show settings",
                         "Show data warehouse and ETL settings after loading all config files")

    def add_arguments(self, parser):
        parser.add_argument("key", help="key to select settings", nargs="*")

    def callback(self, args, settings):
        print(json.dumps(settings(*args.key), indent="    ", sort_keys=True))


class PingCommand(SubCommand):

    def __init__(self):
        super().__init__("ping",
                         "ping data warehouse",
                         "Try to connect to the data warehouse to test connection settings")

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-e", "--as-etl-user", help="try to connect as ETL user (default)",
                           action="store_const", const="etl_access", dest="access_type", default="etl_access")
        group.add_argument("-a", "--as-admin-user", help="try to connect as ETL user",
                           action="store_const", const="admin_access", dest="access_type")

    def callback(self, args, settings):
        dsn = etl.config.env_value(settings("data_warehouse", args.access_type))
        with etl.pg.log_error():
            etl.dw.ping(dsn)


class InitializeSetupCommand(SubCommand):

    def __init__(self):
        super().__init__("initialize",
                         "create schemas, users, and groups",
                         "Create schemas for each source, users, and user groups for etl and analytics")

    def add_arguments(self, parser):
        parser.add_argument("-r", "--skip-user-creation", help="skip user and groups; only create schemas",
                            default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.initial_setup(settings("data_warehouse"), settings("sources"), args.skip_user_creation)


class CreateUserCommand(SubCommand):

    def __init__(self):
        super().__init__("create_user",
                         "add new user",
                         "Add new user and set group membership, optionally add a personal schema")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["username"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-e", "--etl-user", help="add user also to ETL group", action="store_true")
        group.add_argument("-a", "--add-user-schema", help="add new schema, writable for the user",
                           action="store_true")
        group.add_argument("-r", "--skip-user-creation", help="skip new user; only change search path of existing user",
                           action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.create_user(settings("data_warehouse"), settings("sources"), args.username,
                               args.etl_user, args.add_user_schema, args.skip_user_creation)


class DownloadSchemasCommand(SubCommand):

    def __init__(self):
        super().__init__("design",
                         "bootstrap schema information from sources",
                         "Download schema information from upstream sources and compare against current table designs")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "table-design-dir", "dry-run"])
        parser.add_argument("-a", "--auto", help="run a new transformation as a view & autogenerate a design file from the view",
                            default=False, action="store_true")

    def callback(self, args, settings):
        if args.auto:
            schemas, selector = self.pick_schemas(settings('data_warehouse', 'schemas'), args.pattern)
            for schema in schemas:
                schema['dsn'] = etl.config.env_value(settings('data_warehouse', 'etl_access'))
                schema['include_tables'] = [schema['name'] + '.*']
        else:
            schemas, selector = self.pick_schemas(settings("sources"), args.pattern)
            self.add_dsn_for_read_access(schemas)

        local_files = self.find_files_locally(args.table_design_dir, schemas, selector,
                                              empty_is_ok=True, orphans=args.auto)
        if args.auto:
            created = etl.design.bootstrap_views(local_files, schemas, dry_run=args.dry_run)
        else:
            created = []
        try:
            etl.design.download_schemas(args.table_design_dir, local_files, schemas, selector, settings("type_maps"),
                                        dry_run=args.dry_run)
        finally:
            etl.design.cleanup_views(created, schemas, dry_run=args.dry_run)


class CopyToS3Command(SubCommand):

    def __init__(self):
        super().__init__("sync",
                         "copy table design files to S3",
                         "Copy table design files from local directory to S3."
                         " If using the '--force' option, this will delete schema and *data* files.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "table-design-dir", "prefix", "dry-run"])
        parser.add_argument("-f", "--force", help="force sync (deletes all matching files first, including data)",
                            default=False, action="store_true")

    def callback(self, args, settings):
        bucket_name = settings("s3", "bucket_name")
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        if args.force:
            etl.design.delete_in_s3(bucket_name, args.prefix, schemas, selector, dry_run=args.dry_run)
        local_files = self.find_files_locally(args.table_design_dir, schemas, selector)
        etl.design.copy_to_s3(local_files, bucket_name, args.prefix, dry_run=args.dry_run)


class DumpDataToS3Command(SubCommand):

    def __init__(self):
        super().__init__("dump",
                         "dump table data from sources",
                         "Dump table contents to files in S3 along with a manifest file")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "max-partitions", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--with-sqoop", help="dump data using Sqoop (using 'sqoop import', this is the default)",
                           const="sqoop", action="store_const", dest="dumper", default="sqoop")
        group.add_argument("--with-spark", help="dump data using Spark Dataframe (using submit_arthur.sh)",
                           const="spark", action="store_const", dest="dumper")
        parser.add_argument("-k", "--keep-going",
                            help="dump as much data as possible, ignoring errors along the way (Sqoop only)",
                            default=False, action="store_true")

    def callback(self, args, settings):
        bucket_name = settings("s3", "bucket_name")
        sources, selector = self.pick_schemas(settings("sources"), args.pattern)
        self.add_dsn_for_read_access(sources)
        file_sets = self.find_files_in_s3(bucket_name, args.prefix, sources, selector)

        if args.dumper == "sqoop":
            with etl.pg.log_error():
                etl.dump.dump_to_s3_with_sqoop(sources, bucket_name, args.prefix, file_sets, args.max_partitions,
                                               keep_going=args.keep_going, dry_run=args.dry_run)
        else:
            # Make sure that there is a Spark environment. If not, re-launch with spark-submit.
            if "SPARK_ENV_LOADED" in os.environ:
                with etl.pg.log_error():
                    etl.dump.dump_to_s3_with_spark(sources, bucket_name, args.prefix, file_sets, dry_run=args.dry_run)
            else:
                # Try the full path (in the EMR cluster), or try without path and hope for the best.
                submit_arthur = "/tmp/redshift_etl/venv/bin/submit_arthur.sh"
                if not os.path.exists(submit_arthur):
                    submit_arthur = "submit_arthur.sh"
                print("+ exec {} {}".format(submit_arthur, " ".join(sys.argv)), file=sys.stderr)
                os.execvp(submit_arthur, (submit_arthur,) + tuple(sys.argv))
                sys.exit(1)


class LoadRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("load",
                         "load data into Redshift from files in S3",
                         "Load data into Redshift from files in S3 (as a forced reload)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "explain", "dry-run"])
        parser.add_argument("-y", "--skip-copy",
                            help="skip the COPY command (for debugging)",
                            action="store_true")

    def callback(self, args, settings):
        bucket_name = settings("s3", "bucket_name")
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
        with etl.pg.log_error():
            etl.load.load_or_update_redshift(settings("data_warehouse"), bucket_name, file_sets,
                                             drop=True, skip_copy=args.skip_copy,
                                             add_explain_plan=args.add_explain_plan, dry_run=args.dry_run)


class UpdateRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("update",
                         "update data in Redshift",
                         "Update data in Redshift from files in S3 (without schema modifications)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "explain", "dry-run"])

    def callback(self, args, settings):
        # FIXME refactor with load command
        bucket_name = settings("s3", "bucket_name")
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
        with etl.pg.log_error():
            etl.load.load_or_update_redshift(settings("data_warehouse"), bucket_name, file_sets,
                                             drop=False, add_explain_plan=args.add_explain_plan, dry_run=args.dry_run)


class ExtractLoadTransformCommand(SubCommand):

    def __init__(self):
        super().__init__("etl",
                         "run complete ETL (or ELT)",
                         "Validate designs, extract data, and load data, possibly with transforms")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "max-partitions", "dry-run"])
        parser.add_argument("-f", "--force",
                            help="force loading, run 'dump' then 'load' (instead of 'dump' then 'update')",
                            default=False, action="store_true")

    def callback(self, args, settings):
        bucket_name = settings("s3", "bucket_name")
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        self.add_dsn_for_read_access(schemas)

        with etl.pg.log_error():
            file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
            etl.dump.dump_to_s3_with_sqoop(schemas, bucket_name, args.prefix, file_sets, args.max_partitions,
                                           dry_run=args.dry_run)
            # Need to rerun find files since the dump step has added files (data and manifests)
            file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
            etl.load.load_or_update_redshift(settings("data_warehouse"), bucket_name, file_sets,
                                             drop=args.force, dry_run=args.dry_run)


class ValidateDesignsCommand(SubCommand):

    def __init__(self):
        super().__init__("validate",
                         "validate table design files",
                         "Validate table designs (use '-q' to only see errors/warnings)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "table-design-dir", "prefix", "stay-local"])
        parser.add_argument("-k", "--keep-going", help="ignore errors and test as many files as possible",
                            default=False, action="store_true")
        parser.add_argument("-n", "--skip-dependencies-check",
                            help="skip check of dependencies against dependencies",
                            default=False, action="store_true")

    def callback(self, args, settings):
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        if args.stay_local:
            bucket_name = None  # implies localhost
            file_sets = self.find_files_locally(args.table_design_dir, schemas, selector)
        else:
            bucket_name = settings("s3", "bucket_name")
            file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
        dsn = etl.config.env_value(settings("data_warehouse", "etl_access"))
        etl.relation.validate_designs(dsn, file_sets, bucket_name,
                                      keep_going=args.keep_going, skip_deps=args.skip_dependencies_check)


class ExplainQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("explain",
                         "collect explain plans",
                         "Run EXPLAIN on queries (for CTAS or VIEW) for files in local filesystem")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "table-design-dir", "prefix", "stay-local"])

    def callback(self, args, settings):
        schemas, selector = self.pick_schemas(settings("data_warehouse", "schemas"), args.pattern)
        if args.stay_local:
            bucket_name = None  # implies localhost
            file_sets = self.find_files_locally(args.table_design_dir, schemas, selector)
        else:
            bucket_name = settings("s3", "bucket_name")
            file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
        dsn = etl.config.env_value(settings("data_warehouse", "etl_access"))
        etl.relation.test_queries(dsn, file_sets, bucket_name)


class ListFilesCommand(SubCommand):

    def __init__(self):
        super().__init__("ls",
                         "list files in S3",
                         "List files in the S3 bucket and starting with prefix by source, table, and type")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "table-design-dir", "prefix", "stay-local"])
        parser.add_argument("-a", "--long-format", help="add file size and timestamp of last modification",
                            action="store_true")

    def callback(self, args, settings):
        schemas, selector = self.pick_schemas(settings("sources") + settings("data_warehouse", "schemas"), args.pattern)
        # TODO This should be refactored such that the bucket name is stored with the file set (or relation description)
        if args.stay_local:
            bucket_name = None  # implies localhost
            file_sets = self.find_files_locally(args.table_design_dir, schemas, selector)
        else:
            bucket_name = settings("s3", "bucket_name")
            file_sets = self.find_files_in_s3(bucket_name, args.prefix, schemas, selector)
        etl.file_sets.list_files(file_sets, bucket_name, long_format=args.long_format)


class EventsQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("query",
                         "query the events table for the ETL",
                         "Query the table of events written during an ETL")

    def add_arguments(self, parser):
        parser.add_argument("--etl-id", help="pick ETL id to look for")
        add_standard_arguments(parser, ["pattern"])

    def callback(self, args, settings):
        etl.monitor.query_for(args.pattern, args.etl_id)


if __name__ == "__main__":
    run_arg_as_command()
