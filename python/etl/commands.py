"""
Driver for the ETL based on sub-commands and central place to govern command line args.

This can be the entry point for a console script.  Some functions are broken out
so that they can be leveraged by utilities in addition to the top-level script.
"""

import argparse
from datetime import datetime
import getpass
import logging
import os
import uuid
import sys
import traceback

import boto3
import pkg_resources
import simplejson as json

import etl
import etl.config
import etl.dump
import etl.dw
from etl.json_encoder import FancyJsonEncoder
import etl.load
import etl.monitor
import etl.pg
import etl.s3
import etl.schemas
from etl.timer import Timer


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
                etl_events = settings("etl_events")
                etl.monitor.set_environment(getattr(args, "prefix", None),
                                            dynamodb_settings=etl_events.get("dynamodb", {}),
                                            postgresql_settings=etl_events.get("postgresql", {}))
                args.func(args, settings)
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
                    "Name": "Arthur command: {}".format(sub_command),
                    "ActionOnFailure": "CANCEL_AND_WAIT",
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
        json.dump(status["Step"], sys.stdout, indent="    ", sort_keys=True, cls=FancyJsonEncoder)
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
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG")
    if default_config is None:
        group.add_argument("-c", "--config",
                           help="add configuration path or file (required if DATA_WAREHOUSE_CONFIG is not set)",
                           action="append", default=[])
        # N.B. With mutually exclusive params, cannot require config ... so this will fail with missing settings.
    else:
        group.add_argument("-c", "--config",
                           help="add configuration path or file (adds to DATA_WAREHOUSE_CONFIG='%s')" % default_config,
                           action="append", default=[default_config])
    group.add_argument("--submit-to-cluster", help="submit this command to the cluster (EXPERIMENTAL)",
                       dest="cluster_id")

    # Set defaults so that we can avoid having to test the Namespace object.
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

    package = "redshift-etl v{}".format(pkg_resources.get_distribution("redshift-etl").version)
    parser.add_argument("-V", "--version", action="version", version="%(prog)s ({})".format(package))

    # Details for sub-commands lives with sub-classes of sub-commands. Hungry? Get yourself a sub-way.
    subparsers = parser.add_subparsers(help="specify one of these sub-commands (which can all provide more help)",
                                       title="available sub-commands",
                                       dest='sub_command')

    for klass in [InitialSetupCommand, CreateUserCommand,
                  DownloadSchemasCommand, CopyToS3Command, DumpDataToS3Command,
                  LoadRedshiftCommand, UpdateRedshiftCommand, ExtractLoadTransformCommand,
                  ValidateDesignsCommand, ListFilesCommand,
                  PingCommand, ExplainQueryCommand, ShowSettingsCommand,
                  EventsQueryCommand]:
        cmd = klass()
        cmd.add_to_parser(subparsers)

    return parser


def add_standard_arguments(parser, options):
    """
    Provide "standard arguments in the sense that the name and description should be the
    same when used by multiple sub-commands. Also there are some common arguments
    like verbosity that should be part of every sub-command's parser without asking for it.

    :param parser: should be a sub-parser
    :param options: see option strings below, like "prefix", "drop" etc.
    """

    # Choice between verbose and silent simply affects the log level.
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-o", "--prolix", help="send full log to console", default=False, action="store_true")
    group.add_argument("-v", "--verbose", help="increase verbosity",
                       action="store_const", const="DEBUG", dest="log_level")
    group.add_argument("-s", "--silent", help="decrease verbosity",
                       action="store_const", const="WARNING", dest="log_level")

    example_password = uuid.uuid4().hex.title()

    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not modify stuff", default=False, action="store_true")
    if "prefix" in options:
        prefix = parser.add_mutually_exclusive_group()
        prefix.add_argument("-p", "--prefix", default=getpass.getuser(),
                            help="select prefix in S3 bucket (default is user name: '%(default)s')")
        prefix.add_argument("-e", "--prefix-with-date", dest="prefix", metavar="PREFIX", action=AppendDateAction,
                            help="set prefix in S3 bucket to '<PREFIX>/<CURRENT_DATE>'")
    if "table-design-dir" in options:
        parser.add_argument("-t", "--table-design-dir",
                            help="set path to directory with table design files (default: '%(default)s')",
                            default="./schemas")
    if "max-partitions" in options:
        parser.add_argument("-m", "--max-partitions", metavar="N",
                            help="set max number of partitions to write to N (default: %(default)s)", default=4)
    if "drop" in options:
        parser.add_argument("-d", "--drop",
                            help="first drop table or view to force update of definition", default=False,
                            action="store_true")
    if "explain" in options:
        parser.add_argument("-x", "--add-explain-plan", help="add explain plan to log", action="store_true")
    if "target" in options:
        parser.add_argument("target", help="glob pattern or identifier to select target(s)", nargs='*')
    if "username" in options:
        parser.add_argument("username", help="name for new user")
    if "password" in options:
        parser.add_argument("password", help="password for new user (example: '%s')" % example_password, nargs='?')

    return parser


class AppendDateAction(argparse.Action):

    """Callback for argument parser to append current date so that environment has one folder per day."""

    def __call__(self, parser, namespace, values, option_string=None):
        today = datetime.now().strftime("/%Y%m%d_%H%M")
        prefix = values.rstrip('/') + today
        setattr(namespace, self.dest, prefix)


class SubCommand:

    """Instances (of child classes) will setup sub-parsers and have callbacks for those."""

    def __init__(self, name, help, description):
        self.name = name
        self.help = help
        self.description = description

    def add_to_parser(self, parent_parser):
        parser = parent_parser.add_parser(self.name,
                                          help=self.help,
                                          description=self.description)
        parser.set_defaults(func=self.callback)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """Overwrite this method for sub-classes"""
        pass

    def callback(self, args, settings):
        """Overwrite this method for sub-classes"""
        raise NotImplementedError("Instance of {} has no proper callback".format(self.__class__.__name__))


class ShowSettingsCommand(SubCommand):

    def __init__(self):
        super().__init__("show",
                         "Show settings",
                         "Show data warehouse and ETL settings after loading all config files")

    def callback(self, args, settings):
        print(json.dumps(settings(), indent="    ", sort_keys=True))


class PingCommand(SubCommand):

    def __init__(self):
        super().__init__("ping",
                         "Ping data warehouse",
                         "Try to connect to the data warehouse to test connection settings")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.ping(settings)


class InitialSetupCommand(SubCommand):

    def __init__(self):
        super().__init__("initialize",
                         "Create schemas, users, and groups",
                         "Create schemas for each source, users, and user groups for etl and analytics")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["password"])
        parser.add_argument("-r", "--skip-user-creation", help="skip user and groups; only create schemas",
                            default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.initial_setup(settings, args.password, args.skip_user_creation)


class CreateUserCommand(SubCommand):

    def __init__(self):
        super().__init__("create_user",
                         "Add new user",
                         "Add new user and set group membership, optionally add a personal schema")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["username", "password"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-e", "--etl-user", help="add user also to ETL group", action="store_true")
        group.add_argument("-a", "--add-user-schema",
                           help="add new schema, writable for the user",
                           action="store_true")
        group.add_argument("-r", "--skip-user-creation",
                           help="skip new user; only change search path of existing user",
                           default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.create_user(settings, args.username, args.password, args.etl_user,
                               args.add_user_schema, args.skip_user_creation)


class DownloadSchemasCommand(SubCommand):

    def __init__(self):
        super().__init__("design",
                         "Bootstrap schema information from sources",
                         "Download schema information from upstream sources (and compare against current table designs)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "table-design-dir", "dry-run"])

    def callback(self, args, settings):
        etl.schemas.download_schemas(settings, args.target, args.table_design_dir, args.dry_run)


class CopyToS3Command(SubCommand):

    def __init__(self):
        super().__init__("sync",
                         "Copy table design files to S3",
                         "Copy table design files from local directory to S3."
                         " If using the '--force' option, this will delete schema and *data* files.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "table-design-dir", "prefix", "dry-run"])
        parser.add_argument("-f", "--force", help="force sync (deletes all matching files first, including data)",
                            default=False, action="store_true")

    def callback(self, args, settings):
        etl.schemas.copy_to_s3(settings, args.target, args.table_design_dir, args.prefix, args.force, args.dry_run)


class DumpDataToS3Command(SubCommand):

    def __init__(self):
        super().__init__("dump",
                         "Dump table data from sources",
                         "Dump table contents to files in S3 along with a manifest file")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "prefix", "max-partitions", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--sqoop",
                           help="dump data using Sqoop (using 'sqoop import', this is the default)",
                           const="sqoop", action="store_const", dest="dumper", default="sqoop")
        group.add_argument("--spark",
                           help="dump data using Spark Dataframe (using submit_arthur.sh)",
                           const="spark", action="store_const", dest="dumper")
        parser.add_argument("-k", "--keep-going",
                            help="dump as much data as possible, ignoring errors along the way",
                            default=False, action="store_true")

    def callback(self, args, settings):
        if args.dumper == "spark":
            # Make sure that there is a Spark environment.  If not, re-launch with spark-submit.
            if "SPARK_ENV_LOADED" in os.environ:
                with etl.pg.log_error():
                    etl.dump.dump_to_s3_with_spark(settings, args.target, args.prefix,
                                                   keep_going=args.keep_going, dry_run=args.dry_run)
            else:
                # Try the full path (in the EMR cluster), or try without path and hope for the best.
                submit_arthur = "/tmp/redshift_etl/venv/bin/submit_arthur.sh"
                if not os.path.exists(submit_arthur):
                    submit_arthur = "submit_arthur.sh"
                print("+ exec {} {}".format(submit_arthur, " ".join(sys.argv)), file=sys.stderr)
                os.execvp(submit_arthur, (submit_arthur,) + tuple(sys.argv))
                sys.exit(1)
        else:
            with etl.pg.log_error():
                etl.dump.dump_to_s3_with_sqoop(settings, args.target, args.prefix, args.max_partitions,
                                               keep_going=args.keep_going, dry_run=args.dry_run)


class LoadRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("load",
                         "Load data into Redshift from files in S3",
                         "Load data into Redshift from files in S3 (as a forced reload)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "prefix", "explain", "dry-run"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.load.load_or_update_redshift(settings, args.target, args.prefix,
                                             add_explain_plan=args.add_explain_plan, drop=True, dry_run=args.dry_run)


class UpdateRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("update",
                         "Update data in Redshift",
                         "Update data in Redshift from files in S3 (without schema modifications)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "prefix", "explain", "dry-run"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.load.load_or_update_redshift(settings, args.target, args.prefix,
                                             add_explain_plan=args.add_explain_plan, drop=False, dry_run=args.dry_run)


class ExtractLoadTransformCommand(SubCommand):

    def __init__(self):
        super().__init__("etl",
                         "Run complete ETL (or ELT)",
                         "Validate designs, extract data, and load data, possibly with transforms")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "prefix", "max-partitions", "dry-run"])
        parser.add_argument("-f", "--force",
                            help="force loading, run 'dump' then 'load' (instead of 'dump' then 'update')",
                            default=False, action="store_true")

    def callback(self, args, settings):
        etl.dump.dump_to_s3_with_sqoop(settings, args.target, args.prefix, args.max_partitions, dry_run=args.dry_run)
        etl.load.load_or_update_redshift(settings, args.target, args.prefix, drop=args.force, dry_run=args.dry_run)


class ValidateDesignsCommand(SubCommand):

    def __init__(self):
        super().__init__("validate",
                         "Validate table design files",
                         "Validate table designs in local filesystem")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["table-design-dir", "target"])
        parser.add_argument("-k", "--keep-going",
                            help="ignore errors and test as many files as possible",
                            default=False, action="store_true")

    def callback(self, args, settings):
        etl.schemas.validate_designs(settings, args.target, args.table_design_dir, keep_going=args.keep_going)


class ExplainQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("explain",
                         "Collect explain plans",
                         "Run EXPLAIN on queries (for CTAS or VIEW) for LOCAL files")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["table-design-dir", "target"])

    def callback(self, args, settings):
        etl.load.test_queries(settings, args.target, args.table_design_dir)


class ListFilesCommand(SubCommand):

    def __init__(self):
        super().__init__("ls",
                         "List files in S3",
                         "List files in the S3 bucket and starting with prefix by source, table, and type")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "target"])
        parser.add_argument("-l", "--long-format",
                            help="add file size and timestamp of last modification",
                            action="store_true")

    def callback(self, args, settings):
        etl.s3.list_files(settings, args.prefix, args.target, long_format=args.long_format)


class EventsQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("query",
                         "Query the events table for the ETL",
                         "Query the table of events written during an ETL")

    def add_arguments(self, parser):
        parser.add_argument("--etl-id", help="pick ETL id to look for")
        add_standard_arguments(parser, ["target"])

    def callback(self, args, settings):
        etl.monitor.query_for(args.target, args.etl_id)


if __name__ == "__main__":
    run_arg_as_command()
