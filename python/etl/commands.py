"""
Driver for the ETL based on sub-commands and central place to govern command line args.

This can be the entry point for a console script.  Some functions are broken out
so that they can be leveraged by utilities in addition to the top-level script.
"""

import argparse
import logging
import os
import sys
import traceback
from contextlib import contextmanager

import boto3
import simplejson as json

import etl.config
import etl.config.env
import etl.design.bootstrap
import etl.explain
import etl.extract
import etl.dw
import etl.file_sets
import etl.json_encoder
import etl.load
import etl.monitor
import etl.names
import etl.pg
import etl.pipeline
import etl.relation
import etl.selftest
import etl.sync
import etl.unload
import etl.validate
from etl.errors import ETLDelayedExit, ETLError, ETLSystemError, InvalidArgumentsError
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


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


@contextmanager
def execute_or_bail():
    """
    Either execute (the wrapped code) successfully or bail out with a helpful error message.

    Also measures execution time and does some basic error handling so that commands can be chained, UNIX-style.
    """
    timer = Timer()
    try:
        yield
    except InvalidArgumentsError as exc:
        logger.exception("ETL never got off the ground:")
        croak(exc, 1)
    except ETLError as exc:
        if isinstance(exc, ETLDelayedExit):
            logger.critical("Something bad happened in the ETL: %s", str(exc))
        else:
            logger.critical("Something bad happened in the ETL:", exc_info=True)
        logger.info("Ran for %.2fs before this untimely end!", timer.elapsed)
        croak(exc, 2)
    except Exception as exc:
        logger.critical("Something terrible happened:", exc_info=True)
        logger.info("Ran for %.2fs before encountering disaster!", timer.elapsed)
        croak(exc, 3)
    except BaseException as exc:
        logger.critical("Something really terrible happened:", exc_info=True)
        logger.info("Ran for %.2fs before an exceptional termination!", timer.elapsed)
        croak(exc, 5)
    else:
        logger.info("Ran for %.2fs and finished successfully!", timer.elapsed)


def run_arg_as_command(my_name="arthur.py"):
    """
    Use the sub-command's callback in `func` to actually run the sub-command.
    This function can be used as an entry point for a console script.
    """
    parser = build_full_parser(my_name)
    args = parser.parse_args()
    if not args.func:
        parser.print_usage()
    elif args.cluster_id:
        submit_step(args.cluster_id, args.sub_command)
    else:
        # We need to configure logging before running context because that context expects logging to be setup.
        try:
            etl.config.configure_logging(args.prolix, args.log_level)
        except Exception as exc:
            croak(exc, 1)

        with execute_or_bail():
            settings = etl.config.load_config(args.config)
            setattr(args, "bucket_name", etl.config.get_data_lake_config("s3")["bucket_name"])

            if hasattr(args, "prefix"):
                # Only commands which require an environment should require a monitor.
                etl.monitor.set_environment(args.prefix,
                                            dynamodb_settings=settings["etl_events"].get("dynamodb", {}),
                                            postgresql_settings=settings["etl_events"].get("postgresql", {}))

            dw_config = etl.config.get_dw_config()
            if hasattr(args, "pattern") and hasattr(args.pattern, "base_schemas"):
                args.pattern.base_schemas = [s.name for s in dw_config.schemas]
            args.func(args, dw_config)


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
                        "Args": [etl.config.etl_tmp_dir("venv/bin/arthur.py"),
                                 "--config",
                                 etl.config.etl_tmp_dir("config")] + remaining
                    }
                }
            ]
        )
        step_id = response['StepIds'][0]
        status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        json.dump(status["Step"], sys.stdout, indent="    ", sort_keys=True, cls=etl.json_encoder.FancyJsonEncoder)
        sys.stdout.write('\n')
    except Exception as exc:
        logger.exception("Adding step to job flow failed:")
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
    parser = build_basic_parser(prog_name, description="This command allows to drive the ETL steps.")

    package = etl.config.package_version()
    parser.add_argument("-V", "--version", action="version", version="%(prog)s ({})".format(package))

    # Details for sub-commands lives with sub-classes of sub-commands. Hungry? Get yourself a sub-way.
    subparsers = parser.add_subparsers(help="specify one of these sub-commands (which can all provide more help)",
                                       title="available sub-commands",
                                       dest='sub_command')
    for klass in [
            # Commands to deal with data warehouse as admin:
            InitializeSetupCommand, CreateUserCommand,
            # Commands to help with table designs and uploading them
            BootstrapSourcesCommand, BootstrapTransformationsCommand, ValidateDesignsCommand, ExplainQueryCommand,
            SyncWithS3Command,
            # ETL commands to extract, load (or update), or transform
            ExtractToS3Command, LoadDataWarehouseCommand, UpgradeDataWarehouseCommand, UpdateDataWarehouseCommand,
            UnloadDataToS3Command,
            # Helper commands
            CreateSchemasCommand, RestoreSchemasCommand,
            ListFilesCommand, PingCommand, ShowDependentsCommand, ShowPipelinesCommand,
            EventsQueryCommand, SelfTestCommand]:
        cmd = klass()
        cmd.add_to_parser(subparsers)

    return parser


def add_standard_arguments(parser, options):
    """
    Provide "standard" arguments in the sense that the name and description should be the
    same when used by multiple sub-commands.

    :param parser: should be a sub-parser
    :param options: see option strings below, like "prefix", "pattern"
    """
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not modify stuff", default=False, action="store_true")
    if "prefix" in options:
        parser.add_argument("-p", "--prefix",
                            help="select prefix in S3 bucket (default: '%(default)s')",
                            default=etl.config.env.get_default_prefix())
    if "scheme" in options:
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-l", "--local-files", help="use files available on local filesystem (default)",
                           action="store_const", const="file", dest="scheme", default="file")
        group.add_argument("-r", "--remote-files", help="use files in S3",
                           action="store_const", const="s3", dest="scheme")
    if "max-partitions" in options:
        parser.add_argument("-m", "--max-partitions", metavar="N",
                            help="set max number of partitions to write to N (default: %(default)s)", default=4)
    if "skip-copy" in options:
        parser.add_argument("-y", "--skip-copy",
                            help="skip the COPY and INSERT commands (leaves tables empty, for debugging)",
                            action="store_true")
    if "pattern" in options:
        parser.add_argument("pattern", help="glob pattern or identifier to select table(s) or view(s)",
                            nargs='*', action=StorePatternAsSelector)
    # Cannot be set on the command line since changing it is not supported by file sets.
    parser.set_defaults(table_design_dir="./schemas")


class StorePatternAsSelector(argparse.Action):
    """
    Store the list of glob patterns (to pick tables) as a TableSelector instance.
    """
    def __call__(self, parser, namespace, values, option_string=None):
        selector = etl.names.TableSelector(values)
        setattr(namespace, "pattern", selector)


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
        # TODO move this into a parent parser and merge with --submit, --config
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-o", "--prolix", help="send full log to console", default=False, action="store_true")
        group.add_argument("-v", "--verbose", help="increase verbosity",
                           action="store_const", const="DEBUG", dest="log_level")
        group.add_argument("-q", "--quiet", help="decrease verbosity",
                           action="store_const", const="WARNING", dest="log_level")

        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """Override this method for sub-classes"""
        pass

    @staticmethod
    def location(args, default_scheme=None):
        """
        Decide whether we should focus on local or remote files and return appropriate "location" information.

        This expects args to be a Namespace instance from the argument parser with "table_design_dir",
        "bucket_name", "prefix", and hopefully "scheme" as fields.
        """
        scheme = getattr(args, "scheme", default_scheme)
        if scheme == "file":
            return scheme, "localhost", args.table_design_dir
        elif scheme == "s3":
            return scheme, args.bucket_name, args.prefix
        else:
            raise ETLSystemError("scheme invalid")

    def find_relation_descriptions(self, args, default_scheme=None, required_relation_selector=None, return_all=False):
        """
        Most commands need to (1) collect file sets and (2) create relation descriptions around those.
        Commands vary slightly as to what error handling they want to do and whether they need all
        possible descriptions or a selected subset.

        If a "required relation" selector is passed in, we first pick up ALL descriptions (to be able
        to build a dependency tree), build the dependency order, then pick out the matching descriptions.
        """
        if return_all or required_relation_selector is not None:
            selector = etl.names.TableSelector(base_schemas=args.pattern.base_schemas)
        else:
            selector = args.pattern
        file_sets = etl.file_sets.find_file_sets(self.location(args, default_scheme), selector)

        descriptions = etl.relation.RelationDescription.from_file_sets(
            file_sets, required_relation_selector=required_relation_selector)

        if not return_all and required_relation_selector is not None:
            descriptions = [d for d in descriptions if args.pattern.match(d.target_table_name)]

        return descriptions

    def callback(self, args, config):
        """Override this method for sub-classes"""
        raise NotImplementedError("Instance of {} has no proper callback".format(self.__class__.__name__))


class InitializeSetupCommand(SubCommand):

    def __init__(self):
        super().__init__("initialize",
                         "create ETL database",
                         "(Re)create database referenced in ETL credential, optionally creating users and groups."
                         " Normally, we expect this to be a validation database (name starts with 'validation')."
                         " When bringing up your primary production or development database, use the --force option.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument("-f", "--force", help="destructively initialize the referenced database regardless"
                            " of whether it looks like a validation database",
                            default=False, action="store_true")
        parser.add_argument("-u", "--with-user-creation", help="create users and groups before (re)creating database",
                            default=False, action="store_true")

    def callback(self, args, config):
        with etl.pg.log_error():
            etl.dw.initial_setup(config, with_user_creation=args.with_user_creation, force=args.force,
                                 dry_run=args.dry_run)


class CreateUserCommand(SubCommand):

    def __init__(self):
        super().__init__("create_user",
                         "add new user",
                         "Add new user and set group membership, optionally add a personal schema."
                         " Note that you have to set a password for the user in your .pgpass file"
                         " before invoking this command. The password must be valid in Redshift,"
                         " so must contain upper case and lower case characters as well as numbers.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument("username", help="name for new user")
        parser.add_argument("-g", "--group", help="add user to specified group")
        parser.add_argument("-a", "--add-user-schema", help="add new schema, writable for the user",
                            action="store_true")
        parser.add_argument("-r", "--skip-user-creation",
                            help="skip new user; only change search path of existing user", action="store_true")

    def callback(self, args, config):
        with etl.pg.log_error():
            etl.dw.create_new_user(config, args.username,
                                   group=args.group, add_user_schema=args.add_user_schema,
                                   skip_user_creation=args.skip_user_creation, dry_run=args.dry_run)


class BootstrapSourcesCommand(SubCommand):

    def __init__(self):
        super().__init__("design",
                         "bootstrap schema information from sources",
                         "Download schema information from upstream sources and compare against current table designs."
                         " If there is no current design file, then create one as a starting point.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])

    def callback(self, args, config):
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern, allow_empty=True)
        etl.design.bootstrap.bootstrap_sources(config.schemas, args.pattern, args.table_design_dir, local_files,
                                               dry_run=args.dry_run)


class BootstrapTransformationsCommand(SubCommand):

    def __init__(self):
        super().__init__("auto_design",
                         "bootstrap schema information for transformations",
                         "Download schema information as if transformation had been run in data warehouse."
                         " If there is no local design file, then create one as a starting point.")

    def add_arguments(self, parser):
        parser.add_argument("-f", "--force", help="overwrite table design file if it already exists",
                            default=False, action="store_true")
        # FIXME Switch to '[--update|--as-ctas|--as-view]'
        parser.add_argument("-u", "--update", help="EXPERIMENTAL merge with existing table design if available",
                            default=False, action="store_true")
        parser.add_argument('type', choices=['CTAS', 'VIEW'],
                            help="pick whether to create table designs for 'CTAS' or 'VIEW' relations")
        add_standard_arguments(parser, ["pattern", "dry-run"])

    def callback(self, args, config):
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
        etl.design.bootstrap.bootstrap_transformations(config.dsn_etl, config.schemas,
                                                       args.table_design_dir, local_files, args.type == "VIEW",
                                                       update=args.update, replace=args.force, dry_run=args.dry_run)


class SyncWithS3Command(SubCommand):

    def __init__(self):
        super().__init__("sync",
                         "copy table design files to S3",
                         "Copy table design files from local directory to S3."
                         " If using the '--force' option, this will delete schema and *data* files."
                         " If using the '--deploy' option, this will also upload files with warehouse settings"
                         " (*.yaml or *.sh files in config directories, excluding credentials*.sh).")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        parser.add_argument("-f", "--force", help="force sync (deletes all matching files first, including data)",
                            default=False, action="store_true")
        parser.add_argument("-d", "--deploy-config",
                            help="sync local settings files (*.yaml, *.sh) to <prefix>/config folder",
                            default=False, action="store_true")

    def callback(self, args, config):
        if args.deploy_config:
            etl.sync.upload_settings(args.config, args.bucket_name, args.prefix, dry_run=args.dry_run)
        if args.force:
            etl.file_sets.delete_files_in_bucket(args.bucket_name, args.prefix, args.pattern, dry_run=args.dry_run)

        descriptions = self.find_relation_descriptions(args, default_scheme="file")
        etl.sync.sync_with_s3(descriptions, args.bucket_name, args.prefix, dry_run=args.dry_run)


class ExtractToS3Command(SubCommand):

    def __init__(self):
        super().__init__("extract",
                         "extract data from upstream sources",
                         "Extract table contents from upstream databases (unless you decide to use existing"
                         " CSV files) and then gather references to CSV files in S3 into manifests file."
                         " (This last step is the only step needed for static sources where data is created"
                         " outside the ETL.)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--with-sqoop", help="extract data using Sqoop (using 'sqoop import', this is the default)",
                           const="sqoop", action="store_const", dest="extractor", default="sqoop")
        group.add_argument("--with-spark", help="extract data using Spark Dataframe (using submit_arthur.sh)",
                           const="spark", action="store_const", dest="extractor")
        group.add_argument("--use-existing-csv-files",
                           help="skip extraction and go straight to creating manifest files,"
                           " implied default for static sources",
                           const="use-existing-csv-files", action="store_const", dest="extractor")
        parser.add_argument("-k", "--keep-going",
                            help="extract as much data as possible, ignoring errors along the way",
                            default=False, action="store_true")
        parser.add_argument("-m", "--max-partitions", metavar="N", type=int,
                            help="set max number of partitions to write to N (default: %(default)s)",
                            default=8)
        parser.add_argument("--use-sampling",
                            help="use only 10%% of rows in extracted tables that are larger than 1MB",
                            default=False, action="store_true")

    def callback(self, args, config):
        if args.max_partitions < 1:
            raise InvalidArgumentsError("Option for max partitions must be >= 1")

        # Make sure that there is a Spark environment. If not, re-launch with spark-submit.
        # (Without this step, the Spark context is unknown and we won't be able to create a SQL context.)
        if args.extractor == "spark" and "SPARK_ENV_LOADED" not in os.environ:
            # Try the full path (in the EMR cluster), or try without path and hope for the best.
            submit_arthur = etl.config.etl_tmp_dir("venv/bin/submit_arthur.sh")
            if not os.path.exists(submit_arthur):
                submit_arthur = "submit_arthur.sh"
            print("+ exec {} {}".format(submit_arthur, " ".join(sys.argv)), file=sys.stderr)
            os.execvp(submit_arthur, (submit_arthur,) + tuple(sys.argv))
            sys.exit(1)

        descriptions = self.find_relation_descriptions(args, default_scheme="s3",
                                                       required_relation_selector=config.required_in_full_load_selector)
        etl.extract.extract_upstream_sources(args.extractor, config.schemas, descriptions,
                                             max_partitions=args.max_partitions,
                                             use_sampling=args.use_sampling,
                                             keep_going=args.keep_going,
                                             dry_run=args.dry_run)


class LoadDataWarehouseCommand(SubCommand):

    def __init__(self):
        super().__init__("load",
                         "load data into source tables and forcefully update all dependencies",
                         "Load data into the data warehouse from files in S3, which will *rebuild* the data warehouse."
                         " This will operate on entire schemas at once, which will be backed up as necessary."
                         " It is an error to try to select tables unless they are all the tables in the schema.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "skip-copy", "dry-run"])
        parser.add_argument("--no-rollback",
                            help="in case of error, leave warehouse in partially completed state (for debugging)",
                            action="store_true")

    def callback(self, args, config):
        try:
            args.pattern.selected_schemas()
        except ValueError as exc:
            raise InvalidArgumentsError(exc) from exc

        relations = self.find_relation_descriptions(args, default_scheme="s3",
                                                    required_relation_selector=config.required_in_full_load_selector,
                                                    return_all=True)
        with etl.pg.log_error():
            etl.load.load_data_warehouse(relations, args.pattern,
                                         skip_copy=args.skip_copy,
                                         no_rollback=args.no_rollback,
                                         dry_run=args.dry_run)


class UpgradeDataWarehouseCommand(SubCommand):

    def __init__(self):
        super().__init__("upgrade",
                         "load data into source or CTAS tables, create dependent VIEWS along the way",
                         "Delete selected tables and views, then rebuild them along with all of relations"
                         " that depend on the selected ones. This is for debugging since the rebuild is"
                         " visible to users (i.e. outside a transaction).")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "skip-copy", "dry-run"])
        parser.add_argument("--only-selected",
                            help="skip rebuilding relations that only depend on the selected ones"
                            " (leaves warehouse in inconsistent state, for debugging only)",
                            default=False, action="store_true")

    def callback(self, args, config):
        relations = self.find_relation_descriptions(args, default_scheme="s3",
                                                    required_relation_selector=config.required_in_full_load_selector,
                                                    return_all=True)
        with etl.pg.log_error():
            etl.load.upgrade_data_warehouse(relations, args.pattern,
                                            only_selected=args.only_selected,
                                            skip_copy=args.skip_copy,
                                            dry_run=args.dry_run)


class UpdateDataWarehouseCommand(SubCommand):

    def __init__(self):
        super().__init__("update",
                         "update data in the data warehouse from files in S3",
                         "Load data into data warehouse from files in S3 and then update all dependent CTAS relations.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        parser.add_argument("--only-selected",
                            help="only load data into selected relations"
                                 " (leaves warehouse in inconsistent state, for debugging only, default: %(default)s)",
                            default=False, action="store_true")
        parser.add_argument("--vacuum", help="run vacuum after the update to tidy up the place (default: %(default)s)",
                            default=False, action="store_true")

    def callback(self, args, config):
        relations = self.find_relation_descriptions(args, default_scheme="s3", return_all=True)
        with etl.pg.log_error():
            etl.load.update_data_warehouse(relations, args.pattern,
                                           only_selected=args.only_selected, run_vacuum=args.vacuum,
                                           dry_run=args.dry_run)


class UnloadDataToS3Command(SubCommand):

    def __init__(self):
        super().__init__("unload",
                         "unload data from data warehouse to files in S3",
                         "Unload data from data warehouse into files in S3 (along with files of column names).")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        parser.add_argument("-f", "--force",
                            help="enable ALLOWOVERWRITE option to replace existing data files in S3",
                            action="store_true")
        parser.add_argument("-k", "--keep-going",
                            help="unload as much data as possible, ignoring errors along the way",
                            default=False, action="store_true")

    def callback(self, args, config):
        descriptions = self.find_relation_descriptions(args, default_scheme="s3")
        with etl.pg.log_error():
            etl.unload.unload_to_s3(config, descriptions, args.prefix, allow_overwrite=args.force,
                                    keep_going=args.keep_going, dry_run=args.dry_run)


class CreateSchemasCommand(SubCommand):

    def __init__(self):
        super().__init__("create_schemas",
                         "create schemas from data warehouse config",
                         "Create schemas as configured and set permissions."
                         " Optionally move existing schemas to backup."
                         " (Any patterns must be schema names.)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])
        parser.add_argument("-b", "--with-backup", help="backup any existing schemas",
                            default=False, action="store_true")

    def callback(self, args, config):
        schema_names = args.pattern.selected_schemas()
        schemas = [schema for schema in config.schemas if schema.name in schema_names]
        with etl.pg.log_error():
            if args.with_backup:
                etl.dw.backup_schemas(config.dsn_etl, schemas, dry_run=args.dry_run)
            etl.dw.create_schemas(config.dsn_etl, schemas, dry_run=args.dry_run)


class RestoreSchemasCommand(SubCommand):

    def __init__(self):
        super().__init__("restore_schemas",
                         "restore schemas from their backup (after an interrupted load)",
                         "Restore schemas from their backup which should only be useful or used after killing a load.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])

    def callback(self, args, config):
        schema_names = args.pattern.selected_schemas()
        schemas = [schema for schema in config.schemas if schema.name in schema_names]
        with etl.pg.log_error():
            etl.dw.restore_schemas(config.dsn_etl, schemas, dry_run=args.dry_run)


class ValidateDesignsCommand(SubCommand):

    def __init__(self):
        super().__init__("validate",
                         "validate table design files",
                         "Validate table designs by checking their syntax and compatibility with the database"
                         " (use '-nskq' to see only errors or warnings, without connecting to a database).")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument("-k", "--keep-going", help="ignore errors and test as many files as possible",
                            default=False, action="store_true")
        parser.add_argument("-s", "--skip-sources-check",
                            help="skip check of designs against upstream databases",
                            default=False, action="store_true")
        parser.add_argument("-n", "--skip-dependencies-check",
                            help="skip check of dependencies in designs against data warehouse",
                            default=False, action="store_true")

    def callback(self, args, config):
        # NB This does not pick up all designs to speed things up but that may lead to false positives.
        descriptions = self.find_relation_descriptions(args)
        etl.validate.validate_designs(config, descriptions, keep_going=args.keep_going,
                                      skip_sources=args.skip_sources_check,
                                      skip_dependencies=args.skip_dependencies_check)


class ExplainQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("explain",
                         "collect explain plans",
                         "Run EXPLAIN on queries (for CTAS or VIEW), check query plan for distributions.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])

    def callback(self, args, config):
        if args.scheme == "file":
            # When running locally, we accept that there be only a SQL file.
            local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
            descriptions = [etl.relation.RelationDescription(file_set) for file_set in local_files
                            if file_set.sql_file_name]
        else:
            # When running with S3, we expect full sets of files (SQL plus table design)
            descriptions = self.find_relation_descriptions(args)
        with etl.pg.log_error():
            etl.explain.explain_queries(config.dsn_etl, descriptions)


class ListFilesCommand(SubCommand):

    def __init__(self):
        super().__init__("ls",
                         "list files in S3",
                         "List files in the S3 bucket and starting with prefix by source, table, and file type.")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument("-a", "--long-format", help="add file size and timestamp of last modification",
                            action="store_true")

    def callback(self, args, config):
        file_sets = etl.file_sets.find_file_sets(self.location(args), args.pattern)
        etl.file_sets.list_files(file_sets, long_format=args.long_format)


class PingCommand(SubCommand):

    def __init__(self):
        super().__init__("ping",
                         "ping data warehouse",
                         "Try to connect to the data warehouse to test connection settings.")

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-a", "--as-admin-user", help="try to connect as admin user",
                           action="store_true", dest="use_admin")
        group.add_argument("-e", "--as-etl-user", help="try to connect as ETL user (default)",
                           action="store_false", dest="use_admin")

    def callback(self, args, config):
        dsn = config.dsn_admin if args.use_admin else config.dsn_etl
        with etl.pg.log_error():
            etl.pg.ping(dsn)


class ShowDependentsCommand(SubCommand):

    def __init__(self):
        super().__init__("show_dependents",
                         "show dependent relations",
                         "Show relations in execution order (includes selected and all dependent relations).")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])

    def callback(self, args, config):
        descriptions = self.find_relation_descriptions(args,
                                                       required_relation_selector=config.required_in_full_load_selector,
                                                       return_all=True)
        etl.load.show_dependents(descriptions, args.pattern)


class ShowPipelinesCommand(SubCommand):

    def __init__(self):
        super().__init__("show_pipelines",
                         "show installed pipelines",
                         "Show additional information about currently installed pipelines.")

    def add_arguments(self, parser):
        parser.add_argument("selection", help="pick pipelines to show", nargs="*")

    def callback(self, args, config):
        etl.pipeline.show_pipelines(args.selection)


class EventsQueryCommand(SubCommand):

    def __init__(self):
        super().__init__("query",
                         "query the events table for the ETL",
                         "Query the table of events written during an ETL.")

    def add_arguments(self, parser):
        parser.add_argument("--etl-id", help="pick ETL id to look for")
        parser.add_argument("pattern", help="limit what to show", nargs='?')

    def callback(self, args, config):
        etl.monitor.query_for(args.pattern, args.etl_id)


class SelfTestCommand(SubCommand):

    def __init__(self):
        super().__init__("selftest",
                         "run code tests of ETL",
                         "Run self-test of the ETL.")

    def add_arguments(self, parser):
        # For self-tests, dial logging back to (almost) nothing so that logging in console doesn't mix with test output.
        parser.set_defaults(log_level="CRITICAL")
        parser.add_argument("test_family", help="select which family of tests to run",
                            nargs='?', choices=["doctest", "typecheck", "all"], default="all")

    def callback(self, args, config):
        if args.test_family in ("doctest", "all"):
            etl.selftest.run_doctest("etl", args.log_level)
        if args.test_family in ("typecheck", "all"):
            etl.selftest.run_type_checker()


if __name__ == "__main__":
    run_arg_as_command()
