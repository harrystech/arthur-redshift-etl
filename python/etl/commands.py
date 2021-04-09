"""
Driver for the ETL based on sub-commands and central place to govern command line args.

This can be the entry point for a console script.  Some functions are broken out
so that they can be leveraged by utilities in addition to the top-level script.
"""

import argparse
import logging
import os
import shlex
import sys
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import boto3
import simplejson as json
from termcolor import colored

import etl.config
import etl.config.env
import etl.config.log
import etl.config.settings
import etl.data_warehouse
import etl.db
import etl.design.bootstrap
import etl.dialect
import etl.explain
import etl.extract
import etl.file_sets
import etl.json_encoder
import etl.load
import etl.monitor
import etl.names
import etl.pipeline
import etl.relation
import etl.selftest
import etl.sync
import etl.templates
import etl.unload
import etl.validate
from etl.errors import ETLError, ETLSystemError, InvalidArgumentError
from etl.text import join_with_single_quotes
from etl.timer import Timer

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def croak(error, exit_code):
    """
    Print first line of exception and then bail out with the exit code.

    When you have a large stack trace, it's easy to miss the trigger and
    so we call it out here again on stderr.
    """
    exception_only = traceback.format_exception_only(type(error), error)[0]
    header = exception_only.splitlines()[0]
    # Make sure to not send random ASCII sequences to a log file.
    if sys.stderr.isatty():
        header = colored(header, color="red", attrs=["bold"])
    print(f"Bailing out: {header}", file=sys.stderr)
    sys.exit(exit_code)


@contextmanager
def execute_or_bail():
    """
    Either execute (the wrapped code) successfully or bail out with a helpful error message.

    Also measures execution time and does some basic error handling so that commands can be
    chained, UNIX-style.
    """
    timer = Timer()
    try:
        yield
    except InvalidArgumentError as exc:
        logger.debug("Caught exception:", exc_info=True)
        logger.error("ETL never got off the ground: %r", exc)
        croak(exc, 1)
    except ETLError as exc:
        logger.critical("Something bad happened in the ETL: %s\n%s", type(exc).__name__, exc, exc_info=True)
        if exc.__cause__ is not None:
            exc_cause_type = type(exc.__cause__)
            logger.info(
                "The direct cause of this exception was: '%s.%s'",
                exc_cause_type.__module__,
                exc_cause_type.__qualname__,
            )
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
    elif args.cluster_id is not None:
        submit_step(args.cluster_id, args.sub_command)
    else:
        # We need to configure logging before running context because that context expects
        # logging to be setup.
        try:
            etl.config.log.configure_logging(args.prolix, args.log_level)
        except Exception as exc:
            croak(exc, 1)

        with execute_or_bail():
            etl.config.load_config(args.config)

            if hasattr(args, "prefix"):
                # Any command where we can select the "prefix" also needs the bucket.
                # TODO(tom): Need to differentiate between object store (schemas) and data lake
                #     (extracted or unloaded data)
                setattr(args, "bucket_name", etl.config.get_config_value("object_store.s3.bucket_name"))
                etl.config.set_config_value("object_store.s3.prefix", args.prefix)
                etl.config.set_config_value("data_lake.s3.prefix", args.prefix)

                # Create name used as prefix for resources, like DynamoDB tables or SNS topics
                base_env = etl.config.get_config_value("resources.VPC.name").replace("dw-vpc-", "dw-etl-", 1)
                etl.config.set_safe_config_value("resource_prefix", f"{base_env}-{args.prefix}")

                if getattr(args, "use_monitor"):
                    etl.monitor.start_monitors(args.prefix)

            # The region must be set for most boto3 calls to succeed.
            os.environ["AWS_DEFAULT_REGION"] = etl.config.get_config_value("resources.VPC.region")

            dw_config = etl.config.get_dw_config()
            if isinstance(getattr(args, "pattern", None), etl.names.TableSelector):
                args.pattern.base_schemas = [schema.name for schema in dw_config.schemas]

            args.func(args)


def submit_step(cluster_id, sub_command):
    """Send the current arthur command to a cluster instead of running it locally."""
    # Don't even bother trying to submit the case of 'arthur.py --submit "$CLUSTER_ID"' where
    # CLUSTER_ID is not set.
    if not cluster_id:
        raise InvalidArgumentError("cluster id in submit may not be empty")
    # We need to remove --submit and --config to avoid an infinite loop and insert a redirect
    # to the config directory.
    partial_parser = build_basic_parser("arthur.py")
    done, remaining = partial_parser.parse_known_args()
    try:
        client = boto3.client("emr")
        response = client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    "Name": "Arthur command: {}".format(str(sub_command).upper()),
                    # For "interactive" steps, allow a sequence of steps to continue after
                    # failure of prior step.
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            etl.config.etl_tmp_dir("venv/bin/arthur.py"),
                            "--config",
                            etl.config.etl_tmp_dir("config"),
                        ]
                        + remaining,
                    },
                }
            ],
        )
        step_id = response["StepIds"][0]
        status = client.describe_step(ClusterId=cluster_id, StepId=step_id)
        json.dump(status["Step"], sys.stdout, indent="    ", sort_keys=True, cls=etl.json_encoder.FancyJsonEncoder)
        sys.stdout.write("\n")
    except Exception as exc:
        logger.exception("Adding step to job flow failed:")
        croak(exc, 1)


class WideHelpFormatter(argparse.RawTextHelpFormatter):
    """Help formatter for argument parser that sets a wider max for help position."""

    # This boldly ignores the message: "Only the name of this class is considered a public API."
    def __init__(self, prog, indent_increment=2, max_help_position=30, width=None) -> None:
        super().__init__(prog, indent_increment, max_help_position, width)


class FancyArgumentParser(argparse.ArgumentParser):
    """
    Fancier version of the argument parser supporting "@file".

    Add feature to read command line arguments from files and support:
        * One argument per line (whitespace is trimmed)
        * Comments or empty lines (either are ignored)
    This enables direct use of output from show_downstream_dependents and
    show_upstream_dependencies.

    To use this feature, add an argument with "@" and have values ready inside of it, one per line:
        cat > tables <<EOF
        www.users
        www.user_comments
        EOF
        arthur.py load @tables
    """

    def __init__(self, **kwargs) -> None:
        formatter_class = kwargs.pop("formatter_class", WideHelpFormatter)
        fromfile_prefix_chars = kwargs.pop("fromfile_prefix_chars", "@")
        super().__init__(formatter_class=formatter_class, fromfile_prefix_chars=fromfile_prefix_chars, **kwargs)

    def convert_arg_line_to_args(self, arg_line: str) -> List[str]:
        """
        Return argument from the current line (when arguments are processed from a file).

        >>> parser = FancyArgumentParser()
        >>> parser.convert_arg_line_to_args("--verbose")
        ['--verbose']
        >>> parser.convert_arg_line_to_args(" schema.table ")
        ['schema.table']
        >>> parser.convert_arg_line_to_args(
        ...     "show_dependents.output_compatible # index=1, kind=CTAS, is_required=true")
        ['show_dependents.output_compatible']
        >>> parser.convert_arg_line_to_args(" # single-line comment")
        []
        >>> parser.convert_arg_line_to_args("--config accidentally_on_one_line")
        Traceback (most recent call last):
        ValueError: unrecognizable argument value in line: --config accidentally_on_one_line
        """
        args = shlex.split(arg_line, comments=True)
        if len(args) > 1:
            raise ValueError("unrecognizable argument value in line: {}".format(arg_line.strip()))
        return args


def isoformat_datetime_string(argument):
    # "isoformat" is used as a verb here.
    return datetime.strptime(argument, "%Y-%m-%dT%H:%M:%S")


def build_basic_parser(prog_name, description=None):
    """
    Build basic parser that knows about the configuration setting.

    The `--config` option is central and can be easily avoided using the environment
    variable so is always here (meaning between 'arthur.py' and the sub-command).

    Similarly, '--submit-to-cluster' is shared for all sub-commands.
    """
    parser = FancyArgumentParser(prog=prog_name, description=description, fromfile_prefix_chars="@")
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG", "./config")

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-c",
        "--config",
        help="add configuration path or file (default: '%s')" % default_config,
        action="append",
        default=[default_config],
    )
    group.add_argument("--submit-to-cluster", help="submit this command to the cluster", dest="cluster_id")

    # Set some defaults (in case no sub-command's add_to_parser is called)
    parser.set_defaults(prolix=None)
    parser.set_defaults(log_level=None)
    parser.set_defaults(func=None)
    parser.set_defaults(use_monitor=False)
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
    parser.add_argument("-V", "--version", action="version", version=f"%(prog)s ({package})")

    # Details for sub-commands lives with sub-classes of sub-commands.
    # Hungry? Get yourself a sub-way.
    subparsers = parser.add_subparsers(
        help="specify one of these sub-commands (which can all provide more help)",
        title="available sub-commands",
        dest="sub_command",
    )
    for klass in [
        # Commands to deal with data warehouse as admin
        InitializeSetupCommand,
        ShowRandomPassword,
        CreateGroupsCommand,
        CreateUserCommand,
        UpdateUserCommand,
        RunSqlCommand,
        # Commands to help with table designs and uploading them
        BootstrapSourcesCommand,
        BootstrapTransformationsCommand,
        ValidateDesignsCommand,
        ExplainQueryCommand,
        RunQueryCommand,
        CheckConstraintsCommand,
        ShowDdlCommand,
        CreateIndexCommand,
        SyncWithS3Command,
        # ETL commands to extract, load (or update), or transform
        ExtractToS3Command,
        LoadDataWarehouseCommand,
        UpgradeDataWarehouseCommand,
        UpdateDataWarehouseCommand,
        UnloadDataToS3Command,
        # Helper commands (database, filesystem)
        CreateSchemasCommand,
        PromoteSchemasCommand,
        PingCommand,
        TerminateSessionsCommand,
        ListFilesCommand,
        ShowDownstreamDependentsCommand,
        ShowUpstreamDependenciesCommand,
        # Environment commands
        RenderTemplateCommand,
        ShowValueCommand,
        ShowVarsCommand,
        ShowPipelinesCommand,
        DeleteFinishedPipelinesCommand,
        QueryEventsCommand,
        SummarizeEventsCommand,
        TailEventsCommand,
        # General and development commands
        ShowHelpCommand,
        SelfTestCommand,
    ]:
        cmd = klass()
        cmd.add_to_parser(subparsers)

    return parser


def add_standard_arguments(parser, options):
    """
    Add from set of "standard" arguments.

    They are "standard" in that the name and description should be the same when used
    by multiple sub-commands.

    :param parser: should be a sub-parser
    :param options: see option strings below, like "prefix", "pattern"
    """
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not modify stuff", default=False, action="store_true")
    if "prefix" in options:
        parser.add_argument(
            "-p",
            "--prefix",
            help="select prefix in S3 bucket (default unless value is set in settings: '%(default)s')",
            default=etl.config.env.get_default_prefix(),
        )
    if "scheme" in options:
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-l",
            "--local-files",
            help="use files available on local filesystem (default)",
            action="store_const",
            const="file",
            dest="scheme",
            default="file",
        )
        group.add_argument(
            "-r", "--remote-files", help="use files in S3", action="store_const", const="s3", dest="scheme"
        )
    if "max-concurrency" in options:
        parser.add_argument(
            "-x",
            "--max-concurrency",
            metavar="N",
            type=int,
            help="set max number of parallel loads to N (overrides 'resources.RedshiftCluster.max_concurrency')",
        )
    if "wlm-query-slots" in options:
        parser.add_argument(
            "-w",
            "--wlm-query-slots",
            metavar="N",
            type=int,
            help="set the number of Redshift WLM query slots used for transformations"
            " (overrides 'resources.RedshiftCluster.wlm_query_slots')",
        )
    if "skip-copy" in options:
        parser.add_argument(
            "-y",
            "--skip-copy",
            help="skip the COPY and INSERT commands (leaves tables empty, used for validation)",
            action="store_true",
        )
    if "continue-from" in options:
        parser.add_argument(
            "--continue-from",
            help="skip forward in execution until the specified relation, then work forward from it"
            " (the special token '*' is allowed to signify continuing from the first relation,"
            " use ':transformations' as the argument to continue from the first transformation,)"
            " otherwise specify an exact relation or source name)",
        )
    if "pattern" in options:
        parser.add_argument(
            "pattern",
            help="glob pattern or identifier to select table(s) or view(s)",
            nargs="*",
            action=StorePatternAsSelector,
        )
    if "one-pattern" in options:
        parser.add_argument(
            "pattern",
            help="glob pattern or identifier to select table or view",
            nargs=1,
            action=StorePatternAsSelector,
        )


class StorePatternAsSelector(argparse.Action):
    """Store the list of glob patterns (to pick tables) as a TableSelector instance."""

    def __call__(self, parser, namespace, values, option_string=None):
        selector = etl.names.TableSelector(values)
        setattr(namespace, self.dest, selector)


class SubCommand:
    """Instances (of child classes) will setup sub-parsers and have callbacks for those."""

    def __init__(self, name: str, help_: str, description: str, aliases: Optional[List[str]] = None) -> None:
        self.name = name
        self.help = help_
        self.description = description
        self.aliases = aliases

    def add_to_parser(self, parent_parser) -> argparse.ArgumentParser:
        if self.aliases is not None:
            parser = parent_parser.add_parser(
                self.name, help=self.help, description=self.description, aliases=self.aliases
            )
        else:
            parser = parent_parser.add_parser(self.name, help=self.help, description=self.description)
        parser.set_defaults(func=self.callback)

        # Log level and prolix setting need to be always known since `run_arg_as_command` depends
        # on them.
        # TODO move this into a parent parser and merge with --submit, --config
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-o", "--prolix", help="send full log to console", default=False, action="store_true")
        group.add_argument(
            "-v", "--verbose", help="increase verbosity", action="store_const", const="DEBUG", dest="log_level"
        )
        group.add_argument(
            "-q", "--quiet", help="decrease verbosity", action="store_const", const="WARNING", dest="log_level"
        )
        # Cannot be set on the command line since changing it is not supported by file sets.
        parser.set_defaults(table_design_dir="./schemas")

        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """Override this method for sub-classes."""
        pass

    @staticmethod
    def location(args, default_scheme=None):
        """
        Decide whether we should focus on local or remote files.

        This returns the appropriate "location" information. This expects args to be a Namespace
        instance from the argument parser with "table_design_dir", "bucket_name", "prefix",
        and hopefully "scheme" as fields.
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
        Most commands need to collect file sets and create relation descriptions around those.

        Commands vary slightly as to what error handling they want to do and whether they need all
        possible descriptions or a selected subset.

        If a "required relation" selector is passed in, we first pick up ALL descriptions (to be
        able to build a dependency tree), build the dependency order, then pick out the matching
        descriptions.

        Set the default_scheme to "s3" to leverage the object store -- avoid relying on having
        current files locally and instead opt for the "publish first, then use S3" pattern.
        """
        if return_all or required_relation_selector is not None:
            selector = etl.names.TableSelector(base_schemas=args.pattern.base_schemas)
        else:
            selector = args.pattern
        file_sets = etl.file_sets.find_file_sets(self.location(args, default_scheme), selector)

        descriptions = etl.relation.RelationDescription.from_file_sets(
            file_sets, required_relation_selector=required_relation_selector
        )

        if not return_all and required_relation_selector is not None:
            descriptions = [
                description for description in descriptions if args.pattern.match(description.target_table_name)
            ]

        return descriptions

    def callback(self, args):
        """Override this method for sub-classes."""
        raise NotImplementedError("Instance of {} has no proper callback".format(self.__class__.__name__))


class MonitoredSubCommand(SubCommand):
    """A sub-command that will also use monitors to update some event table."""

    def add_to_parser(self, parent_parser) -> argparse.ArgumentParser:
        parser = super().add_to_parser(parent_parser)
        parser.set_defaults(use_monitor=True)
        return parser


class InitializeSetupCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "initialize",
            "create ETL database",
            "(Re)create database referenced in ETL credential, optionally creating users and groups."
            " Normally, we expect this to be a validation database (name starts with 'validation')."
            " When bringing up your primary production or development database, use the --force option.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument(
            "-f",
            "--force",
            help="destructively initialize the referenced database regardless"
            " of whether it looks like a validation database",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "-u",
            "--with-user-creation",
            help="create users and groups before (re)creating database",
            default=False,
            action="store_true",
        )

    def callback(self, args):
        with etl.db.log_error():
            etl.data_warehouse.initial_setup(
                with_user_creation=args.with_user_creation, force=args.force, dry_run=args.dry_run
            )


class ShowRandomPassword(SubCommand):
    def __init__(self):
        super().__init__(
            "show_random_password",
            "show a random password compatible with Redshift",
            "Show a random password with upper-case, lower-case and a number.",
        )

    def add_arguments(self, parser):
        parser.set_defaults(log_level="CRITICAL")

    def callback(self, args):
        random_password = uuid.uuid4().hex
        example_password = random_password[:16].upper() + random_password[16:].lower()
        print(example_password)


class CreateGroupsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "create_groups",
            "create all groups from the configuration",
            "Make sure that all groups mentioned in the configuration file actually exist."
            " (This allows to specify a group (as reader or writer) on a schema when that"
            " group does not appear with a user and thus may not have been previously"
            " created using a 'create_user' call.)",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])

    def callback(self, args):
        with etl.db.log_error():
            etl.data_warehouse.create_groups(dry_run=args.dry_run)


class CreateUserCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "create_user",
            "add new user",
            "Add new user and set group membership, optionally create a personal schema."
            " It is ok to re-initialize a user defined in a settings file."
            " Note that you have to set a password for the user in your '~/.pgpass' file"
            " before invoking this command. The password must be valid in Redshift,"
            " so must contain upper-case and lower-case characters as well as numbers.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument("-g", "--group", help="add user to specified group")
        parser.add_argument(
            "-a", "--add-user-schema", help="add new schema, writable for the user", action="store_true"
        )
        parser.add_argument("username", help="name for new user")

    def callback(self, args):
        with etl.db.log_error():
            etl.data_warehouse.create_new_user(
                args.username, group=args.group, add_user_schema=args.add_user_schema, dry_run=args.dry_run
            )


class UpdateUserCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "update_user",
            "update user's group, password, and path",
            "Update an existing user with group membership, password, and search path."
            " Note that you have to set a password for the user in your '~/.pgpass' file"
            " before invoking this command if you want to update the password. The password must"
            " be valid in Redshift, so must contain upper-case and lower-case characters as well"
            " as numbers. If you leave the line out, the password will not be changed.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument("-g", "--group", help="add user to specified group")
        parser.add_argument(
            "-a", "--add-user-schema", help="add new schema, writable for the user", action="store_true"
        )
        parser.add_argument("username", help="name of existing user")

    def callback(self, args):
        with etl.db.log_error():
            etl.data_warehouse.update_user(
                args.username, group=args.group, add_user_schema=args.add_user_schema, dry_run=args.dry_run
            )


class RunSqlCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "run_sql_template",
            "run one of the canned queries",
            "Run a query from the templates, optionally with a target relation,"
            " or list all the available SQL templates.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix"])
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-l", "--list", help="list available templates", action="store_true")
        group.add_argument("template", help="name of SQL template", nargs="?")
        as_user = parser.add_mutually_exclusive_group()
        as_user.add_argument(
            "-a", "--as-admin-user", help="connect as admin user", action="store_true", dest="use_admin"
        )
        as_user.add_argument(
            "-e", "--as-etl-user", help="connect as ETL user (default)", action="store_false", dest="use_admin"
        )
        parser.add_argument(
            action=StorePatternAsSelector,
            dest="schemas",
            help="select schemas for the query (not all templates may use this)",
            nargs="*",
        )

    def callback(self, args):
        if args.list:
            etl.templates.list_sql_templates()
            return

        dw_config = etl.config.get_dw_config()
        try:
            args.schemas.base_schemas = [schema.name for schema in dw_config.schemas]
        except ValueError as exc:
            raise InvalidArgumentError("schemas must be part of configuration") from exc

        dsn = dw_config.dsn_admin_on_etl_db if args.use_admin else dw_config.dsn_etl
        sql_stmt = etl.templates.render_sql(args.template)
        sql_args = {"selected_schemas": tuple(args.schemas.selected_schemas())}
        rows = etl.db.run_statement_with_args(dsn, sql_stmt, sql_args)
        etl.db.print_result(f"Running template: '{args.template}'", rows)


class BootstrapSourcesCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "bootstrap_sources",
            "bootstrap schema information from sources",
            "Download schema information from upstream sources for table designs."
            " If there is no current design file, then create one as a starting point.",
            aliases=["design"],
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-f",
            "--force",
            action="store_true",
            default=False,
            help="overwrite table design file if it already exists",
        )
        group.add_argument(
            "-u",
            "--update",
            action="store_true",
            default=False,
            help="merge new information with existing table design",
        )

    def callback(self, args):
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern, allow_empty=True)
        etl.design.bootstrap.bootstrap_sources(
            args.pattern,
            args.table_design_dir,
            local_files,
            update=args.update,
            replace=args.force,
            dry_run=args.dry_run,
        )


class BootstrapTransformationsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "bootstrap_transformations",
            "bootstrap schema information from transformations",
            "Download schema information as if transformation had been run in data warehouse."
            " If there is no local design file, then create one as a starting point."
            " (With 'check-only', no file is written and only changes in the design are flagged.)",
            aliases=["auto_design"],
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-f",
            "--force",
            action="store_true",
            default=False,
            help="overwrite table design file if it already exists",
        )
        group.add_argument(
            "-u",
            "--update",
            action="store_true",
            default=False,
            help="merge new information with existing table design",
        )
        parser.add_argument(
            "type",
            choices=["CTAS", "VIEW", "update", "check-only"],
            help="pick whether to create table designs for 'CTAS' or 'VIEW' relations"
            " , update the current relation, or check the current designs",
        )
        # Note that patterns must follow the choice of CTAS, VIEW, update etc.
        add_standard_arguments(parser, ["pattern"])

    def callback(self, args):
        if args.update and args.type not in ("CTAS", "VIEW"):
            raise InvalidArgumentError("option '--update' should be used with CTAS or VIEW only")
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
        etl.design.bootstrap.bootstrap_transformations(
            args.table_design_dir,
            local_files,
            args.type if args.type in ("CTAS", "VIEW") else None,
            check_only=args.type == "check-only",
            update=args.update or args.type == "update",
            replace=args.force,
            dry_run=args.dry_run,
        )


class SyncWithS3Command(SubCommand):
    def __init__(self):
        super().__init__(
            "sync",
            "copy table design files to S3",
            "Copy table design files from your local directory to S3."
            " By default, this also copies configuration files"
            " (*.yaml or *.sh files in config directories, excluding credentials*.sh).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        parser.add_argument(
            "-d",
            "--deploy-config",
            action="store_true",
            default=True,
            help="sync local settings files (*.yaml, *.sh) to <prefix>/config folder (default)",
        )
        parser.add_argument(
            "--without-deploy-config",
            action="store_false",
            dest="deploy_config",
            help="do not sync local settings files (*.yaml, *.sh) to <prefix>/config folder",
        )
        parser.add_argument(
            "--delete",
            action="store_true",
            default=False,
            help="delete matching table design and SQL files to make sure target has no extraneous files",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            default=False,
            help="force sync which deletes all matching files, including data",
        )

    def callback(self, args):
        relations = self.find_relation_descriptions(args, default_scheme="file")
        etl.sync.sync_with_s3(
            relations,
            args.config,
            args.bucket_name,
            args.prefix,
            deploy_config=args.deploy_config,
            delete_schemas_pattern=args.pattern if args.delete or args.force else None,
            delete_data_pattern=args.pattern if args.force else None,
            dry_run=args.dry_run,
        )


class ExtractToS3Command(MonitoredSubCommand):
    def __init__(self):
        super().__init__(
            "extract",
            "extract data from upstream sources",
            "Extract table contents from upstream databases (unless you decide to use existing"
            " data files) and then gather references to data files in S3 into manifests file."
            " (This last step is the only step needed for static sources where data is created"
            " outside the ETL.)",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--with-sqoop",
            help="extract data using Sqoop (using 'sqoop import', this is the default)",
            const="sqoop",
            action="store_const",
            dest="extractor",
            default="sqoop",
        )
        group.add_argument(
            "--with-spark",
            help="extract data using Spark Dataframe (using submit_arthur.sh)",
            const="spark",
            action="store_const",
            dest="extractor",
        )
        group.add_argument(
            "--use-existing-csv-files",
            help="skip extraction and go straight to creating manifest files, implied default for static sources",
            const="manifest-only",
            action="store_const",
            dest="extractor",
        )
        parser.add_argument(
            "-k",
            "--keep-going",
            help="extract as much data as possible, ignoring errors along the way",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "-m",
            "--max-partitions",
            metavar="N",
            type=int,
            help="set max number of partitions to write to N (overrides 'resources.EMR.max_partitions')",
        )
        parser.add_argument(
            "--use-sampling",
            help="use only 10%% of rows in extracted tables that are larger than 100MB",
            default=False,
            action="store_true",
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        max_partitions = args.max_partitions or etl.config.get_config_int("resources.EMR.max_partitions")
        if max_partitions < 1:
            raise InvalidArgumentError("option for max partitions must be >= 1")
        if args.extractor not in ("sqoop", "spark", "manifest-only"):
            raise ETLSystemError("bad extractor value: {}".format(args.extractor))

        # Make sure that there is a Spark environment. If not, re-launch with spark-submit.
        # (Without this step, the Spark context is unknown and we won't be able to create a
        # SQL context.)
        if args.extractor == "spark" and "SPARK_ENV_LOADED" not in os.environ:
            # Try the full path (in the EMR cluster), or try without path and hope for the best.
            submit_arthur = etl.config.etl_tmp_dir("venv/bin/submit_arthur.sh")
            if not os.path.exists(submit_arthur):
                submit_arthur = "submit_arthur.sh"
            logger.info("Restarting to submit to cluster (using '%s')", submit_arthur)
            print("+ exec {} {}".format(submit_arthur, " ".join(sys.argv)), file=sys.stderr)
            os.execvp(submit_arthur, (submit_arthur,) + tuple(sys.argv))
            sys.exit(1)

        descriptions = self.find_relation_descriptions(
            args, default_scheme="s3", required_relation_selector=dw_config.required_in_full_load_selector
        )
        etl.monitor.Monitor.marker_payload("extract").emit(dry_run=args.dry_run)
        etl.extract.extract_upstream_sources(
            args.extractor,
            dw_config.schemas,
            descriptions,
            max_partitions=max_partitions,
            use_sampling=args.use_sampling,
            keep_going=args.keep_going,
            dry_run=args.dry_run,
        )


class LoadDataWarehouseCommand(MonitoredSubCommand):
    def __init__(self):
        super().__init__(
            "load",
            "load data into source tables and forcefully update all dependencies",
            "Load data into the data warehouse from files in S3, which will *rebuild* the data warehouse."
            " This will operate on entire schemas at once, which will be backed up as necessary."
            " It is an error to try to select tables unless they are all the tables in the schema.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(
            parser, ["pattern", "prefix", "max-concurrency", "wlm-query-slots", "skip-copy", "dry-run"]
        )
        parser.add_argument(
            "--concurrent-extract",
            help="watch DynamoDB for extract step completion and load source tables as extracts finish"
            " assuming another Arthur in this prefix is running extract (default: %(default)s)",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "--without-staging-schemas",
            help="do NOT do all the work in hidden schemas and publish to standard names on completion"
            " (default: use staging schemas)",
            default=True,
            action="store_false",
            dest="use_staging_schemas",
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        try:
            args.pattern.selected_schemas()
        except ValueError as exc:
            raise InvalidArgumentError(exc) from exc

        relations = self.find_relation_descriptions(
            args,
            default_scheme="s3",
            required_relation_selector=dw_config.required_in_full_load_selector,
            return_all=True,
        )
        etl.monitor.Monitor.marker_payload("load").emit(dry_run=args.dry_run)
        max_concurrency = args.max_concurrency or etl.config.get_config_int(
            "resources.RedshiftCluster.max_concurrency", 1
        )
        wlm_query_slots = args.wlm_query_slots or etl.config.get_config_int(
            "resources.RedshiftCluster.wlm_query_slots", 1
        )
        etl.load.load_data_warehouse(
            relations,
            args.pattern,
            max_concurrency=max_concurrency,
            wlm_query_slots=wlm_query_slots,
            concurrent_extract=args.concurrent_extract,
            skip_copy=args.skip_copy,
            use_staging=args.use_staging_schemas,
            dry_run=args.dry_run,
        )


class UpgradeDataWarehouseCommand(MonitoredSubCommand):
    def __init__(self):
        super().__init__(
            "upgrade",
            "load data into source or CTAS tables and create dependent VIEWS along the way",
            "Delete selected tables and views, then rebuild them along with all of relations"
            " that depend on the selected ones. This is for debugging since the rebuild is"
            " visible to users (i.e. outside a transaction).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(
            parser, ["pattern", "prefix", "max-concurrency", "wlm-query-slots", "continue-from", "skip-copy", "dry-run"]
        )
        parser.add_argument(
            "--only-selected",
            action="store_true",
            default=False,
            help="skip rebuilding relations that depend on the selected ones"
            " (leaves warehouse in inconsistent state, for debugging only)",
        )
        parser.add_argument(
            "--include-immediate-views",
            action="store_true",
            help="include views that are downstream of selected relations without any CTAS before"
            " (this is the default and only useful with '--only-selected', for debugging only)",
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--with-staging-schemas",
            action="store_true",
            default=False,
            dest="use_staging_schemas",
            help="do all the work using hidden schemas (default: do not use staging schemas,"
            " note this is the opposite of 'load' command)",
        )
        group.add_argument(
            "--into-schema",
            dest="target_schema",
            help="build relations in this target schema (selected relations must not depend on each other)",
        )

    def callback(self, args):
        if args.target_schema and len(args.pattern) == 0:
            raise InvalidArgumentError("option '--into-schema' requires that relations are selected")
        if args.include_immediate_views and not args.only_selected:
            logger.warning("Option '--include-immediate-views' is default unless '--only-selected' is used")
        if args.target_schema and not args.only_selected:
            logger.warning("Option '--into-schema' implies '--only-selected'")
            args.only_selected = True
        dw_config = etl.config.get_dw_config()
        relations = self.find_relation_descriptions(
            args,
            default_scheme="s3",
            required_relation_selector=dw_config.required_in_full_load_selector,
            return_all=True,
        )
        etl.monitor.Monitor.marker_payload("upgrade").emit(dry_run=args.dry_run)
        max_concurrency = args.max_concurrency or etl.config.get_config_int(
            "resources.RedshiftCluster.max_concurrency", 1
        )
        wlm_query_slots = args.wlm_query_slots or etl.config.get_config_int(
            "resources.RedshiftCluster.wlm_query_slots", 1
        )
        etl.load.upgrade_data_warehouse(
            relations,
            args.pattern,
            max_concurrency=max_concurrency,
            wlm_query_slots=wlm_query_slots,
            only_selected=args.only_selected,
            include_immediate_views=args.include_immediate_views,
            continue_from=args.continue_from,
            use_staging=args.use_staging_schemas,
            target_schema=args.target_schema,
            skip_copy=args.skip_copy,
            dry_run=args.dry_run,
        )


class UpdateDataWarehouseCommand(MonitoredSubCommand):
    def __init__(self):
        super().__init__(
            "update",
            "update data in the data warehouse from files in S3",
            "Load data into data warehouse from files in S3 and then update all dependent CTAS relations"
            " (within a transaction).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "wlm-query-slots", "dry-run"])
        parser.add_argument(
            "--only-selected",
            help="only load data into selected relations"
            " (leaves warehouse in inconsistent state, for debugging only, default: %(default)s)",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "--scheduled-start-time",
            metavar="TIME",
            default=None,
            type=isoformat_datetime_string,
            help="require recent successful extract events for all selected source relations "
            "after UTC time TIME (or, by default, don't require extract events)",
        )
        parser.add_argument(
            "--vacuum",
            help="run vacuum after the update to tidy up the place (default: %(default)s)",
            default=False,
            action="store_true",
        )

    def callback(self, args):
        relations = self.find_relation_descriptions(args, default_scheme="s3", return_all=True)
        etl.monitor.Monitor.marker_payload("update").emit(dry_run=args.dry_run)
        wlm_query_slots = args.wlm_query_slots or etl.config.get_config_int(
            "resources.RedshiftCluster.wlm_query_slots", 1
        )
        etl.load.update_data_warehouse(
            relations,
            args.pattern,
            wlm_query_slots=wlm_query_slots,
            only_selected=args.only_selected,
            run_vacuum=args.vacuum,
            start_time=args.scheduled_start_time,
            dry_run=args.dry_run,
        )


class UnloadDataToS3Command(MonitoredSubCommand):
    def __init__(self):
        super().__init__(
            "unload",
            "unload data from data warehouse to files in S3",
            "Unload data from data warehouse into CSV files in S3 (along with files of column names).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "dry-run"])
        parser.add_argument(
            "-f",
            "--force",
            help="enable ALLOWOVERWRITE option to replace existing data files in S3",
            action="store_true",
        )
        parser.add_argument(
            "-k",
            "--keep-going",
            help="unload as much data as possible, ignoring errors along the way",
            default=False,
            action="store_true",
        )

    def callback(self, args):
        descriptions = self.find_relation_descriptions(args, default_scheme="s3")
        etl.monitor.Monitor.marker_payload("unload").emit(dry_run=args.dry_run)
        etl.unload.unload_to_s3(
            descriptions, allow_overwrite=args.force, keep_going=args.keep_going, dry_run=args.dry_run
        )


class CreateSchemasCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "create_schemas",
            "create schemas from data warehouse config",
            "Create schemas as configured and set permissions."
            " Optionally move existing schemas to backup or create in staging position."
            " (Any patterns must be schema names.)",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-b", "--with-backup", help="backup any existing schemas", default=False, action="store_true"
        )
        group.add_argument(
            "--with-staging", help="create schemas in staging position", default=False, action="store_true"
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        schema_names = args.pattern.selected_schemas()
        schemas = [schema for schema in dw_config.schemas if schema.name in schema_names]
        with etl.db.log_error():
            if args.with_backup:
                etl.data_warehouse.backup_schemas(schemas, dry_run=args.dry_run)
            etl.data_warehouse.create_schemas(schemas, use_staging=args.with_staging, dry_run=args.dry_run)


class PromoteSchemasCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "promote_schemas",
            "move staging or backup schemas into standard position",
            "Move hidden schemas (staging or backup) to standard position (schema names and permissions)."
            " When promoting from staging, current standard position schemas are backed up first."
            " Promoting (ie, restoring) a backup should only happen after a load finished successfully"
            " but left bad data behind.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "dry-run"])
        parser.add_argument(
            "--from-position",
            help="which hidden schema should be promoted",
            choices=["staging", "backup"],
            required=True,
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        schema_names = args.pattern.selected_schemas()
        schemas = [schema for schema in dw_config.schemas if schema.name in schema_names]
        with etl.db.log_error():
            if args.from_position == "staging":
                etl.data_warehouse.publish_schemas(schemas, dry_run=args.dry_run)
            elif args.from_position == "backup":
                etl.data_warehouse.restore_schemas(schemas, dry_run=args.dry_run)


class ValidateDesignsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "validate",
            "validate table design files",
            "Validate table designs by checking their syntax and compatibility with the database"
            " (use '-nskq' to see only errors or warnings, without connecting to a database).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument(
            "-k",
            "--keep-going",
            help="ignore errors and test as many files as possible",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "-s",
            "--skip-sources-check",
            help="skip check of designs against upstream databases",
            default=False,
            action="store_true",
        )
        parser.add_argument(
            "-n",
            "--skip-dependencies-check",
            help="skip check of dependencies in designs against data warehouse",
            default=False,
            action="store_true",
        )

    def callback(self, args):
        # This does not pick up all designs to speed things up but that may lead to false positives.
        descriptions = self.find_relation_descriptions(args)
        etl.validate.validate_designs(
            descriptions,
            keep_going=args.keep_going,
            skip_sources=args.skip_sources_check,
            skip_dependencies=args.skip_dependencies_check,
        )


class RunQueryCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "run_query",
            "run transformation and print result",
            "Run the query for a transformation and show results in a pretty table.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["one-pattern", "prefix", "scheme"])
        parser.add_argument(
            "-n",
            "--limit",
            help="limit the number of rows returned by the query",
            type=int,
        )
        parser.add_argument(
            "--with-staging-schemas",
            action="store_true",
            default=False,
            help="use the relations in staging schemas for the query",
        )

    def callback(self, args):
        relations = self.find_relation_descriptions(args)
        transformations = [relation for relation in relations if relation.is_transformation]
        if len(transformations) != 1:
            raise InvalidArgumentError("selected %d transformations" % len(transformations))
        with etl.db.log_error():
            etl.load.run_query(transformations[0], args.limit, args.with_staging_schemas)


class CheckConstraintsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "check_constraints",
            "check constraints of selected relations",
            "Run all the table constraints for the selected relations",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument(
            "--with-staging-schemas",
            action="store_true",
            default=False,
            help="use the relations in staging schemas for the query",
        )

    def callback(self, args):
        relations = self.find_relation_descriptions(args)
        with etl.db.log_error():
            etl.load.check_constraints(relations, args.with_staging_schemas)


class ExplainQueryCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "explain",
            "collect explain plans",
            "Run EXPLAIN on queries (for CTAS or VIEW), check query plan for distributions.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        if args.scheme == "file":
            # When running locally, we accept that there be only a SQL file.
            local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
            descriptions = [
                etl.relation.RelationDescription(file_set) for file_set in local_files if file_set.sql_file_name
            ]
        else:
            # When running with S3, we expect full sets of files (SQL plus table design)
            descriptions = self.find_relation_descriptions(args)
        with etl.db.log_error():
            etl.explain.explain_queries(dw_config.dsn_etl, descriptions)


class ShowDdlCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_ddl",
            "show the DDL that will be used to create the table",
            "Show DDL corresponding to the selected relations based on their (local) table designs.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern"])

    def callback(self, args):
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
        descriptions = [
            etl.relation.RelationDescription(file_set) for file_set in local_files if file_set.design_file_name
        ]
        etl.dialect.show_ddl(descriptions)


class CreateIndexCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "create_index",
            "create Markdown-formatted index of schemas and tables",
            "Create an index that lists by schema the tables available."
            " It is possible to filter by the groups that have access.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern"])
        parser.add_argument("--group", action="append", default=[], help="filter by reader group (repeat as needed)")
        parser.add_argument("--with-columns", action="store_true", help="add detailed tables with column information")

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        local_files = etl.file_sets.find_file_sets(self.location(args, "file"), args.pattern)
        descriptions = [
            etl.relation.RelationDescription(file_set) for file_set in local_files if file_set.design_file_name
        ]
        unknown = frozenset(args.group).difference(dw_config.groups)
        if unknown:
            raise InvalidArgumentError(f"unknown group(s): {join_with_single_quotes(unknown)}")
        selected_groups = args.group or dw_config.groups  # Nothing is everything.
        etl.relation.create_index(descriptions, selected_groups, args.with_columns)


class ListFilesCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "ls",
            "list files in local directory or in S3",
            "List files in local directory or in the S3 bucket and starting with prefix by"
            " source, table, and file type."
            " (If sorting by timestamp, only print filename and timestamp.)",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument(
            "-a", "--long-format", help="add file size and timestamp of last modification", action="store_true"
        )
        parser.add_argument(
            "-t", "--sort-by-time", help="sort files by timestamp (and list in single column)", action="store_true"
        )

    def callback(self, args):
        file_sets = etl.file_sets.find_file_sets(self.location(args), args.pattern)
        etl.file_sets.list_files(file_sets, long_format=args.long_format, sort_by_time=args.sort_by_time)


class PingCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "ping",
            "ping data warehouse or upstream database",
            "Try to connect to the data warehouse or upstream databases to test connection settings.",
        )

    def add_arguments(self, parser):
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "-a", "--as-admin-user", help="try to connect as admin user", action="store_true", dest="use_admin"
        )
        group.add_argument(
            "-e", "--as-etl-user", help="try to connect as ETL user (default)", action="store_false", dest="use_admin"
        )
        group.add_argument(
            "--for-schema",
            help="ping upstream database (instead of data warehouse) based on the target schema",
            action=StorePatternAsSelector,
            nargs="+",
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        if args.for_schema is None:
            dsns = [dw_config.dsn_admin if args.use_admin else dw_config.dsn_etl]
        else:
            try:
                args.for_schema.base_schemas = [
                    schema.name for schema in dw_config.schemas if schema.is_database_source
                ]
            except ValueError as exc:
                raise InvalidArgumentError("selected schema is not for upstream database") from exc
            try:
                selected = args.for_schema.selected_schemas()
            except ValueError as exc:
                raise InvalidArgumentError("pattern must match schemas") from exc
            logger.info("Selected upstream sources based on schema(s): %s", join_with_single_quotes(selected))
            dsns = [schema.dsn for schema in dw_config.schemas if schema.name in selected]
        with etl.db.log_error():
            for dsn in dsns:
                etl.db.ping(dsn)


class TerminateSessionsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "terminate_sessions",
            "terminate sessions holding table locks",
            "Terminate sessions that hold table locks and might interfere with the ETL. "
            "This is always run as the admin user.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])

    def callback(self, args):
        with etl.db.log_error():
            etl.data_warehouse.terminate_sessions(dry_run=args.dry_run)


class ShowDownstreamDependentsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_downstream_dependents",
            "show dependent relations",
            "Show relations that follow in execution order and are selected or depend on them.",
            aliases=["show_dependents"],
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme", "continue-from"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--list-dependencies",
            help="deprecated in favor of --with-dependencies",
            action="store_true",
            dest="with_dependencies",
        )
        group.add_argument(
            "--with-dependencies", help="show list of dependencies (downstream) for every relation", action="store_true"
        )
        group.add_argument(
            "--with-dependents", help="show list of dependents (upstream) for every relation", action="store_true"
        )

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        relations = self.find_relation_descriptions(
            args, required_relation_selector=dw_config.required_in_full_load_selector, return_all=True
        )
        etl.load.show_downstream_dependents(
            relations,
            args.pattern,
            continue_from=args.continue_from,
            with_dependencies=args.with_dependencies,
            with_dependents=args.with_dependents,
        )


class ShowUpstreamDependenciesCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_upstream_dependencies",
            "show relations that feed the selected relations (including themselves)",
            "Follow dependencies upstream to their sources to chain all relations"
            " that the selected ones depend on (with the selected ones).",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])

    def callback(self, args):
        dw_config = etl.config.get_dw_config()
        relations = self.find_relation_descriptions(
            args, required_relation_selector=dw_config.required_in_full_load_selector, return_all=True
        )
        etl.load.show_upstream_dependencies(relations, args.pattern)


class RenderTemplateCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "render_template",
            "render selected template by filling in configuration settings",
            "Print template after replacing placeholders (like '${resources.VPC.region}') with values"
            " from the settings files",
        )

    def add_arguments(self, parser):
        parser.set_defaults(log_level="CRITICAL")
        add_standard_arguments(parser, ["prefix"])
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument("-l", "--list", help="list available templates", action="store_true")
        group.add_argument("template", help="name of template", nargs="?")
        parser.add_argument("-t", "--compact", help="produce compact output", action="store_true")

    def callback(self, args):
        if args.list:
            etl.templates.list_templates(compact=args.compact)
            return

        etl.templates.render(args.template, compact=args.compact)


class ShowValueCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_value", "show variable setting", "Print value of variable based on the configuration files."
        )

    def add_arguments(self, parser):
        parser.set_defaults(log_level="CRITICAL")
        add_standard_arguments(parser, ["prefix"])
        parser.add_argument("name", help="print the value for the chosen setting")
        parser.add_argument("default", nargs="?", help="set default in case the setting is unset")

    def callback(self, args):
        etl.config.settings.show_value(args.name, args.default)


class ShowVarsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_vars",
            "show variables available for template files",
            "Print list of variables and their values based on the configuration files."
            " These variables can be used with ${name} substitutions in templates.",
            aliases=["settings"],
        )

    def add_arguments(self, parser):
        parser.set_defaults(log_level="CRITICAL")
        add_standard_arguments(parser, ["prefix"])
        parser.add_argument("name", help="print just the value for the chosen setting", nargs="*")

    def callback(self, args):
        etl.config.settings.show_vars(args.name)


class ShowPipelinesCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "show_pipelines",
            "show installed pipelines",
            "Show information about currently installed pipelines."
            " If you select a single pipeline, more details are revealed.",
        )

    def add_arguments(self, parser):
        parser.add_argument("-j", "--as-json", help="write output in JSON format", action="store_true", default=False)
        parser.add_argument("selection", help="pick pipelines using a glob pattern", nargs="*")

    def callback(self, args):
        etl.pipeline.show_pipelines(args.selection, as_json=args.as_json)


class DeleteFinishedPipelinesCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "delete_finished_pipelines",
            "delete pipelines that finished yesterday or before",
            "Delete pipelines if they are finished and they finished more than 24 hours ago.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run"])
        parser.add_argument("selection", help="pick specific pipelines", nargs="*")

    def callback(self, args):
        etl.pipeline.delete_finished_pipelines(args.selection, dry_run=args.dry_run)


class QueryEventsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "query_events",
            "query the tables of ETL events",
            "Query the table of events written during an ETL."
            " When an ETL is specified, then it is used as a filter."
            " Otherwise ETLs from the last 48 hours are listed.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix"])
        parser.add_argument(
            "--column",
            action="append",
            choices=["step", "event", "elapsed", "rowcount"],
            help="select output column (in addition to target and timestamp),"
            " use multiple times so add more columns",
        )
        parser.add_argument("etl_id", help="pick particular ETL from the past", nargs="?")

    def callback(self, args):
        # TODO(tom): This is starting to become awkward: make finding latest ETL a separate command.
        if args.etl_id is None:
            # Going back two days should cover at least one complete and one running rebuild ETL.
            etl.monitor.query_for_etl_ids(days_ago=2)
        else:
            etl.monitor.scan_etl_events(args.etl_id, args.column)


class SummarizeEventsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "summarize_events",
            "summarize events from the latest ETL (for given step)",
            "For selected (or all) relations, show events from ETL, " "grouped by schema.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix", "scheme"])
        parser.add_argument(
            "-s",
            "--step",
            choices=["extract", "load", "upgrade", "update", "unload"],
            help="pick which step to summarize",
        )

    def callback(self, args):
        relations = self.find_relation_descriptions(args)
        etl.monitor.summarize_events(relations, args.step)


class TailEventsCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "tail_events",
            "show tail of the ETL events and optionally follow for changes",
            "Show latest ETL events for the selected tables in a 15-minute window or"
            " since the given start time. (Use '-t #{@latestRunTime}' in a Data Pipeline definition.)"
            " Optionally keep looking for events in 30s intervals,"
            " which automatically quits when no new event arrives within an hour.",
        )

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["pattern", "prefix"])
        parser.add_argument(
            "-s", "--step", choices=["extract", "load", "upgrade", "update", "unload"], help="pick which step to tail"
        )
        now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).isoformat()
        parser.add_argument(
            "-t", "--start-time", help="beginning of time window, e.g. '%s'" % now, type=isoformat_datetime_string
        )
        parser.add_argument("-f", "--follow", help="keep checking for events", default=False, action="store_true")

    def callback(self, args):
        start_time = args.start_time or (datetime.utcnow() - timedelta(seconds=15 * 60))
        if args.follow:
            update_interval = 30
            idle_time_out = 60 * 60
        else:
            update_interval = idle_time_out = None

        # This will sort events by 30s time buckets and execution order within those buckets.
        # (If events for all tables already happen to exist, then this matches the desired
        # execution order.)
        all_relations = self.find_relation_descriptions(args, default_scheme="s3", return_all=True)
        selected_relations = etl.relation.select_in_execution_order(all_relations, args.pattern)
        if not selected_relations:
            return
        etl.monitor.tail_events(
            selected_relations,
            start_time=start_time,
            update_interval=update_interval,
            idle_time_out=idle_time_out,
            step=args.step,
        )


class ShowHelpCommand(SubCommand):
    def __init__(self):
        super().__init__("help", "show help by topic", "Show helpful information around selected topic.")
        self.topics = ["extract", "load", "pipeline", "sync", "unload", "validate"]

    def add_arguments(self, parser):
        parser.set_defaults(log_level="CRITICAL")
        parser.add_argument("topic", help="select topic", choices=self.topics)

    def callback(self, args):
        print(sys.modules["etl." + args.topic].__doc__.strip() + "\n")


class SelfTestCommand(SubCommand):
    def __init__(self):
        super().__init__("selftest", "run code tests of ETL", "Run self test of the ETL.")

    def add_arguments(self, parser):
        # For self-tests, dial logging back to (almost) nothing so that logging in console
        # doesn't mix with test output.
        parser.set_defaults(log_level="CRITICAL")

    def callback(self, args):
        etl.selftest.run_doctest("etl", args.log_level)


if __name__ == "__main__":
    run_arg_as_command()
