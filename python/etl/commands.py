"""
Driver for the ETL based on sub-commands and central place to govern command line args.

This can be the entry point for a console script.  Some functions are broken out
so that they can be leveraged by utilities in addition to the top-level script.
"""

import argparse
from datetime import datetime
import getpass
import os
import uuid
import sys

import pkg_resources

import etl
import etl.config
import etl.copy
import etl.dump
import etl.dw
import etl.load
import etl.pg
import etl.s3
import etl.update


def run_arg_as_command(my_name="arthur"):
    try:
        args = build_full_parser(my_name).parse_args()
        etl.config.configure_logging(args.log_level)
        settings = etl.config.load_settings(args.config)
        if args.func:
            with etl.measure_elapsed_time():
                args.func(args, settings)
        #else:
        # parser.print_usage()
    except:
        # Any log traces have already been printed by the context manager, so just bail out.
        sys.exit(1)


def build_full_parser(prog_name):
    """
    Build a parser by adding sub-parsers for sub-commands.
    Other options, even if shared between sub-commands, are in the sub-parsers to avoid
    having to awkwardly insert them betwween program name and sub-command name.

    :param prog_name: Name that should show up as command name in help
    :return: instance of ArgumentParser that is ready to parse and run sub-commands
    """
    parser = build_basic_parser(prog_name, description="This command allows to drive the Redshift ETL.")

    package = "redshift-etl v{}".format(pkg_resources.get_distribution("redshift-etl").version)
    parser.add_argument("-V", "--version", action="version", version="%(prog)s ({})".format(package))

    # Details for sub-commands lives with sub-classes of sub-commands. Hungry? Get yourself a sub-way.
    subparsers = parser.add_subparsers(help="specify one of these sub-commands (which can all provide more help)",
                                       title="available sub-commands (full names with aliases)")
    list_files = ListFilesCommand()
    list_files.add_to_parser(subparsers)

    initial_setup = InitialSetupCommand()
    initial_setup.add_to_parser(subparsers)

    create_user = CreateUserCommand()
    create_user.add_to_parser(subparsers)

    dump_schemas_to_s3 = DumpSchemasToS3Command()
    dump_schemas_to_s3.add_to_parser(subparsers)

    dump_to_s3 = DumpToS3Command()
    dump_to_s3.add_to_parser(subparsers)

    copy_to_s3 = CopyToS3Command()
    copy_to_s3.add_to_parser(subparsers)

    load_to_redshift = LoadToRedshiftCommand()
    load_to_redshift.add_to_parser(subparsers)

    update_in_redshift = UpdateInRedshiftCommand()
    update_in_redshift.add_to_parser(subparsers)

    return parser


def build_basic_parser(prog_name, description):
    """
    Build basic parser that knows about the configuration setting.

    The `--config` option is central and can be easily avoided using the env. var. so is always here.
    """
    parser = argparse.ArgumentParser(prog=prog_name, description=description)

    # Show different help message depending on whether user has already set the environment variable.
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG")
    if default_config is None:
        parser.add_argument("-c", "--config",
                            help="set path to configuration file (required if DATA_WAREHOUSE_CONFIG is not set)",
                            required=True)
    else:
        parser.add_argument("-c", "--config",
                            help="change path to configuration file (using DATA_WAREHOUSE_CONFIG=%(default)s)",
                            default=default_config)

    # Set defaults so that we can simplify tests.
    parser.set_defaults(log_level=None)
    parser.set_defaults(func=None)

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
    group.add_argument("-v", "--verbose", help="increase verbosity",
                       action="store_const", const="DEBUG", dest="log_level")
    group.add_argument("-s", "--silent", help="decrease verbosity",
                       action="store_const", const="WARNING", dest="log_level")

    parser.add_argument("-n", "--dry-run", help="do not actually copy data", default=False, action="store_true")

    example_password = uuid.uuid4().hex.title()

    if "prefix" in options:
        prefix = parser.add_mutually_exclusive_group()
        prefix.add_argument("-p", "--prefix", default=getpass.getuser(),
                            help="select prefix in S3 bucket (default is user name: '%(default)s')")
        prefix.add_argument("-e", "--prefix-env", dest="prefix", metavar="ENV", action=AppendDateAction,
                            help="set prefix in S3 bucket to '<ENV>/<CURRENT_DATE>'")
    if "table-design-dir" in options:
        parser.add_argument("-t", "--table-design-dir",
                            help="set path to directory with table design files (default: '%(default)s')",
                            default="./schemas")
    if "drop" in options:
        parser.add_argument("-d", "--drop",
                            help="DROP TABLE or VIEW TO FORCE UPDATE OF definition", default=False,
                            action="store_true")
    elif "drop-table" in options:
        parser.add_argument("-d", "--drop-table",
                            help="DROP TABLE to FORCE UPDATE OF TABLE definition", default=False,
                            action="store_true")
    # XXX Still needed?
    if "force" in options:
        parser.add_argument("-f", "--force", help="allow overwriting data files", default=False,
                            action="store_true")

    if "table" in options:
        parser.add_argument("table", help="glob pattern or identifier to select target table(s)", nargs='*')
    elif "ctas_or_view" in options:
        parser.add_argument("ctas_or_view", help="glob pattern or identifier to select target(s)", nargs='*')
    if "username" in options:
        parser.add_argument("username", help="name for new user")
    if "password" in options:
        parser.add_argument("password", help="password for new user (example: '%s')" % example_password, nargs='?')

    return parser


def check_positive_int(s):
    """
    Helper method for argument parser to make sure optional arg with value 's'
    is a positive integer (meaning, s > 0)
    """
    try:
        i = int(s)
        if i <= 0:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("%s is not a positive int" % s)
    return i


class SubCommand:

    """Instances (of child classes) will setup sub-parsers and have callbacks for those."""

    def __init__(self, name, aliases, help, description):
        self.name = name
        self.aliases = aliases
        self.help = help
        self.description = description

    def add_to_parser(self, parent_parser):
        parser = parent_parser.add_parser(self.name,
                                          aliases=self.aliases,
                                          help=self.help,
                                          description=self.description)
        parser.set_defaults(func=self.callback)
        self.add_arguments(parser)
        return parser

    def add_arguments(self, parser):
        """Overwrite this method for sub-classes"""
        pass

    def callback(self, args, settings):
        raise NotImplementedError("Instance of %s has no proper callback" % self.__class__.__name__)


class ListFilesCommand(SubCommand):

    def __init__(self):
        super().__init__("list", ["ls"],
                         "list files",
                         "list files starting with prefix in the S3 bucket by source, table, type")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "table"])

    def callback(self, args, settings):
        etl.s3.list_files(args, settings)


class InitialSetupCommand(SubCommand):

    def __init__(self):
        super().__init__("initial_setup", ["init"],
                         "create schemas, users, groups",
                         "create schemas for each source, users, and user groups for etl and analytics")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["config", "password"])
        parser.add_argument("-k", "--skip-user-creation", help="Skip user and groups, only create schemas",
                            default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.initial_setup(args, settings)


class CreateUserCommand(SubCommand):

    def __init__(self):
        super().__init__("create_user", [],
                         "add new user",
                         "add new user and set group membership")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["config", "username", "password"])
        parser.add_argument("-e", "--etl-user", help="Add user also to ETL group", action="store_true")
        parser.add_argument("-a", "--add-user-schema",
                            help="Add new schema, writable for the user",
                            action="store_true")
        parser.add_argument("-k", "--skip-user-creation",
                            help="Skip new user; only change search path of existing user",
                            default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.create_user(args, settings)


class DumpSchemasToS3Command(SubCommand):

    def __init__(self):
        super().__init__("dump_schemas_to_s3", ["schemas"],
                         "extract schema information from sources",
                         "extract schema information from sources")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "dry-run", "table-design-dir", "table"])
        parser.add_argument("-j", "--jobs", help="Number of parallel connections (default: %(default)s)",
                            type=check_positive_int, default=1)

    def callback(self, args, settings):
        etl.dump.dump_schemas_to_s3(args, settings)


class DumpToS3Command(SubCommand):

    def __init__(self):
        super().__init__("dump_data_to_s3", ["dump"],
                         "dump table data from sources",
                         "dump table contents to files in S3")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "dry-run", "table"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dump.dump_to_s3(args, settings)


class CopyToS3Command(SubCommand):

    def __init__(self):
        super().__init__("copy_data_to_s3", ["copy"],
                         "copy table design files",
                         "copy local table design files to S3")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "dry-run", "table-design-dir", "data-dir", "table"])
        group = parser.add_mutually_exclusive_group()
        group.add_argument("-w", "--with-data", help="Copy data files (including manifest)", action="store_true")
        group.add_argument("-g", "--git-modified", help="Copy files modified in work tree", action="store_true")

    def callback(self, args, settings):
        etl.copy.copy_to_s3(args, settings)


class LoadToRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("load_to_redshift", ["load"],
                         "load data into Redshift from files in S3",
                         "load data into Redshift from files in S3")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "dry-run", "drop-table", "table"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.load.load_to_redshift(args, settings)


class UpdateInRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("update_in_redshift", ["update"],
                         "update CTAS and views in Redshift",
                         "update CTAS and views in Redshift from files in S3")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "prefix_env", "dry-run", "drop", "ctas_or_view"])
        parser.add_argument("-x", "--add-explain-plan", help="Add explain plan to log", action="store_true")
        parser.add_argument("-a", "--skip-views", help="Skip updating views, only reload CTAS", action="store_true")
        parser.add_argument("-k", "--skip-ctas", help="Skip updating CTAS, only reload views", action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.update.update_ctas_or_views(args, settings)


class AppendDateAction(argparse.Action):

    """Callback for argument parser to append current date so that environment has one folder per day."""

    def __call__(self, parser, namespace, values, option_string=None):
        today = datetime.now().strftime("/%Y%m%d_%H%M")
        setattr(namespace, self.dest, values + today)


if __name__ == "__main__":
    run_arg_as_command()
