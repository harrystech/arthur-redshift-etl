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
import etl.dump
import etl.dw
import etl.load
import etl.pg
import etl.s3
import etl.schemas


def run_arg_as_command(my_name="arthur"):
    try:
        parser = build_full_parser(my_name)
        args = parser.parse_args()
        if args.func:
            etl.config.configure_logging(args.log_level)
            settings = etl.config.load_settings(args.config)
            with etl.measure_elapsed_time():
                args.func(args, settings)
        else:
            parser.print_usage()
    except:
        # Any log traces have already been printed by the context manager, so just bail out.
        sys.exit(1)


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
    subparsers = parser.add_subparsers(help="Specify one of these sub-commands (which can all provide more help)",
                                       title="available sub-commands")

    for klass in [InitialSetupCommand, CreateUserCommand,
                  DownloadSchemasCommand, CopyToS3Command, DumpDataToS3Command,
                  LoadRedshiftCommand, UpdateRedshiftCommand, ExtractLoadTransformCommand,
                  ValidateDesignsCommand, ListFilesCommand]:
        cmd = klass()
        cmd.add_to_parser(subparsers)

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
                            help="Set path to configuration file (required if DATA_WAREHOUSE_CONFIG is not set)",
                            required=True)
    else:
        parser.add_argument("-c", "--config",
                            help="Change path to configuration file (using DATA_WAREHOUSE_CONFIG=%(default)s)",
                            default=default_config)

    # Set defaults so that we can avoid having to test the Namespace object.
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
    group.add_argument("-v", "--verbose", help="Increase verbosity",
                       action="store_const", const="DEBUG", dest="log_level")
    group.add_argument("-s", "--silent", help="Decrease verbosity",
                       action="store_const", const="WARNING", dest="log_level")

    example_password = uuid.uuid4().hex.title()

    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not modify stuff", default=False, action="store_true")
    if "prefix" in options:
        prefix = parser.add_mutually_exclusive_group()
        prefix.add_argument("-p", "--prefix", default=getpass.getuser(),
                            help="Select prefix in S3 bucket (default is user name: '%(default)s')")
        prefix.add_argument("-e", "--prefix-env", dest="prefix", metavar="ENV", action=AppendDateAction,
                            help="Set prefix in S3 bucket to '<ENV>/<CURRENT_DATE>'")
    if "table-design-dir" in options:
        parser.add_argument("-t", "--table-design-dir",
                            help="Set path to directory with table design files (default: '%(default)s')",
                            default="./schemas")
    if "drop" in options:
        parser.add_argument("-d", "--drop",
                            help="First drop table or view to force update of definition", default=False,
                            action="store_true")
    if "explain" in options:
        parser.add_argument("-x", "--add-explain-plan", help="Add explain plan to log", action="store_true")
    if "force" in options:
        parser.add_argument("-f", "--force", help="force updates", default=False, action="store_true")
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
        setattr(namespace, self.dest, values + today)


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
        raise NotImplementedError("Instance of %s has no proper callback" % self.__class__.__name__)


class InitialSetupCommand(SubCommand):

    def __init__(self):
        super().__init__("initialize",
                         "Create schemas, users, and groups",
                         "Create schemas for each source, users, and user groups for etl and analytics")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["password"])
        parser.add_argument("-k", "--skip-user-creation", help="Skip user and groups; only create schemas",
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
        group.add_argument("-e", "--etl-user", help="Add user also to ETL group", action="store_true")
        group.add_argument("-a", "--add-user-schema",
                           help="Add new schema, writable for the user",
                           action="store_true")
        group.add_argument("-k", "--skip-user-creation",
                           help="Skip new user; only change search path of existing user",
                           default=False, action="store_true")

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dw.create_user(settings, args.user_name, args.password,
                               args.etl_user, args.add_user_schema, args.skip_user_creation)


class DownloadSchemasCommand(SubCommand):

    def __init__(self):
        super().__init__("design",
                         "Bootstrap schema information from sources",
                         "Download schema information from sources (and compare against current table designs)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "table-design-dir", "dry-run"])
        parser.add_argument("-j", "--jobs", help="Number of parallel connections (default: %(default)s)",
                            type=check_positive_int, default=1)

    def callback(self, args, settings):
        etl.schemas.download_schemas(settings, args.target, args.table_design_dir, args.jobs, args.dry_run)


class CopyToS3Command(SubCommand):

    def __init__(self):
        super().__init__("sync",
                         "Copy table design files to S3",
                         "Copy table design files from local directory to S3")

    def add_arguments(self, parser):
        # XXX Add option to "force" sync (delete out-of-date files)
        add_standard_arguments(parser, ["target", "table-design-dir", "prefix", "dry-run"])
        parser.add_argument("-g", "--git-modified", help="Copy files modified in work tree", action="store_true")

    def callback(self, args, settings):
        etl.schemas.copy_to_s3(settings, args.target, args.table_design_dir, args.prefix,
                               args.git_modified, args.dry_run)


class DumpDataToS3Command(SubCommand):

    def __init__(self):
        super().__init__("dump",
                         "Dump table data from sources",
                         "Dump table contents to files in S3 (needs Spark context)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["target", "prefix", "dry-run"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.dump.dump_to_s3(settings, args.target, args.prefix, args.dry_run)


class LoadRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("load",
                         "Load data into Redshift from files in S3",
                         "Load data into Redshift from files in S3 (as a forced reload)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run", "prefix", "target"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.load.load_to_redshift(settings, args.target, args.prefix, args.dry_run)


class UpdateRedshiftCommand(SubCommand):

    def __init__(self):
        super().__init__("update",
                         "Update data in Redshift",
                         "Update data in Redshift from files in S3 (without schema modifications)")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run", "prefix", "target"])

    def callback(self, args, settings):
        with etl.pg.log_error():
            etl.load.update_in_redshift(settings, args.target, args.prefix, args.dry_run)


class ExtractLoadTransformCommand(SubCommand):

    def __init__(self):
        super().__init__("etl",
                         "Run complete ETL (or ELT)",
                         "Validate designs, extract data, and load data, possibly with transforms")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["dry-run", "prefix", "force"])

    def callback(self, args, settings):
        # XXX Need to validate against upstream ... etl.dump.download_schemas(args, settings)
        etl.dump.dump_to_s3(settings, args.target, args.prefix, args.dry_run)
        if args.force:
            etl.load.load_to_redshift(settings, [], args.prefix, args.dry_run)
        else:
            etl.load.update_in_redshift(settings, [], args.prefix, args.dry_run)


class ValidateDesignsCommand(SubCommand):

    def __init__(self):
        super().__init__("validate",
                         "Validate table design files",
                         "Validate table design files")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "table-design-dir", "target"])
        parser.add_argument("-g", "--git-modified", help="Check files modified in work tree", action="store_true")

    def callback(self, args, settings):
        etl.schemas.validate_designs(settings, args.target, args.table_design_dir, args.git_modified)


class ListFilesCommand(SubCommand):

    def __init__(self):
        super().__init__("ls",
                         "List files in S3",
                         "List files in the S3 bucket and starting with prefix by source, table, and type")

    def add_arguments(self, parser):
        add_standard_arguments(parser, ["prefix", "target"])

    def callback(self, args, settings):
        etl.s3.list_files(settings, args.prefix, args.target)


if __name__ == "__main__":
    run_arg_as_command()
