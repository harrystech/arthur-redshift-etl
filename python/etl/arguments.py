import argparse
from datetime import datetime
import getpass
import os
import uuid

import pkg_resources


class AppendDateAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        today = datetime.now().strftime("_%Y%m%d_%H%M")
        setattr(namespace, self.dest, values + today)


def argument_parser(options: list, **kwargs) -> argparse.ArgumentParser:
    """
    Create default argument parser with standard set of options for our ETL

    :param options: List of command line options to be constructed
    :param kwargs: Arguments for the ArgumentParser
    :return: Instance of an ArgumentParser
    """
    parser = argparse.ArgumentParser(**kwargs)
    package = "redshift-etl v{}".format(pkg_resources.get_distribution("redshift-etl").version)
    parser.add_argument("--version", action="version", version="%(prog)s ({})".format(package))

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", help="increase verbosity",
                       action="store_const", const="DEBUG", dest="log_level")
    group.add_argument("-s", "--silent", help="decrease verbosity",
                       action="store_const", const="WARNING", dest="log_level")

    example_password = uuid.uuid4().hex.title()
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG")

    if "config" in options:
        if default_config is None:
            parser.add_argument("-c", "--config",
                                help="set path to configuration file (required if DATA_WAREHOUSE_CONFIG is not set)",
                                required=True)
        else:
            parser.add_argument("-c", "--config",
                                help="change path to configuration file (using DATA_WAREHOUSE_CONFIG=%(default)s)",
                                default=default_config)
    if "prefix" or "prefix_env" in options:
        prefix = parser.add_mutually_exclusive_group()
        if "prefix" in options:
            prefix.add_argument("-p", "--prefix", default=getpass.getuser(),
                                help="select prefix in S3 bucket (default is user name: '%(default)s')")
        if "prefix_env" in options:
            prefix.add_argument("-e", "--prefix-env", dest="prefix", metavar="ENV", action=AppendDateAction,
                                help="set prefix in S3 bucket to '<ENV>_<DATE>'")
    if "data-dir" in options:
        parser.add_argument("-o", "--data-dir", help="set path to data directory (default: '%(default)s')",
                            default="./data")
    if "table-design-dir" in options:
        parser.add_argument("-t", "--table-design-dir",
                            help="set path to directory with table design files (default: '%(default)s')",
                            default="./schemas")
    if "drop" in options:
        parser.add_argument("-d", "--drop",
                            help="drop table or view to force update of definition", default=False, action="store_true")
    elif "drop-table" in options:
        parser.add_argument("-d", "--drop-table",
                            help="drop table to force update of table definition", default=False, action="store_true")
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not actually copy data", default=False, action="store_true")
    if "force" in options:
        parser.add_argument("-f", "--force", help="allow overwriting data files", default=False, action="store_true")
    if "table" in options:
        parser.add_argument("table", help="glob pattern or identifier to select target table(s)", nargs='*')
    elif "ctas_or_view" in options:
        parser.add_argument("ctas_or_view", help="glob pattern or identifier to select target(s)", nargs='*')
    if "username" in options:
        parser.add_argument("username", help="name for new user")
    if "password" in options:
        parser.add_argument("password", help="password for new user (example: '%s')" % example_password, nargs='?')
    return parser
