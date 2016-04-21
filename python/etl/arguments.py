import argparse
import getpass
import os
import uuid

import pkg_resources


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
    parser.add_argument("-v", "--verbose", action="store_true", help="increase verbosity")

    example_password = uuid.uuid4().hex.title()
    default_config = os.environ.get("DATA_WAREHOUSE_CONFIG")

    if "config" in options:
        if default_config is None:
            parser.add_argument("-c", "--config",
                                help="path to configuration file (required if DATA_WAREHOUSE_CONFIG is not set)",
                                required=True)
        else:
            parser.add_argument("-c", "--config",
                                help="path to configuration file (default: DATA_WAREHOUSE_CONFIG=%(default)s)",
                                default=default_config)
    if "prefix" in options:
        parser.add_argument("-p", "--prefix", help="prefix in S3 bucket (default is user name: '%(default)s')",
                            default=getpass.getuser())
    if "data-dir" in options:
        parser.add_argument("-o", "--data-dir", help="path to data directory (default: '%(default)s')", default="data")
    if "table-design-dir" in options:
        parser.add_argument("-s", "--table-design-dir",
                            help="path to directory with table design files (default: '%(default)s')",
                            default="schemas")
    if "drop-table" in options:
        parser.add_argument("-d", "--drop-table",
                            help="drop table to force update of table definition", default=False, action="store_true")
    if "drop-view" in options:
        parser.add_argument("-d", "--drop-view",
                            help="drop view to force update of view definition", default=False, action="store_true")
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not actually copy data", default=False, action="store_true")
    if "force" in options:
        parser.add_argument("-f", "--force", help="allow overwriting files", default=False, action="store_true")
    if "table" in options:
        parser.add_argument("table", help="glob pattern or identifier to select target table(s)", nargs='*')
    if "view" in options:
        parser.add_argument("view", help="glob pattern or identifier to select target view(s)", nargs='*')
    if "username" in options:
        parser.add_argument("username", help="name for new user")
    if "password" in options:
        parser.add_argument("password", help="password for new user (example: '%s')" % example_password, nargs='?')
    return parser
