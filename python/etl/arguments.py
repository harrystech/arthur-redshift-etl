import argparse
from datetime import datetime
import os
import uuid


def argument_parser(options, **kwargs):
    """
    Create default argument parser with standard set of options for our ETL
    """
    parser = argparse.ArgumentParser(**kwargs)
    example_password = uuid.uuid4().hex.title()
    default_config = os.environ["DATA_WAREHOUSE_CONFIG"] if "DATA_WAREHOUSE_CONFIG" in os.environ else None

    if "config" in options:
        if default_config is None:
            parser.add_argument("-c", "--config",
                                help="path to configuration file (required if DATA_WAREHOUSE_CONFIG is not set)",
                                required=True)
        else:
            parser.add_argument("-c", "--config", help="path to configuration file (default: '%(default)s')",
                                default=default_config)
    if "prefix" in options:
        parser.add_argument("-p", "--prefix", help="prefix in S3 bucket (default based on date: '%(default)s')",
                            default=datetime.now().strftime("%Y-%m-%d"))
    if "data-dir" in options:
        parser.add_argument("-o", "--data-dir", help="path to data directory (default: '%(default)s')", default="data")
    if "table-design-dir" in options:
        parser.add_argument("-s", "--table-design-dir",
                            help="path to directory with table design files (default: '%(default)s')",
                            default="schemas")
    if "dry-run" in options:
        parser.add_argument("-n", "--dry-run", help="do not actually copy data", default=False, action="store_true")
    if "force" in options:
        parser.add_argument("-f", "--force", help="allow overwriting files", default=False, action="store_true")
    if "table" in options:
        parser.add_argument("table", help="glob pattern or identifier to select target table(s)", nargs='?')
    if "username" in options:
        parser.add_argument("username", help="name for new user")
    if "password" in options:
        parser.add_argument("password", help="password for new user (example: '%s')" % example_password, nargs='?')
    return parser
