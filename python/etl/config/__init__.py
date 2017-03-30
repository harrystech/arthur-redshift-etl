"""
Objects to deal with configuration of our data warehouse, like
* data warehouse itself
* its schemas
* its users

We use "config" files to refer to all files that may reside in the "config" directory:
* "Settings" files (ending in '.yaml') which drive the data warehouse settings
* Environment files (with variables)
* Other files (like release notes)
"""

from collections import defaultdict
from functools import lru_cache
import logging
import logging.config
import os
import os.path
import sys
from typing import Iterable, List, Sequence

import pkg_resources
import jsonschema
import simplejson as json
import yaml

import etl
import etl.pg


# Local temp directory used for bootstrap, temp files, etc.
ETL_TMP_DIR = "/tmp/redshift_etl"


def etl_tmp_dir(path: str) -> str:
    """
    Return the absolute path within the ETL runtime directory for the selected path.
    """
    return os.path.join(ETL_TMP_DIR, path)


class DataWarehouseUser:
    """
    Data warehouse users have always a name and group associated with them.
    Users may have a schema "belong" to them which they then have write access to.
    This is useful for system users, mostly, since end users should treat the
    data warehouse as read-only.
    """
    def __init__(self, user_info):
        self.name = user_info["name"]
        self.group = user_info["group"]
        self.schema = user_info.get("schema")


class DataWarehouseSchema:
    """
    Schemas in the data warehouse fall into one of four buckets:
    (1) Upstream source backed by a database.  Data will be dumped from there and
    so we need to have a DSN with which we can connect.
    (2) Upstream source backed by CSV files in S3.  Data will be "dumped" in the sense
    that the ETL will create a manifest file suitable for the COPY command.  No DSN
    is needed here.
    (2.5) Target in S3 for "unload" command, which may also be an upstream source.
    (3) Schemas with CTAS or VIEWs that are computed during the ETL.  Data cannot be dumped
    (but maybe unload'ed).
    (4) Schemas reserved for users (where user could be a BI tool)

    Although there is a (logical) distinction between "sources" and "schemas" in the settings file
    those are really all the same here ...
    """
    def __init__(self, schema_info, etl_access=None):
        self.name = schema_info["name"]
        self.description = schema_info.get("description")
        # Schemas have an 'owner' user (with ALL privileges)
        # and lists of 'reader' and 'writer' groups with corresponding permissions
        self.owner = schema_info["owner"]
        self.reader_groups = schema_info.get("readers", schema_info.get("groups", []))
        self.writer_groups = schema_info.get("writers", [])
        # Booleans to help figure out which bucket the schema is in (see doc for class)
        self.is_database_source = "read_access" in schema_info
        self.is_static_source = "s3_bucket" in schema_info and "s3_path_template" in schema_info
        self.is_an_unload_target = "s3_bucket" in schema_info and "s3_unload_path_template" in schema_info
        # How to access the source of the schema (per DSN (of source or DW)? per S3?)
        if self.is_database_source:
            self._dsn_env_var = schema_info["read_access"]
        elif self.is_static_source or self.is_an_unload_target:
            self._dsn_env_var = None
        else:
            self._dsn_env_var = etl_access
        self.has_dsn = self._dsn_env_var is not None

        self.s3_bucket = schema_info.get("s3_bucket")
        self.s3_path_template = schema_info.get("s3_path_template")
        self.s3_unload_path_template = schema_info.get("s3_unload_path_template")
        # When dealing with this schema of some upstream source, which tables should be used? skipped?
        self.include_tables = schema_info.get("include_tables", [self.name + ".*"])
        self.exclude_tables = schema_info.get("exclude_tables", [])

    @property
    def dsn(self):
        """
        Return connection string suitable for the schema which is
        - the value of the environment variable named in the read_access field for upstream sources
        - the value of the environment variable named in the etl_access field of the data warehouse for schemas
            that have CTAS or views
        Evaluation of the DSN (and the environment variable) is deferred so that an environment variable
        may be not set if it is actually not used.
        """
        # Note this returns None for a static source.
        if self._dsn_env_var:
            return etl.pg.parse_connection_string(env_value(self._dsn_env_var))

    @property
    def groups(self):
        return self.reader_groups + self.writer_groups

    @property
    def backup_name(self):
        return '$'.join(("arthur_temp", self.name))

    def validate_access(self):
        """
        Raise exception if environment variable is not set ... only used for the side effect of test.
        """
        # FIXME need to start checking env vars before running anything heavy
        if self._dsn_env_var is not None and self._dsn_env_var not in os.environ:
            raise KeyError("Environment variable to access '{0.name}' not set: {0._read_access}".format(self))


class DataWarehouseConfig:
    """
    Pretty face to the object from the settings files.
    """
    def __init__(self, settings):
        dw_settings = settings["data_warehouse"]

        # Environment variables with DSN
        self._admin_access = dw_settings["admin_access"]
        self._etl_access = dw_settings["etl_access"]
        self._reference_warehouse_access = dw_settings["reference_warehouse_access"]
        root = DataWarehouseUser(dw_settings["owner"])
        # Users are in the order from the config
        other_users = [DataWarehouseUser(user) for user in dw_settings.get("users", []) if user["name"] != "default"]

        # Note that the "owner," which is our super-user of sorts, comes first.
        self.users = [root] + other_users
        schema_owner_map = {u.schema: u.name for u in self.users if u.schema}

        # Schemas (upstream sources followed by transformations)
        self.schemas = [
            DataWarehouseSchema(
                dict(info, owner=schema_owner_map.get(info['name'], root.name)),
                self._etl_access)
            for info in settings["sources"] + dw_settings["schemas"]
        ]

        # Schemas may grant access to groups that have no bootstrapped users, so create all mentioned user groups
        other_groups = {u.group for u in other_users} | {g for schema in self.schemas for g in schema.reader_groups}

        # Groups are in sorted order after the root group
        self.groups = [root.group] + sorted(other_groups)
        # Surely You're Joking, Mr. Feynman?  Nope, pop works here.
        self.default_group = [user["group"] for user in dw_settings["users"] if user["name"] == "default"].pop()
        # Credentials used in COPY command that allow jumping into our data lake
        self.iam_role = dw_settings["iam_role"]
        # Mapping SQL types to be able to automatically insert "expressions" into table design files.
        self.type_maps = settings["type_maps"]
        # Relation glob patterns indicating unacceptable load failures; matches everything if unset
        self.required_in_full_load_selector = etl.TableSelector(settings.get("required_in_full_load", []))

    @property
    def owner(self):
        return self.users[0].name

    @property
    def dsn_admin(self):
        return etl.pg.parse_connection_string(env_value(self._admin_access))

    @property
    def dsn_etl(self):
        return etl.pg.parse_connection_string(env_value(self._etl_access))

    @property
    def dsn_admin_on_etl_db(self):
        # To connect as a superuser, but on the database on which you would ETL
        return dict(self.dsn_admin, database=self.dsn_etl['database'])

    @property
    def dsn_reference(self):
        return etl.pg.parse_connection_string(env_value(self._reference_warehouse_access))


def configure_logging(full_format: bool=False, log_level: str=None) -> None:
    """
    Setup logging to go to console and application log file

    If full_format is True, then use the terribly verbose format of
    the application log file also for the console.  And log at the DEBUG level.
    Otherwise, you can choose the log level by passing one in.
    """
    config = load_json('logging.json')
    if full_format:
        config["formatters"]["console"]["format"] = config["formatters"]["file"]["format"]
        config["handlers"]["console"]["level"] = logging.DEBUG
    elif log_level:
        config["handlers"]["console"]["level"] = log_level
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    logging.getLogger(__name__).info('Starting log for "%s" (%s)', ' '.join(sys.argv), etl.package_version())


def load_environ_file(filename: str) -> None:
    """
    Load additional environment variables from file.

    Only lines that look like 'NAME=VALUE' or 'export NAME=VALUE' are used,
    other lines are silently dropped.
    """
    logging.getLogger(__name__).info("Loading environment variables from '%s'", filename)
    with open(filename) as f:
        for line in f:
            tokens = [token.strip() for token in line.split('=', 1)]
            if len(tokens) == 2 and not tokens[0].startswith('#'):
                name = tokens[0].replace("export", "").strip()
                value = tokens[1]
                os.environ[name] = value


def load_settings_file(filename: str, settings: dict) -> None:
    """
    Load new settings from config file or a directory of config files
    and UPDATE settings (old settings merged with new).
    """
    logger = logging.getLogger(__name__)
    logger.info("Loading settings from '%s'", filename)
    with open(filename) as f:
        new_settings = yaml.safe_load(f)
        for key in new_settings:
            # Try to update only update-able settings
            if key in settings and isinstance(settings[key], dict):
                settings[key].update(new_settings[key])
            else:
                settings[key] = new_settings[key]


def read_release_file(filename: str) -> None:
    """
    Read the release file and echo its contents to the log.
    Life's exciting. And short. But mostly exciting.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Loading release information from '%s'", filename)
    with open(filename) as f:
        lines = [line.strip() for line in f]
    logger.info("Release information: %s", ', '.join(lines))


def yield_config_files(config_files: Sequence[str], default_file: str=None) -> Iterable[str]:
    """
    Generate filenames from the list of files or directories in :config_files and :default_file

    If the default_file is not None, then it is always prepended to the list of files.
    (It is an error (sadly, at runtime) if the default file is not a file that's part of the package.)

    Note that files in directories are always sorted by their name.
    """
    if default_file:
        yield pkg_resources.resource_filename(__name__, default_file)

    for name in config_files:
        if os.path.isdir(name):
            files = sorted(os.path.join(name, n) for n in os.listdir(name))
        else:
            files = [name]
        for filename in files:
            yield filename


def load_config(config_files: Sequence[str], default_file: str="default_settings.yaml") -> dict:
    """
    Load settings and environment from config files (starting with the default if provided).

    If the config "file" is actually a directory, (try to) read all the
    files in that directory.

    The settings are validated against their schema before being returned.
    """
    logger = logging.getLogger(__name__)
    settings = defaultdict(dict)
    count_settings = 0
    for filename in yield_config_files(config_files, default_file):
        if filename.endswith(".sh"):
            load_environ_file(filename)
        elif filename.endswith((".yaml", ".yml")):
            load_settings_file(filename, settings)
            count_settings += 1
        elif filename.endswith("release.txt"):
            read_release_file(filename)
        else:
            logger.info("Skipping config file '%s'", filename)

    # Need to load at least the defaults and some installation specific file:
    if count_settings < 2:
        raise RuntimeError("Failed to find enough configuration files (need at least default and local config)")

    schema = load_json("settings.schema")
    jsonschema.validate(settings, schema)
    return dict(settings)


def gather_setting_files(config_files: Sequence[str]) -> List[str]:
    """
    Gather all settings files (*.yaml files) -- this drops any hierarchy in the config files (!).

    It is an error if we detect that there are settings files in separate directories that have the same filename.
    So trying '-c hello/world.yaml -c hola/world.yaml' triggers an exception.
    """
    settings_found = set()
    settings_with_path = []

    for fullname in yield_config_files(config_files):
        if fullname.endswith(('.yaml', '.yml')):
            filename = os.path.basename(fullname)
            if filename not in settings_found:
                settings_found.add(filename)
            else:
                raise KeyError("Found configuration file in multiple locations: '%s'" % filename)
            settings_with_path.append(fullname)
    return sorted(settings_with_path)


def env_value(name: str) -> str:
    """
    Retrieve environment variable or error out if variable is not set.
    This is mildly more readable than direct use of os.environ.

    :param name: Name of environment variable
    :return: Value of environment variable
    """
    value = os.environ.get(name)
    if value is None:
        raise KeyError('Environment variable "%s" not set' % name)
    if not value:
        raise ValueError('Environment variable "%s" is empty' % name)
    return os.environ[name]


@lru_cache()
def load_json(filename: str):
    return json.loads(pkg_resources.resource_string(__name__, filename))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    user_name = env_value("USER")
    print("Hello {}!".format(user_name))
