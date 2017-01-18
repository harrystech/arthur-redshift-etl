"""
Objects to deal with configuration of our data warehouse, like
* data warehouse itself
* its schemas
* its users
"""

from collections import defaultdict
from functools import lru_cache
import logging
import logging.config
import os
import sys

import pkg_resources
import jsonschema
import simplejson as json
import yaml

import etl
import etl.pg
from etl import package_version


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
    Schemas in the data warehouse fall into three buckets:
    (1) Upstream source backed by a database.  Data will be dumped from there and
    so we need to have a DSN with which we can connect.
    (2) Upstream source backed by CSV files in S3.  Data will be "dumped" in the sense
    that the ETL will create a manifest file suitable for the COPY command.  No DSN
    is needed here.
    (3) Schemas with CTAS or VIEWs that are computed during the ETL.  Data cannot be dumped
    (but maybe unload'ed).

    Although there is a (logical) distinction between "sources" and "schemas" in the settings file
    those are really all the same here ...
    """
    def __init__(self, schema_info, etl_access):
        self.name = schema_info["name"]
        self.description = schema_info.get("description")
        # Schemas have an 'owner' user (with ALL privileges)
        # and lists of 'reader' and 'writer' groups with corresponding permissions
        self.owner = schema_info["owner"]
        self.reader_groups = schema_info.get("readers", schema_info.get("groups", []))
        self.writer_groups = schema_info.get("writers", [])
        # Booleans to help figure out which bucket the schema is in (see doc for class)
        self.is_database_source = "read_access" in schema_info
        self.is_static_source = "s3_bucket" in schema_info
        self.is_upstream_source = self.is_database_source or self.is_static_source
        # How to access the source of the schema (per DSN (of source or DW)? per S3?)
        self._dsn_env_var = schema_info.get("read_access", etl_access) if not self.is_static_source else None
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
        if not self.is_static_source:
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
        # For creating table design files automatically
        self.type_maps = settings["type_maps"]
        # Relation glob patterns downgrading unique constraints to warnings; matches nothing if unset
        self.constraints_as_warnings_selector = etl.TableSelector(settings.get("constraints_as_warnings") or ['noop'])
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
    logging.getLogger(__name__).info('Starting log for "%s" (%s)', ' '.join(sys.argv), package_version())


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
        for source in new_settings.get('sources', []):
            if 'groups' in source:
                logger.warning(
                    'Settings file %s uses a deprecated permissioning API for source "%s":'
                    ' Specifying permissions with "groups" for upstream sources is deprecated.'
                    ' Please migrate to "readers" and "writers". Interpreting "groups" as "readers"',
                    filename, source.get('name', 'unnamed'))
                if 'readers' in source:
                    raise etl.ETLError('Settings file %s illegally used both "groups" and "readers" keys for source "%s"',
                                        filename, source.get('name', 'unnamed'))


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


def load_settings(config_files: list, default_file: str="default_settings.yaml"):
    """
    Load settings (and environment) from defaults and config files.

    If the config "file" is actually a directory, (try to) read all the
    files in that directory.
    """
    logger = logging.getLogger(__name__)
    settings = defaultdict(dict)
    default_file = pkg_resources.resource_filename(__name__, default_file)

    count_settings = 0
    for name in [default_file] + config_files:
        if os.path.isdir(name):
            files = sorted(os.path.join(name, n) for n in os.listdir(name))
        else:
            files = [name]
        for filename in files:
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
        raise RuntimeError("Failed to find enough configuration files")

    schema = load_json("settings.schema")
    jsonschema.validate(settings, schema)
    return settings


def env_value(name: str) -> str:
    """
    Retrieve environment variable or error out if variable is not set.
    This is mildly more readable than direct use of os.environ.

    :param name: Name of environment variable
    :return: Value of environment variable
    """
    if name not in os.environ:
        raise KeyError('Environment variable "%s" not set' % name)
    return os.environ[name]


@lru_cache()
def load_json(filename):
    return json.loads(pkg_resources.resource_string(__name__, filename))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    user_name = env_value("USER")
    print("Hello {}!".format(user_name))
