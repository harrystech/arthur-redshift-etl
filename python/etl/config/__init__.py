"""
We use "config" files to refer to all files that may reside in the "config" directory:
* "Settings" files (ending in '.yaml') which drive the data warehouse settings
* Environment files (with variables)
* Other files (like release notes)

This module provides global access to settings.  Always treat them nicely and read-only.
"""

import logging
import logging.config
import os
import os.path
import sys
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import pkg_resources
import jsonschema
import simplejson as json
import yaml

import etl.config.dw
from etl.config.dw import DataWarehouseConfig
from etl.errors import SchemaInvalidError, SchemaValidationError
import etl.monitor

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# Global config objects - always use accessors!
_dw_config = None  # type: Optional[DataWarehouseConfig]
_mapped_config = None  # type: Optional[Dict[str, str]]

# Local temp directory used for bootstrap, temp files, etc.
ETL_TMP_DIR = "/tmp/redshift_etl"


def package_version(package_name="redshift_etl"):
    return "{} v{}".format(package_name, pkg_resources.get_distribution(package_name).version)


def get_dw_config():
    return _dw_config


def get_config_value(name: str, default: Optional[str]=None) -> Optional[str]:
    assert _mapped_config is not None, "attempted to get config value before reading config map"
    return _mapped_config.get(name, default)


def set_config_value(name: str, value: str) -> None:
    if _mapped_config is not None:
        _mapped_config.setdefault(name, value)


def get_config_map() -> Dict[str, str]:
    if _mapped_config is None:
        return {}
    else:
        return dict(_mapped_config)


def _flatten_hier(prefix, props):
    assert isinstance(props, dict), "oops, this should only be called with dicts, got {}".format(type(props))
    for key in sorted(props):
        full_key = "{}.{}".format(prefix, key)
        if isinstance(props[key], dict):
            for sub_key, sub_prop in _flatten_hier(full_key, props[key]):
                yield sub_key, sub_prop
        else:
            yield full_key, props[key]


def _build_config_map(settings):
    mapping = OrderedDict()
    for section in {"arthur_settings", "object_store", "resources", "etl_events"}.intersection(settings):
        for name, value in _flatten_hier(section, settings[section]):
            mapping[name] = value
    return mapping


def etl_tmp_dir(path: str) -> str:
    """
    Return the absolute path within the ETL runtime directory for the selected path.
    """
    return os.path.join(ETL_TMP_DIR, path)


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
    # Ignored due to lack of stub in type checking library
    logging.captureWarnings(True)  # type: ignore
    logger.info("Starting log for %s with ETL ID %s", package_version(), etl.monitor.Monitor.etl_id)
    logger.info('Command line: "%s"', ' '.join(sys.argv))
    logger.debug("Current working directory: '%s'", os.getcwd())
    logger.info(get_release_info())


def load_environ_file(filename: str) -> None:
    """
    Load additional environment variables from file.

    Only lines that look like 'NAME=VALUE' or 'export NAME=VALUE' are used,
    other lines are silently dropped.
    """
    logger.info("Loading environment variables from '%s'", filename)
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
    logger.info("Loading settings from '%s'", filename)
    with open(filename) as f:
        new_settings = yaml.safe_load(f)
        for key in new_settings:
            # Try to update only update-able settings
            if key in settings and isinstance(settings[key], dict):
                settings[key].update(new_settings[key])
            else:
                settings[key] = new_settings[key]


def get_release_info() -> str:
    """
    Read the release file and return all lines bunched into one comma-separated value.
    Life's exciting. And short. But mostly exciting.
    """
    if pkg_resources.resource_exists(__name__, "release.txt"):
        content = pkg_resources.resource_string(__name__, "release.txt")
        text = content.decode(errors='ignore').strip()
        lines = [line.strip() for line in text.split('\n')]
        release_info = ", ".join(lines)
    else:
        release_info = "Not available. Hint: release info will be created by upload_env.sh"
    return "Release information: " + release_info


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


def load_config(config_files: Sequence[str], default_file: str="default_settings.yaml") -> None:
    """
    Load settings and environment from config files (starting with the default if provided),
    set our global settings.

    The settings are validated against their schema.
    If the config "file" is actually a directory, (try to) read all the files in that directory.
    """
    settings = dict()  # type: Dict[str, Any]
    count_settings = 0
    for filename in yield_config_files(config_files, default_file):
        if filename.endswith(".sh"):
            load_environ_file(filename)
        elif filename.endswith((".yaml", ".yml")):
            load_settings_file(filename, settings)
            count_settings += 1
        else:
            logger.info("Skipping unknown config file '%s'", filename)

    # Need to load at least the defaults and some installation specific file:
    if count_settings < 2:
        raise RuntimeError("Failed to find enough configuration files (need at least default and local config)")

    validate_with_schema(settings, "settings.schema")

    # TODO Clean up this backwards-compatible code
    settings.setdefault("object_store", {})
    s3_bucket_name = settings.get("data_lake", {}).get("s3", {}).get("bucket_name")
    if s3_bucket_name is not None:
        settings["object_store"].setdefault("s3", {"bucket_name": s3_bucket_name})
    iam_role = settings.get("data_lake", {}).get("iam_role")
    if iam_role is not None:
        settings["object_store"].setdefault("iam_role", iam_role)

    global _mapped_config
    _mapped_config = _build_config_map(settings)

    global _dw_config
    _dw_config = etl.config.dw.DataWarehouseConfig(settings)


def validate_with_schema(obj: dict, schema_name: str) -> None:
    """
    Validate the given object (presumably from reading a YAML file) against its schema.

    This will also validate the schema itself!
    """
    validation_internal_errors = (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
        json.scanner.JSONDecodeError)
    try:
        schema = etl.config.load_json(schema_name)
        jsonschema.Draft4Validator.check_schema(schema)
    except validation_internal_errors as exc:
        raise SchemaInvalidError("schema in '%s' is not valid" % schema_name) from exc
    try:
        jsonschema.validate(obj, schema)
    except validation_internal_errors as exc:
        raise SchemaValidationError("failed to validate against '%s'" % schema_name) from exc


def gather_setting_files(config_files: Sequence[str]) -> List[str]:
    """
    Gather all settings files (*.yaml and *.sh files) -- this drops any hierarchy in the config files (!).

    It is an error if we detect that there are settings files in separate directories that have the same filename.
    So trying '-c hello/world.yaml -c hola/world.yaml' triggers an exception.
    """
    settings_found = set()  # type: Set[str]
    settings_with_path = []

    for fullname in yield_config_files(config_files):
        filename = os.path.basename(fullname)
        if filename.startswith("credentials") and filename.endswith(".sh"):
            continue
        # TODO Once we have route53 setup, drop support of .hosts file
        if filename.endswith((".yaml", ".yml", ".sh", ".hosts")):
            if filename not in settings_found:
                settings_found.add(filename)
            else:
                raise KeyError("found configuration file in multiple locations: '%s'" % filename)
            settings_with_path.append(fullname)
    return sorted(settings_with_path)


@lru_cache()
def load_json(filename: str):
    return json.loads(pkg_resources.resource_string(__name__, filename))


if __name__ == "__main__":
    print(get_release_info())
