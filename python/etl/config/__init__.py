"""
This module provides global access to settings.  Always treat them nicely and read-only.

We use the term "config" files to refer to all files that may reside in the "config" directory:
* "Settings" files (ending in '.yaml') which drive the data warehouse or resource settings
* Environment files (with variables used in connections)
* Other files (like release notes)
"""

import datetime
import logging
import os
import os.path
import re
import sys
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Set

import jsonschema
import pkg_resources
import simplejson as json
import yaml
from simplejson.errors import JSONDecodeError

import etl.config.dw
from etl.config.dw import DataWarehouseConfig
from etl.errors import ETLRuntimeError, InvalidArgumentError, SchemaInvalidError, SchemaValidationError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# Global config objects - always use accessors!
_dw_config: Optional[DataWarehouseConfig] = None
_mapped_config: Optional[Dict[str, str]] = None

# Local temp directory used for bootstrap, temp files, etc.
# TODO(tom): This is a misnomer -- it's also the install directory on EC2 hosts.
ETL_TMP_DIR = "/tmp/redshift_etl"


def package_version(package_name="redshift_etl"):
    return "{} v{}".format(package_name, pkg_resources.get_distribution(package_name).version)


def get_dw_config():
    return _dw_config


def get_config_value(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Lookup configuration value in known and flattened settings -- pass in a fully-qualified name.

    Note the side effect here: once accessed, the settings remember the default if it wasn't set
    before.
    """
    assert _mapped_config is not None, "attempted to get config value before reading config map"
    if default is None:
        return _mapped_config.setdefault(name)
    return _mapped_config.setdefault(name, default)


def get_config_int(name: str, default: Optional[int] = None) -> int:
    """
    Lookup a configuration value that is an integer.

    It is an error if the value (even when using the default) is None.
    """
    if default is None:
        value = get_config_value(name)
    else:
        value = get_config_value(name, str(default))
    if value is None:
        raise InvalidArgumentError("missing config for {}".format(name))
    return int(value)


def get_config_list(name: str) -> List[int]:
    """Lookup a configuration value that is a List."""
    value = get_config_value(name)
    if value is None:
        raise InvalidArgumentError("missing config for {}".format(name))
    return list(map(int, value.split(",")))


def set_config_value(name: str, value: str) -> None:
    """Set configuration value to given string."""
    assert _mapped_config is not None, "attempted to set config value before reading config map"
    _mapped_config[name] = value


def set_safe_config_value(name: str, value: str) -> None:
    """
    Replace "unsafe" characters with '-' and set configuration value.

    >>> etl.config._mapped_config = {}
    >>> set_safe_config_value("test_value", "something/unsafe")
    >>> get_config_value("test_value")
    'something-unsafe'
    """
    set_config_value(name, "-".join(re.findall("[a-zA-Z0-9_.-]+", value)))


def get_config_map() -> Dict[str, str]:
    if _mapped_config is None:
        return {}
    else:
        # Since the mapped config is flattened, we don't worry about a deep copy here.
        return dict(_mapped_config)


def _flatten_hierarchy(prefix, props):
    assert isinstance(props, dict), "oops, this should only be called with dicts, got {}".format(type(props))
    for key in sorted(props):
        full_key = "{}.{}".format(prefix, key)
        if isinstance(props[key], dict):
            for sub_key, sub_prop in _flatten_hierarchy(full_key, props[key]):
                yield sub_key, sub_prop
        else:
            yield full_key, props[key]


def _build_config_map(settings):
    mapping = OrderedDict()
    # Load everything that is not explicitly handled by the data warehouse configuration.
    for section in frozenset(settings).difference({"data_warehouse", "sources", "type_maps"}):
        for name, value in _flatten_hierarchy(section, settings[section]):
            mapping[name] = value
    return mapping


def etl_tmp_dir(path: str) -> str:
    """Return the absolute path within the ETL runtime directory for the selected path."""
    return os.path.join(ETL_TMP_DIR, path)


def load_environ_file(filename: str) -> None:
    """
    Set environment variables based on file contents.

    Only lines that look like 'NAME=VALUE' or 'export NAME=VALUE' are used,
    other lines are silently dropped.
    """
    logger.info("Loading environment variables from '%s'", filename)
    assignment_re = re.compile(r"\s*(?:export\s+)?(\w+)=(\S+)")
    with open(filename) as content:
        settings = [match.groups() for match in map(assignment_re.match, content) if match is not None]
    for name, value in settings:
        os.environ[name] = value


def _deep_update(old: dict, new: dict) -> None:
    """
    Update "old" dict with values found in "new" dict at the right hierarchy level.

    Examples:
    >>> old = {'l1': {'l2': {'a': 'A', 'b': 'B'}}, 'c': 'CC'}
    >>> new = {'l1': {'l2': {'a': 'AA', 'b': 'BB'}, 'd': 'DD'}}
    >>> _deep_update(old, new)
    >>> old['l1']['l2']['a'], old['l1']['l2']['b'], old['c'], old['l1']['d']
    ('AA', 'BB', 'CC', 'DD')
    """
    for key, value in new.items():
        if key in old:
            if isinstance(value, dict):
                _deep_update(old[key], value)
            else:
                old[key] = value
        else:
            old[key] = value


def load_settings_file(filename: str, settings: dict) -> None:
    """Load new settings from config file and merge with given settings."""
    logger.info("Loading settings from '%s'", filename)
    with open(filename) as content:
        new_settings = yaml.safe_load(content)
    _deep_update(settings, new_settings)


def get_python_info() -> str:
    """Return minimal information about this Python version."""
    return ("Running Python {version_info[0]}.{version_info[1]}.{version_info[2]} ({platform})").format(
        version_info=sys.version_info, platform=sys.platform
    )


def get_release_info() -> str:
    """
    Read the release file and return all lines bunched into one comma-separated value.

    Life's exciting. And short. But mostly exciting.
    """
    if pkg_resources.resource_exists(__name__, "release.txt"):
        content = pkg_resources.resource_string(__name__, "release.txt")
        text = content.decode(errors="ignore").strip()
        lines = [line.strip() for line in text.split("\n")]
        release_info = ", ".join(lines)
    else:
        release_info = "Not available. Hint: release info will be created by upload_env.sh"
    return "Release information: " + release_info


def yield_config_files(config_files: Iterable[str], default_file: str = None) -> Iterable[str]:
    """
    Generate filenames from the list of files or directories in config_files and default_file.

    If the default_file is not None, then it is always prepended to the list of files.
    It is an error (sadly, at runtime) if the default file is not a file that's part of the package.

    Note that files in directories are always sorted by their name.
    """
    if default_file:
        yield pkg_resources.resource_filename(__name__, default_file)

    for name in config_files:
        if os.path.isdir(name):
            files = sorted(os.path.join(name, child) for child in os.listdir(name))
        else:
            files = [name]
        for filename in files:
            yield filename


def load_config(config_files: Iterable[str], default_file: str = "default_settings.yaml") -> None:
    """
    Load settings and environment from config files and set our global settings.

    The default, if provided, is always the first file to be loaded.
    If the config "file" is actually a directory, (try to) read all the files in that directory.

    The settings are validated against their schema.
    """
    settings: Dict[str, Any] = dict()
    count_settings = 0
    for filename in yield_config_files(config_files, default_file):
        if filename.endswith(".sh"):
            load_environ_file(filename)
        elif filename.endswith((".yaml", ".yml")):
            load_settings_file(filename, settings)
            count_settings += 1
        else:
            logger.debug("Skipping unknown config file '%s'", filename)

    # Need to load at least the defaults and some installation specific file:
    if count_settings < 2:
        raise ETLRuntimeError("failed to find enough configuration files (need at least default and local config)")

    validate_with_schema(settings, "settings.schema")

    # Set values for 'date.today' and 'date.yesterday' (in case they aren't set already.)
    # The values are wrt current UTC and look like a path, e.g. '2017/05/16'.
    today = datetime.datetime.utcnow().date()
    date_settings = settings.setdefault("date", {})
    date_settings.setdefault("today", today.strftime("%Y/%m/%d"))
    date_settings.setdefault("yesterday", (today - datetime.timedelta(days=1)).strftime("%Y/%m/%d"))

    global _dw_config
    _dw_config = etl.config.dw.DataWarehouseConfig(settings)

    global _mapped_config
    _mapped_config = _build_config_map(settings)
    if _mapped_config is not None:
        _mapped_config["data_warehouse.owner.name"] = _dw_config.owner.name

    set_config_value("version", package_version())


def validate_with_schema(obj: dict, schema_name: str) -> None:
    """
    Validate the given object (presumably from reading a YAML file) against its schema.

    This will also validate the schema itself!
    """
    validation_internal_errors = (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
        JSONDecodeError,
    )
    schema = load_json_schema(schema_name)
    try:
        jsonschema.validate(obj, schema, format_checker=jsonschema.draft7_format_checker)
    except validation_internal_errors as exc:
        raise SchemaValidationError("failed to validate against '%s'" % schema_name) from exc


def gather_setting_files(config_files: Iterable[str]) -> List[str]:
    """
    Gather all settings files (*.yaml and *.sh files) that should be deployed together.

    NOTE This drops any hierarchy in the config files. It is an error if we detect that there are
    settings files in separate directories that have the same filename.
    So trying '-c hello/world.yaml -c hola/world.yaml' triggers an exception.
    """
    settings_found: Set[str] = set()
    settings_with_path = []

    for fullname in yield_config_files(config_files):
        filename = os.path.basename(fullname)
        if filename.startswith("credentials") and filename.endswith(".sh"):
            continue
        if not filename.endswith((".yaml", ".yml", ".sh")):
            continue
        if filename in settings_found:
            raise KeyError("found configuration file in multiple locations: '%s'" % filename)
        settings_found.add(filename)
        settings_with_path.append(fullname)
    return sorted(settings_with_path)


@lru_cache()
def load_json(filename: str):
    """Load JSON-formatted file into native data structure."""
    return json.loads(pkg_resources.resource_string(__name__, filename))


@lru_cache()
def load_json_schema(schema_name: str):
    """Load JSON-formatted file validate it assuming that it represents a schema."""
    validation_internal_errors = (
        jsonschema.exceptions.ValidationError,
        jsonschema.exceptions.SchemaError,
        JSONDecodeError,
    )
    try:
        schema = load_json(schema_name)
        jsonschema.Draft7Validator.check_schema(schema)
    except validation_internal_errors as exc:
        raise SchemaInvalidError("schema in '%s' is not valid" % schema_name) from exc
    return schema


if __name__ == "__main__":
    print(get_release_info())
