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

import etl.s3


def configure_logging(log_level: str=None) -> None:
    """
    Setup logging to go to console and application log file
    """
    config = load_json('logging.json')
    if log_level:
        config["handlers"]["console"]["level"] = log_level
    logging.config.dictConfig(load_json("logging.json"))
    logging.captureWarnings(True)
    logging.getLogger(__name__).info("Starting log for '%s'", sys.argv[0])


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
    logging.getLogger(__name__).info("Loading settings from '%s'", filename)
    with open(filename) as f:
        new_settings = yaml.safe_load(f)
        for key in new_settings:
            # Try to update only update-able settings
            if key in settings and isinstance(settings[key], dict):
                settings[key].update(new_settings[key])
            else:
                settings[key] = new_settings[key]


def load_settings(config_files: list, default_file: str="defaults.yaml"):
    """
    Load settings (and environment) from defaults and config files.
    """
    logger = logging.getLogger(__name__)
    settings = defaultdict(dict)
    default_file = pkg_resources.resource_filename(__name__, default_file)

    for name in [default_file] + config_files:
        if os.path.isdir(name):
            files = etl.s3.list_local_files(name)
        else:
            files = [name]
        for filename in files:
            if filename.endswith(".sh"):
                load_environ_file(filename)
            elif filename.endswith((".yaml", ".yml")):
                load_settings_file(filename, settings)
            else:
                logger.info("Skipping config file '%s'", filename)
    try:
        schema = load_json("settings.schema")
        jsonschema.validate(settings, schema)
    except Exception:
        raise

    class Accessor:
        def __init__(self, data):
            self._data = data

        def __call__(self, *argv):
            # TODO Add better error handling
            value = self._data
            for arg in argv:
                value = value[arg]
            return value

    return Accessor(settings)


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
