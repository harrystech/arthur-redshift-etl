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

from etl import package_version


def configure_logging(full_format: bool=False, log_level: str=None,) -> None:
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
            else:
                logger.info("Skipping config file '%s'", filename)

    # Need to load at least the defaults and some installation specific file:
    if count_settings < 2:
        raise RuntimeError("Failed to find enough configuration files")

    schema = load_json("settings.schema")
    jsonschema.validate(settings, schema)

    class Accessor:
        def __init__(self, data):
            self._data = data

        def __call__(self, *argv):
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
