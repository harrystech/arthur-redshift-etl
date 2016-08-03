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


def load_env():
    """
    Load additional environment variables from file.

    Only lines that look like 'NAME=VALUE' or 'export NAME=VALUE' are used,
    other lines are silently dropped.
    """
    if os.path.exists("/var/tmp/config/credentials.sh"):
        with open("/var/tmp/config/credentials.sh") as f:
            for line in f:
                tokens = [token.strip() for token in line.split('=', 1)]
                if len(tokens) == 2 and not tokens[0].startswith('#'):
                    name = tokens[0].replace("export", "").strip()
                    value = tokens[1]
                    os.environ[name] = value


def load_settings(config_file: str, default_file: str="defaults.yaml"):
    """
    Load settings from defaults and config file.
    """
    settings = defaultdict(dict)
    logger = logging.getLogger(__name__)
    default_file = pkg_resources.resource_filename(__name__, default_file)
    for filename in (default_file, config_file):
        with open(filename) as f:
            logger.info("Loading configuration file '%s'", filename)
            new_settings = yaml.safe_load(f)
            for key in new_settings:
                # Try to update only update-able settings
                if key in settings and isinstance(settings[key], dict):
                    settings[key].update(new_settings[key])
                else:
                    settings[key] = new_settings[key]

    try:
        schema = load_json("settings.schema")
        jsonschema.validate(settings, schema)
    except Exception:
        logger.error("Failed to validate settings")
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
