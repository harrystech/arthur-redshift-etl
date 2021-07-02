import logging
import logging.config

from etl.config import load_json


def configure_logging(full_format: bool = False, log_level: str = None) -> None:
    """
    Set up logging to go to console and application log file.

    If full_format is True, then use the terribly verbose format of
    the application log file also for the console.  And log at the DEBUG level.
    Otherwise, you can choose the log level by passing one in.
    """
    config = load_json("../logs/logging.json")
    if full_format:
        config["formatters"]["console"] = dict(config["formatters"]["file"])
        config["handlers"]["console"]["level"] = logging.DEBUG
    elif log_level:
        config["handlers"]["console"]["level"] = log_level

    logging.config.dictConfig(config)
    logging.captureWarnings(True)
