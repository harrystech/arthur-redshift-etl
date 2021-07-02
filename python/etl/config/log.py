import logging
import logging.config
import os
import sys

import etl.monitor
from etl.config import get_python_info, get_release_info, load_json, package_version

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def configure_logging(full_format: bool = False, log_level: str = None) -> None:
    """
    Set up logging to go to console and application log file.

    If full_format is True, then use the terribly verbose format of
    the application log file also for the console.  And log at the DEBUG level.
    Otherwise, you can choose the log level by passing one in.
    """
    config = load_json("logging.json")
    if full_format:
        config["formatters"]["console"] = dict(config["formatters"]["file"])
        config["handlers"]["console"]["level"] = logging.DEBUG
    elif log_level:
        config["handlers"]["console"]["level"] = log_level
    logging.config.dictConfig(config)
    logging.captureWarnings(True)
    logger.info("Starting log for %s with ETL ID %s", package_version(), etl.monitor.Monitor.etl_id)
    logger.info('Command line: "%s"', " ".join(sys.argv))
    logger.debug("Current working directory: '%s'", os.getcwd())
    logger.info(get_release_info())
    logger.debug(get_python_info())
