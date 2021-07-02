import datetime
import logging
import logging.config

import boto3
import watchtower

import etl.monitor
from etl.config import get_config_value, load_json

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


def configure_cloudwatch_logging(prefix):
    session = boto3.session.Session()
    log_group = get_config_value("arthur_settings.logging.cloudwatch.log_group")
    now = datetime.datetime.utcnow()
    stream_name = f"{now.year}/{now.month}/{now.day}/{prefix}/{etl.monitor.Monitor.etl_id}"

    handler = watchtower.CloudWatchLogHandler(
        boto3_session=session,
        log_group=log_group,
        log_group_retention_days=180,
        send_interval=10,
        stream_name=stream_name,
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
