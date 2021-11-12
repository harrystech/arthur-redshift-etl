import datetime
import json
import logging
import logging.config

import boto3
import watchtower

import etl.monitor
from etl.config import get_config_value
from etl.logs.formatter import JsonFormatter

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def add_cloudwatch_logging(prefix: str) -> None:
    """
    Add logging to CloudWatch by adding another handler and formatter to the log stream.

    Args:
        prefix: Top-level group of CloudWatch stream.
    """
    session = boto3.session.Session()
    log_group = get_config_value("arthur_settings.logging.cloudwatch.log_group")
    now = datetime.datetime.utcnow()
    stream_name = f"{prefix}/{now.year}/{now.month}/{now.day}/{etl.monitor.Monitor.etl_id}"

    logger.info(f"Starting logging to CloudWatch stream '{log_group}/{stream_name}'")
    handler = watchtower.CloudWatchLogHandler(
        boto3_session=session,
        log_group=log_group,
        log_group_retention_days=180,
        send_interval=10,
        stream_name=stream_name,
    )

    log_level = get_config_value("arthur_settings.logging.cloudwatch.log_level")
    handler.setLevel(log_level)
    # The extra "str()" gets around the meta class approach to store the etl_id.
    handler.setFormatter(JsonFormatter(prefix, str(etl.monitor.Monitor.etl_id)))

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)


def tail(prefix: str, start_time: datetime.datetime) -> None:
    """
    Fetch log lines from CloudWatch, filtering for `prefix` as the environment.

    Args:
        prefix: Top-level group of CloudWatch stream.
        start_time: How far to go back when loading log lines.
    """
    client = boto3.client("logs")
    log_group = get_config_value("arthur_settings.logging.cloudwatch.log_group")
    logger.info(f"Searching log streams '{log_group}/{prefix}/*' (starting at '{start_time})'")

    paginator = client.get_paginator("filter_log_events")
    response_iterator = paginator.paginate(
        logGroupName=log_group,
        logStreamNamePrefix=prefix,
        startTime=int(start_time.timestamp() * 1000.0),
    )
    for response in response_iterator:
        for event in response["events"]:
            stream_name = event["logStreamName"]
            message = json.loads(event["message"])
            print(f"{stream_name} {message['gmtime']} {message['log_level']} {message['message']}")
            if "metrics" in message:
                print(f"{stream_name} {message['gmtime']} (metrics) {message['metrics']}")
