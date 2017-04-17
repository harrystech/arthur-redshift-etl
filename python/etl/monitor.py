"""
Monitoring (and logging) for ETL steps.

This module provides a context for the ETL that allows to monitor
the start time of an ETL step along with its successful or
unsuccessful completion.  Events for start, finish or failure
may be emitted to a persistence layer.
"""

import logging
import os
import random
import re
import threading
import time
import traceback
import uuid
from contextlib import closing
from copy import deepcopy
from decimal import Decimal
from typing import List

import boto3
import botocore.exceptions
import simplejson as json

import etl.config.env
import etl.pg
from etl.json_encoder import FancyJsonEncoder
from etl.timer import utc_now, elapsed_seconds


def trace_key():
    """
    Return a "trace key" suitable to track program execution.  It's most likely unique between invocations.
    """
    # We will never make a 32-bit operating system.
    return uuid.uuid4().hex[:16].upper()


class MetaMonitor(type):
    """
    Metaclass to implement read-only attributes of our ETL's Monitor

    If you need to find out the current trace key, call Monitor.etl_id.
    If you want to know the "environment" (selected by using --prefix or the user's login),
    then use Monitor.environment.
    If you want to know the runtime environment (EMR, instance, step), use Monitor.cluster_info.

    Behind the scenes, some properties actually do a lazy evaluation.
    """
    @property
    def etl_id(cls):
        if cls._trace_key is None:
            cls._trace_key = trace_key()
        return cls._trace_key

    @property
    def environment(cls):
        if cls._environment is None:
            raise ValueError("Value of 'environment' is None")
        return cls._environment

    @environment.setter
    def environment(cls, value):
        cls._environment = value

    @property
    def dynamo_sanitized_environment(cls):
        """
        Access the environment, replacing any unpermitted characters with '-'
        DynamoDB tables must match this pattern: [a-zA-Z0-9_.-]+
        """
        pat = re.compile('[a-zA-Z0-9_.-]+')
        return '-'.join(pat.findall(cls.environment))

    @property
    def cluster_info(cls):
        if cls._cluster_info is None:
            job_flow = '/mnt/var/lib/info/job-flow.json'
            if os.path.exists(job_flow):
                with open(job_flow) as f:
                    data = json.load(f)
                cluster_info = {
                    'cluster_id': data['jobFlowId'],
                    'instance_id': data['masterInstanceId']
                }
                parent_dir, current_dir = os.path.split(os.getcwd())
                if parent_dir == "/mnt/var/lib/hadoop/steps":
                    cluster_info["step_id"] = current_dir
            else:
                cluster_info = {}
            cls._cluster_info = cluster_info
        return cls._cluster_info


class Monitor(metaclass=MetaMonitor):
    """
    Context manager to monitor ETL steps for some target table


    Monitor instances have these properties which will be stored in the event payload:
        environment: a description of the source folder (aka prefix)
        etl_id: a UUID for each ETL run (All monitors of the same ETL run with the same 'etl_id'.)
        target: name of table or view in the data warehouse
        step: command that is running, like 'dump', or 'load'

    The payloads will have at least the properties of the Monitor instance and:
        event: one of 'start', 'finish', 'fail'
        timestamp: UTC timestamp

    In case of errors, they are added as an array 'errors'.  It is also possible to send
    some extra information into monitor payloads.  Anything extra must be of type list,
    dict, str, or int (or bad things will happen).

    Example usage of attributes:
    >>> id_ = Monitor.etl_id
    >>> isinstance(id_, str)
    True
    >>> Monitor.etl_id == id_
    True
    >>> Monitor.environment
    Traceback (most recent call last):
        ...
    ValueError: Value of 'environment' is None
    >>> Monitor.environment = 'saturn'
    >>> Monitor.environment
    'saturn'

    Example use of a monitor instance (with dry_run=True to avoid persistence calls during testing):
    >>> m = Monitor('schema.table', 'frobnicate', dry_run=True)
    >>> payload = MonitorPayload(m, 'test', utc_now())
    >>> payload.step
    'frobnicate'
    >>> payload.event
    'test'

    Normally, you would leave the creation of the payload to the context manager:
    >>> with Monitor('schema.table', 'frobnicate', dry_run=True):
    ...     pass
    """
    # See MetaMonitor class for getters and setters
    _trace_key = None
    _environment = None
    _cluster_info = None

    def __init__(self, target: str, step: str, dry_run: bool=False, **kwargs) -> None:
        self._logger = logging.getLogger(__name__)
        self._monitor_id = trace_key()
        self._target = target
        self._step = step
        self._dry_run = dry_run
        # Create a deep copy so that changes that the caller might make later do not alter our payload
        self._extra = deepcopy(dict(**kwargs))
        self._index = self._extra.get("index")

    # Read-only properties (in order of cardinality)

    @property
    def environment(self):
        return Monitor.environment

    @property
    def cluster_info(self):
        return Monitor.cluster_info

    @property
    def etl_id(self):
        return Monitor.etl_id

    @property
    def target(self):
        return self._target

    @property
    def step(self):
        return self._step

    @property
    def monitor_id(self):
        return self._monitor_id

    def __enter__(self):
        if self._index:
            self._logger.info("Starting %s step for '%s' (%d/%d)",
                              self.step, self.target, self._index["current"], self._index["final"])
        else:
            self._logger.info("Starting %s step for '%s'", self.step, self.target)
        self._start_time = utc_now()
        payload = MonitorPayload(self, 'start', self._start_time, extra=self._extra)
        payload.emit(dry_run=self._dry_run)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._end_time = utc_now()
        seconds = elapsed_seconds(self._start_time, self._end_time)
        if exc_type is None:
            self._logger.info("Finished %s step for '%s' (%0.2fs)", self._step, self._target, seconds)
            payload = MonitorPayload(self, 'finish', self._end_time, elapsed=seconds, extra=self._extra)
        else:
            self._logger.warning("Failed %s step for '%s' (%0.2fs)", self._step, self._target, seconds)
            payload = MonitorPayload(self, 'fail', self._end_time, extra=self._extra)
            payload.errors = [{'code': (exc_type.__module__ + '.' + exc_type.__qualname__).upper(),
                               'message': traceback.format_exception_only(exc_type, exc_value)[0].strip()}]
        payload.emit(dry_run=self._dry_run)


class MonitorPayload:
    """
    Simple class to encapsulate data for Monitor events which knows how to morph itself for JSON etc.
    You should consider all attributes to be read-only with the possible exception of 'errors'
    that may be set to a list of objects (in JSON-terminology) with 'code' and 'message' fields.
    """

    # Append instances with a 'store' method here (skipping writing a metaclass this time)
    dispatchers = []  # type: List[PayloadDispatcher]

    def __init__(self, monitor, event, timestamp, elapsed=None, extra=None):
        # Basic info
        self.environment = monitor.environment
        self.etl_id = monitor.etl_id
        self.target = monitor.target
        self.step = monitor.step
        self.monitor_id = monitor.monitor_id
        self.event = event
        self.timestamp = timestamp
        # Premium info (when available)
        self.cluster_info = monitor.cluster_info
        self.elapsed = elapsed
        self.extra = extra
        self.errors = None

    def emit(self, dry_run=False):
        logger = logging.getLogger(__name__)
        payload = vars(self)
        # Delete entries that are often not present:
        for key in ['cluster_info', 'elapsed', 'extra', 'errors']:
            if not payload[key]:
                del payload[key]

        compact_text = json.dumps(payload, sort_keys=True, separators=(',', ':'), cls=FancyJsonEncoder)
        if dry_run:
            logger.debug("Dry-run: payload = %s", compact_text)
        else:
            logger.debug("Monitor payload = %s", compact_text)
            for d in MonitorPayload.dispatchers:
                d.store(payload)


class PayloadDispatcher:

    def store(self, payload):
        """
        Send payload to persistence layer
        """
        raise NotImplementedError("PayloadDispatcher failed to implement store method")


class DynamoDBStorage(PayloadDispatcher):
    """
    Store ETL events in a DynamoDB table.

    Note the table is created if it doesn't already exist when class is instantiated.
    """

    def __init__(self, table_name, capacity, region_name):
        self.table_name = table_name
        self.capacity = capacity
        self.region_name = region_name
        # Avoid default sessions and have one table reference per thread
        self._thread_local_table = threading.local()

    def _get_table(self):
        """
        Fetch table or create it (within a new session)
        """
        logger = logging.getLogger(__name__)
        session = boto3.session.Session(region_name=self.region_name)
        dynamodb = session.resource('dynamodb')
        try:
            table = dynamodb.Table(self.table_name)
            status = table.table_status
            logger.info("Found existing events table '%s' in DynamoDB (status: %s)", self.table_name, status)
        except botocore.exceptions.ClientError as exc:
            # Check whether this is just a ResourceNotFoundException (sadly a 400, not a 404)
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 400:
                raise
            # Nullify assignment and start over
            table = None
            status = None
        # TODO Should we readjust the capacity if a new number is passed in?
        if table is None:
            logger.info("Creating DynamoDB table: '%s'", self.table_name)
            table = dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {'AttributeName': 'target', 'KeyType': 'HASH'},
                    {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'target', 'AttributeType': 'S'},
                    {'AttributeName': 'timestamp', 'AttributeType': 'N'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': self.capacity, 'WriteCapacityUnits': self.capacity}
            )
            status = table.table_status
        if status != "ACTIVE":
            logger.info("Waiting for events table '%s' to become active", self.table_name)
            table.wait_until_exists()
            logger.debug("Finished creating or updating events table '%s' (arn=%s)", self.table_name, table.table_arn)
        return table

    def store(self, payload: dict, _retry: bool=True):
        """
        Actually send the payload to the DynamoDB table.
        If this is the first call at all, then get a reference to the table,
        or even create the table as necessary.
        This method will try to store the payload a second time if there's an
        error in the first attempt.
        """
        try:
            table = getattr(self._thread_local_table, 'table', None)
            if not table:
                table = self._get_table()
                setattr(self._thread_local_table, 'table', table)
            item = dict(payload)
            # Cast timestamp (and elapsed seconds) into Decimal since DynamoDB cannot handle float.
            # But decimals maybe finicky when instantiated from float so we make sure to fix the number of decimals.
            item["timestamp"] = Decimal("%.6f" % item['timestamp'].timestamp())
            if "elapsed" in item:
                item["elapsed"] = Decimal("%.6f" % item['elapsed'])
            table.put_item(Item=item)
        except botocore.exceptions.ClientError:
            # Something bad happened while talking to the service ... just try one more time
            if _retry:
                logger = logging.getLogger(__name__)
                logger.exception("Trying to store payload a second time after this mishap:")
                delay = random.uniform(3, 10)
                logger.debug("Snoozing for %.1fs", delay)
                time.sleep(delay)
                setattr(self._thread_local_table, 'table', None)
                self.store(payload, _retry=False)
            else:
                raise


class RelationalStorage(PayloadDispatcher):
    """
    Store ETL events in a PostgreSQL table.

    Note the table is created if it doesn't already exist when class is instantiated.
    """

    def __init__(self, table_name, write_access):
        logger = logging.getLogger(__name__)
        self.table_name = table_name
        self.dsn = etl.config.env.get(write_access)
        logger.info("Creating table '%s' (unless it already exists)", table_name)
        with closing(etl.pg.connection(self.dsn)) as conn:
            etl.pg.execute(conn, """
                CREATE TABLE IF NOT EXISTS "{0.table_name}" (
                    environment CHARACTER VARYING(255),
                    etl_id      CHARACTER VARYING(255) NOT NULL,
                    target      CHARACTER VARYING(255) NOT NULL,
                    step        CHARACTER VARYING(255) NOT NULL,
                    monitor_id  CHARACTER VARYING(16) NOT NULL,
                    event       CHARACTER VARYING(255) NOT NULL,
                    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    payload     JSONB NOT NULL
                )""".format(self))
            conn.commit()

    def store(self, payload):
        data = {key: payload[key] for key in ["environment", "etl_id", "target", "step", "monitor_id", "event"]}
        data["timestamp"] = payload["timestamp"].isoformat(' ')
        data["payload"] = json.dumps(payload, sort_keys=True, cls=FancyJsonEncoder)

        with closing(etl.pg.connection(self.dsn)) as conn:
            with conn.cursor() as cursor:
                quoted_column_names = ", ".join('"{}"'.format(column) for column in data)
                column_values = tuple(data.values())
                cursor.execute('INSERT INTO "{0.table_name}" ({}) VALUES %s'.format(self, quoted_column_names),
                               (column_values,))


class InsertTraceKey(logging.Filter):
    """
    Called as a logging filter, insert the ETL id into the logging record for the log's trace key.
    """
    def filter(self, record):
        record.trace_key = Monitor.etl_id
        return True


def set_environment(environment, dynamodb_settings, postgresql_settings):
    Monitor.environment = environment
    if dynamodb_settings:
        ddb = DynamoDBStorage(dynamodb_settings["table_prefix"] + '-' + Monitor.dynamo_sanitized_environment,
                              dynamodb_settings["capacity"],
                              dynamodb_settings["region"])
        MonitorPayload.dispatchers.append(ddb)
    if postgresql_settings:
        rel = RelationalStorage(postgresql_settings["table_prefix"] + '_' + environment,
                                postgresql_settings["write_access"])
        MonitorPayload.dispatchers.append(rel)


def query_for(target_list, etl_id=None):
    logger = logging.getLogger(__name__)
    logger.info("Querying for: etl_id=%s target=%s", etl_id, target_list)
    # Left for the reader as an exercise.
