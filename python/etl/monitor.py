"""
Monitoring (and logging) for ETL steps.

This module provides a context for the ETL that allows to monitor
the start time of an ETL step along with its successful or
unsuccessful completion.  Events for start, finish or failure
may be emitted to a persistence layer.
"""

from contextlib import closing
from copy import deepcopy
from decimal import Decimal
import logging
import os
import traceback
import uuid

import boto3
import botocore.exceptions
import simplejson as json

import etl.config
from etl.json_encoder import FancyJsonEncoder
from etl.timer import utc_now, elapsed_seconds
import etl.pg


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
            # We will never make a 32-bit operating system.
            cls._trace_key = uuid.uuid4().hex.upper()[:16]
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
        target: name of table or view in Redshift
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

    def __init__(self: "Monitor", target: str, step: str, dry_run: bool=False, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._target = target
        self._step = step
        self._dry_run = dry_run
        # Create a deep copy so that changes that the caller might make later do not alter our payload
        self._extra = deepcopy(dict(**kwargs))

    @property
    def environment(self):
        return Monitor.environment

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
    def cluster_info(self):
        return Monitor.cluster_info

    def __enter__(self):
        self._logger.info("Starting %s step for '%s'", self.step, self.target)
        self._start_time = utc_now()
        payload = MonitorPayload(self, 'start', self._start_time, extra=self._extra)
        payload.emit(dry_run=self._dry_run)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._end_time = utc_now()
        seconds = elapsed_seconds(self._start_time, self._end_time)
        if exc_type is None:
            self._logger.info("Finished %s step for '%s' (after %0.fs)", self._step, self._target, seconds)
            payload = MonitorPayload(self, 'finish', self._end_time, elapsed=seconds, extra=self._extra)
        else:
            self._logger.warning("Failed %s step for '%s' (after %0.fs)", self._step, self._target, seconds)
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
    persister = []

    def __init__(self, monitor, event, timestamp, elapsed=None, extra=None):
        # Basic info
        self.environment = monitor.environment
        self.etl_id = monitor.etl_id
        self.target = monitor.target
        self.step = monitor.step
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
            for p in MonitorPayload.persister:
                p.store(payload)


class DynamoDBStorage:
    """
    Store ETL events in a DynamoDB table.

    Note the table is created if it doesn't already exist when class is instantiated.
    """

    def __init__(self, table_name, capacity):
        self.table_name = table_name
        self.capacity = capacity
        self._table = None

    def set_table(self):
        """Fetch table or create it, then set reference"""
        logger = logging.getLogger(__name__)
        dynamodb = boto3.resource('dynamodb')
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
            logger.debug("Waiting for new events table '%s' to exist", self.table_name)
            table.wait_until_exists()
            logger.info("Finished creating events table '%s' (arn=%s)", self.table_name, table.table_arn)
        # TODO Should we readjust the capacity if a new number is passed in?
        self._table = table

    def store(self, payload):
        if not self._table:
            self.set_table()
        item = dict(payload)
        # Cast timestamp (and elpased seconds) into Decimal since DynamoDB cannot handle float.
        # But decimals maybe finicky when instantiated from float so we make sure to fix the number of decimals.
        item["timestamp"] = Decimal("%.6f" % item['timestamp'].timestamp())
        if "elapsed" in item:
            item["elapsed"] = Decimal("%.6f" % item['elapsed'])
        self._table.put_item(Item=item)


class RelationalStorage:
    """
    Store ETL events in a PostgreSQL table.

    Note the table is created if it doesn't already exist when class is instantiated.
    """

    def __init__(self, table_name, write_access):
        logger = logging.getLogger(__name__)
        self.table_name = table_name
        self.dsn = etl.config.env_value(write_access)
        logger.info("Creating table '%s' (unless it already exists)", table_name)
        with closing(etl.pg.connection(self.dsn)) as conn:
            etl.pg.execute(conn, """
                CREATE TABLE IF NOT EXISTS %s (
                    id          SERIAL PRIMARY KEY,
                    environment CHARACTER VARYING(255),
                    etl_id      CHARACTER VARYING(255) NOT NULL,
                    target      CHARACTER VARYING(255) NOT NULL,
                    step        CHARACTER VARYING(255) NOT NULL,
                    event       CHARACTER VARYING(255) NOT NULL,
                    "timestamp" TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    payload     JSONB NOT NULL)""" % self.table_name)
            conn.commit()

    def store(self, payload):
        data = {key: payload[key] for key in ["environment", "etl_id", "target", "step", "event"]}
        data["timestamp"] = payload["timestamp"].isoformat(' ')
        data["payload"] = json.dumps(payload, sort_keys=True, cls=FancyJsonEncoder)

        with closing(etl.pg.connection(self.dsn)) as conn:
            with conn:
                quoted_column_names = ", ".join('"{}"'.format(column) for column in data)
                etl.pg.execute(conn,
                               """INSERT INTO "%s" (%s) VALUES %%s""" % (self.table_name, quoted_column_names),
                               (tuple(data.values()),))


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
        ddb = DynamoDBStorage(dynamodb_settings["table_prefix"] + '-' + environment, dynamodb_settings["capacity"])
        MonitorPayload.persister.append(ddb)
    if postgresql_settings:
        rel = RelationalStorage(postgresql_settings["table_prefix"] + '_' + environment, postgresql_settings["write_access"])
        MonitorPayload.persister.append(rel)


def query_for(target_list, etl_id=None):
    logger = logging.getLogger(__name__)
    logger.info("Querying for: etl_id=%s target=%s", etl_id, target_list)
