"""
Monitoring (and logging) for ETL steps.

This module provides a context for the ETL that allows to monitor
the start time of an ETL step along with its successful or
unsuccessful completion.  Events for start, finish or failure
may be emitted to a persistence layer.
"""

import http.server
import itertools
import logging
import os
import queue
import random
import socketserver
import sys
import threading
import time
import traceback
import urllib.parse
import uuid
from calendar import timegm
from collections import Counter, OrderedDict
from copy import deepcopy
from datetime import datetime, timedelta
from decimal import Decimal
from http import HTTPStatus
from operator import itemgetter
from typing import Dict, Iterable, List, Optional, Union

import boto3
import botocore.exceptions
import funcy as fy
import simplejson as json
from boto3.dynamodb.types import TypeDeserializer
from tqdm import tqdm

import etl.assets
import etl.config
import etl.text
from etl.errors import ETLRuntimeError
from etl.json_encoder import FancyJsonEncoder
from etl.timer import Timer, elapsed_seconds, utc_now

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


STEP_START = "start"
STEP_FINISH = "finish"
STEP_FAIL = "fail"
_DUMMY_TARGET = "#.dummy"


def trace_key():
    """
    Return a "trace key" suitable to track program execution.

    It's most likely unique between invocations.
    """
    # We will never make a 32-bit operating system.
    return uuid.uuid4().hex[:16].upper()


class MetaMonitor(type):
    """
    Metaclass to implement read-only attributes of our ETL's Monitor.

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
            raise ValueError("value of 'environment' is None")
        return cls._environment

    @environment.setter
    def environment(cls, value):
        cls._environment = value

    @property
    def cluster_info(cls):
        if cls._cluster_info is None:
            job_flow = "/mnt/var/lib/info/job-flow.json"
            if os.path.exists(job_flow):
                with open(job_flow) as f:
                    data = json.load(f)
                cluster_info = {"cluster_id": data["jobFlowId"], "instance_id": data["masterInstanceId"]}
                parent_dir, current_dir = os.path.split(os.getcwd())
                if parent_dir == "/mnt/var/lib/hadoop/steps":
                    cluster_info["step_id"] = current_dir
            else:
                cluster_info = {}
            cls._cluster_info = cluster_info
        return cls._cluster_info


class Monitor(metaclass=MetaMonitor):
    """
    Context manager to monitor ETL steps for some target table.

    Monitor instances have these properties which will be stored in the event payload:
        environment: a description of the source folder (aka prefix)
        etl_id: a UUID for each ETL run (All monitors of the same ETL run with the same 'etl_id'.)
        target: name of table or view in the data warehouse
        step: command that is running, like 'dump', or 'load'

    The payloads will have at least the properties of the Monitor instance and:
        event: one of ('start', 'finish', 'fail')
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
    ValueError: value of 'environment' is None
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

    def __init__(self, target: str, step: str, dry_run: bool = False, **kwargs) -> None:
        self._monitor_id = trace_key()
        self._target = target
        self._step = step
        self._dry_run = dry_run
        # Create a deep copy so that changes made later by the caller don't alter our payload.
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
            logger.info(
                "Starting %s step for '%s' (%d/%d)",
                self.step,
                self.target,
                self._index["current"],
                self._index["final"],
            )
        else:
            logger.info("Starting %s step for '%s'", self.step, self.target)
        self._start_time = utc_now()
        payload = MonitorPayload(self, STEP_START, self._start_time, extra=self._extra)
        payload.emit(dry_run=self._dry_run)
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._end_time = utc_now()
        seconds = elapsed_seconds(self._start_time, self._end_time)
        if exc_type is None:
            event = STEP_FINISH
            errors = None
            logger.info("Finished %s step for '%s' (%0.2fs)", self._step, self._target, seconds)
        else:
            event = STEP_FAIL
            errors = [
                {
                    "code": (exc_type.__module__ + "." + exc_type.__qualname__).upper(),
                    "message": traceback.format_exception_only(exc_type, exc_value)[0].strip(),
                }
            ]
            logger.warning("Failed %s step for '%s' (%0.2fs)", self._step, self._target, seconds)

        payload = MonitorPayload(self, event, self._end_time, elapsed=seconds, errors=errors, extra=self._extra)
        payload.emit(dry_run=self._dry_run)

    def add_extra(self, key, value):
        if key in self._extra:
            raise KeyError("duplicate key in 'extra' payload")
        self._extra[key] = value

    @classmethod
    def marker_payload(cls, step: str):
        monitor = cls(_DUMMY_TARGET, step)
        return MonitorPayload(monitor, STEP_FINISH, utc_now(), elapsed=0, extra={"is_marker": True})


class PayloadDispatcher:
    def store(self, payload):
        """Send payload to persistence layer."""
        raise NotImplementedError("PayloadDispatcher failed to implement store method")


class MonitorPayload:
    """
    Simple class to encapsulate data for Monitor events which knows how to morph into JSON etc.

    You should consider all attributes to be read-only with the possible exception of 'errors'
    that may be set to a list of objects (in JSON-terminology) with 'code' and 'message' fields.
    (Which is to say: do not modify the payload object!)
    """

    # Append instances with a 'store' method here (skipping writing a metaclass this time)
    dispatchers: List[PayloadDispatcher] = []

    def __init__(self, monitor, event, timestamp, elapsed=None, errors=None, extra=None):
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
        self.errors = errors
        self.extra = extra

    def emit(self, dry_run=False):
        payload = vars(self)
        # Delete entries that are often not present:
        for key in ["cluster_info", "elapsed", "extra", "errors"]:
            if not payload[key]:
                del payload[key]

        compact_text = json.dumps(payload, sort_keys=True, separators=(",", ":"), cls=FancyJsonEncoder)
        if dry_run:
            logger.debug("Dry-run: payload = %s", compact_text)
        else:
            logger.debug("Monitor payload = %s", compact_text)
            for d in MonitorPayload.dispatchers:
                d.store(payload)


class DynamoDBStorage(PayloadDispatcher):
    """
    Store ETL events in a DynamoDB table.

    Note the table is created if it doesn't already exist when a payload needs to be stored.
    """

    @staticmethod
    def factory() -> "DynamoDBStorage":
        table_name = "{}-{}".format(etl.config.get_config_value("resource_prefix"), "events")
        return DynamoDBStorage(
            table_name,
            etl.config.get_config_value("etl_events.read_capacity"),
            etl.config.get_config_value("etl_events.write_capacity"),
            etl.config.get_config_value("resources.VPC.region"),
        )

    def __init__(self, table_name, read_capacity, write_capacity, region_name):
        self.table_name = table_name
        self.initial_read_capacity = read_capacity
        self.initial_write_capacity = write_capacity
        self.region_name = region_name
        # Avoid default sessions and have one table reference per thread
        self._thread_local_table = threading.local()

    def get_table(self, create_if_not_exists=True):
        """Get table reference from DynamoDB or create it (within a new session)."""
        session = boto3.session.Session(region_name=self.region_name)
        dynamodb = session.resource("dynamodb")
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
        if not (status == "ACTIVE" or create_if_not_exists):
            raise ETLRuntimeError("DynamoDB table '%s' does not exist or is not active" % self.table_name)
        if table is None:
            logger.info("Creating DynamoDB table: '%s'", self.table_name)
            table = dynamodb.create_table(
                TableName=self.table_name,
                KeySchema=[
                    {"AttributeName": "target", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "target", "AttributeType": "S"},
                    {"AttributeName": "timestamp", "AttributeType": "N"},
                ],
                ProvisionedThroughput={
                    "ReadCapacityUnits": self.initial_read_capacity,
                    "WriteCapacityUnits": self.initial_write_capacity,
                },
            )
            status = table.table_status
        if status != "ACTIVE":
            logger.info("Waiting for events table '%s' to become active", self.table_name)
            table.wait_until_exists()
            logger.debug("Finished creating or updating events table '%s' (arn=%s)", self.table_name, table.table_arn)
        return table

    def store(self, payload: dict, _retry: bool = True):
        """
        Actually send the payload to the DynamoDB table.

        If this is the first call at all, then get a reference to the table,
        or even create the table as necessary.
        This method will try to store the payload a second time if there's an
        error in the first attempt.
        """
        try:
            table = getattr(self._thread_local_table, "table", None)
            if not table:
                table = self.get_table()
                self._thread_local_table.table = table
            item = dict(payload)
            # Cast timestamp (and elapsed seconds) into Decimal since DynamoDB cannot handle float.
            # But decimals maybe finicky when instantiated from float so we make sure to fix the
            # number of decimals.
            item["timestamp"] = Decimal("%.6f" % item["timestamp"].timestamp())
            if "elapsed" in item:
                item["elapsed"] = Decimal("%.6f" % item["elapsed"])
            table.put_item(Item=item)
        except botocore.exceptions.ClientError:
            # Something bad happened while talking to the service ... just try one more time
            if _retry:
                logger.warning("Trying to store payload a second time after this mishap:", exc_info=True)
                self._thread_local_table.table = None
                delay = random.uniform(3, 10)
                logger.debug("Snoozing for %.1fs", delay)
                time.sleep(delay)
                self.store(payload, _retry=False)
            else:
                raise


class _ThreadingSimpleServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    pass


class MemoryStorage(PayloadDispatcher):
    """
    Store ETL events in memory and make the events accessible via HTTP.

    When the ETL is running for extract, load, or unload, connect to port 8086.

    When the ETL is running on a host other than your local computer, say in EC2, then use
    port forwarding, to send requests from your host to an address seen on the other host:
        ssh -L 8086:localhost:8086 <hostname>

    The output should pass validator at https://validator.w3.org/#validate_by_input+with_options
    """

    SERVER_HOST = ""  # meaning: all that we can bind to locally
    SERVER_PORT = 8086

    def __init__(self):
        self.queue = queue.Queue()
        self.events = OrderedDict()
        self.start_server()

    def store(self, payload: dict):
        self.queue.put(payload)

    def _drain_queue(self):
        try:
            while True:
                payload = self.queue.get_nowait()
                if not payload.get("extra", {}).get("is_marker", False):
                    # Overwrite earlier events by later ones
                    key = payload["target"], payload["step"]
                    self.events[key] = payload
        except queue.Empty:
            pass

    def get_indices(self):
        self._drain_queue()
        indices = {}
        counter = Counter()
        for payload in self.events.values():
            index = dict(payload.get("extra", {}).get("index", {}))
            name = index.setdefault("name", "N/A")
            if name not in indices:
                indices[name] = index
            elif index["current"] > indices[name]["current"]:
                indices[name].update(index)
            if payload["event"] != STEP_START:
                counter[name] += 1
            indices[name]["counter"] = counter[name]
        indices_as_list = [indices[name] for name in sorted(indices)]
        return etl.assets.Content(json=indices_as_list)

    def get_events(self, event_id: Optional[str]):
        self._drain_queue()
        if event_id is None:
            events_as_list = sorted(
                (self.events[key] for key in self.events),
                key=lambda p: (2 if p["event"] == STEP_START else 1, p["timestamp"]),
                reverse=True,
            )
        else:
            events_as_list = [event for event in self.events.values() if event["monitor_id"] == event_id]
        return etl.assets.Content(json=events_as_list)

    def create_handler(self):
        """Return a handler that serves our storage content, used as factory method."""
        storage = self
        http_logger = logging.getLogger("arthur_http")

        class MonitorHTTPHandler(http.server.BaseHTTPRequestHandler):

            server_version = "MonitorHTTPServer/1.0"
            log_error = http_logger.error
            log_message = http_logger.info

            def do_GET(self):
                """
                Serve a GET (or HEAD) request.

                We serve assets or JSON via the API.
                If the command is HEAD (and not GET), only the header is sent. Duh.
                """
                parts = urllib.parse.urlparse(self.path.rstrip("/"))
                path = (parts.path or "/index.html").lstrip("/")
                if path == "api/etl-id":
                    result = etl.assets.Content(json={"id": Monitor.etl_id})
                elif path == "api/indices":
                    result = storage.get_indices()
                elif path.startswith("api/events"):
                    segment = path.replace("api/events", "").strip("/")
                    result = storage.get_events(segment or None)
                elif path == "api/command-line":
                    result = etl.assets.Content(json={"args": " ".join(sys.argv)})
                elif etl.assets.asset_exists(path):
                    result = etl.assets.get_asset(path)
                else:
                    # self.send_response(HTTPStatus.NOT_FOUND)
                    self.send_response(HTTPStatus.MOVED_PERMANENTLY)
                    new_parts = (parts.scheme, parts.netloc, "/", None, None)
                    new_url = urllib.parse.urlunsplit(new_parts)
                    self.send_header("Location", new_url)
                    self.end_headers()
                    return

                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", result.content_type)
                self.send_header("Content-Length", result.content_length)
                if result.content_encoding is not None:
                    self.send_header("Content-Encoding", result.content_encoding)
                self.send_header("Last-Modified", result.last_modified)
                if result.cache_control is not None:
                    self.send_header("Cache-Control", result.cache_control)
                self.end_headers()
                if self.command == "GET":
                    self.wfile.write(result.content)

            do_HEAD = do_GET

        return MonitorHTTPHandler

    def start_server(self):
        """Start background daemon to serve our events."""
        handler_class = self.create_handler()

        class BackgroundServer(threading.Thread):
            def run(self):
                logger.info("Starting background server for monitor on port %d", MemoryStorage.SERVER_PORT)
                try:
                    httpd = _ThreadingSimpleServer(
                        (MemoryStorage.SERVER_HOST, MemoryStorage.SERVER_PORT), handler_class
                    )
                    httpd.serve_forever()
                except Exception as exc:
                    logger.info("Background server stopped: %s", str(exc))

        try:
            thread = BackgroundServer(daemon=True)
            thread.start()
        except RuntimeError:
            logger.warning("Failed to start monitor server:", exc_info=True)


class InsertTraceKey(logging.Filter):
    """Called as a logging filter: insert the ETL id as the trace key into the logging record."""

    def filter(self, record):
        record.trace_key = Monitor.etl_id
        return True


def start_monitors(environment):
    Monitor.environment = environment
    memory = MemoryStorage()
    MonitorPayload.dispatchers.append(memory)

    if etl.config.get_config_value("etl_events.enabled"):
        ddb = DynamoDBStorage.factory()
        MonitorPayload.dispatchers.append(ddb)
    else:
        logger.warning("Writing events to a DynamoDB table is disabled in settings.")


def _format_output_column(key: str, value: str) -> str:
    if value is None:
        return "---"
    elif key == "timestamp":
        # Make timestamp readable by turning epoch seconds into a date.
        return datetime.utcfromtimestamp(float(value)).replace(microsecond=0).isoformat()
    elif key == "elapsed":
        # Reduce number of decimals to 2.
        return "{:6.2f}".format(float(value))
    elif key == "rowcount":
        return "{:9d}".format(int(value))
    else:
        return value


def _query_for_etls(step=None, hours_ago=0, days_ago=0) -> List[dict]:
    """Search for ETLs by looking for the "marker" event at the start of an ETL command."""
    start_time = datetime.utcnow() - timedelta(days=days_ago, hours=hours_ago)
    epoch_seconds = timegm(start_time.utctimetuple())
    attribute_values = {
        ":marker": _DUMMY_TARGET,
        ":epoch_seconds": epoch_seconds,
        ":finish_event": STEP_FINISH,
    }
    if step is not None:
        attribute_values[":step"] = step
    filter_exp = "event = :finish_event"
    if step is not None:
        filter_exp += " and step = :step"

    ddb = DynamoDBStorage.factory()
    table = ddb.get_table(create_if_not_exists=False)
    response = table.query(
        ConsistentRead=True,
        ExpressionAttributeNames={"#timestamp": "timestamp"},  # "timestamp" is a reserved word. You're welcome.
        ExpressionAttributeValues=attribute_values,
        KeyConditionExpression="target = :marker and #timestamp > :epoch_seconds",
        FilterExpression=filter_exp,
        ProjectionExpression="etl_id, step, #timestamp",
        ReturnConsumedCapacity="TOTAL",
    )
    if "LastEvaluatedKey" in response:
        logger.warning("This is is a partial result! Last evaluated key: '%s'", response["LastEvaluatedKey"])

    logger.info(
        "Query result: count = %d, scanned count = %d, consumed capacity = %f",
        response["Count"],
        response["ScannedCount"],
        response["ConsumedCapacity"]["CapacityUnits"],
    )
    return response["Items"]


def query_for_etl_ids(hours_ago=0, days_ago=0) -> None:
    """Show recent ETLs with their step and execution start."""
    etl_info = _query_for_etls(hours_ago=hours_ago, days_ago=days_ago)
    keys = ["etl_id", "step", "timestamp"]
    rows = [[_format_output_column(key, info[key]) for key in keys] for info in etl_info]
    rows.sort(key=itemgetter(keys.index("timestamp")))
    print(etl.text.format_lines(rows, header_row=keys))


def scan_etl_events(etl_id, selected_columns: Optional[Iterable[str]] = None) -> None:
    """
    Scan for all events belonging to a specific ETL.

    If a list of columns is provided, then the output is limited to those columns.
    But note that the target (schema.table) and the event are always present.
    """
    ddb = DynamoDBStorage.factory()
    table = ddb.get_table(create_if_not_exists=False)
    available_columns = ["target", "step", "event", "timestamp", "elapsed", "rowcount"]
    if selected_columns is None:
        selected_columns = available_columns
    # We will always select "target" and "event" to have a meaningful output.
    columns = list(fy.filter(frozenset(selected_columns).union(["target", "event"]), available_columns))
    keys = ["extra.rowcount" if column == "rowcount" else column for column in columns]

    # We need to scan here since the events are stored by "target" and not by "etl_id".
    # TODO Try to find all the "known" relations and query on them with a filter on the etl_id.
    client = boto3.client("dynamodb")
    paginator = client.get_paginator("scan")
    response_iterator = paginator.paginate(
        TableName=table.name,
        ConsistentRead=False,
        ExpressionAttributeNames={"#timestamp": "timestamp"},
        ExpressionAttributeValues={
            ":etl_id": {"S": etl_id},
            ":marker": {"S": _DUMMY_TARGET},
            ":start_event": {"S": STEP_START},
        },
        FilterExpression="etl_id = :etl_id and target <> :marker and event <> :start_event",
        ProjectionExpression="target, step, event, #timestamp, elapsed, extra.rowcount",
        ReturnConsumedCapacity="TOTAL",
        # PaginationConfig={
        #     "PageSize": 100
        # }
    )
    logger.info("Scanning events table '%s' for elapsed times", table.name)
    consumed_capacity = 0.0
    scanned_count = 0
    rows: List[List[str]] = []
    deserialize = TypeDeserializer().deserialize

    for response in response_iterator:
        consumed_capacity += response["ConsumedCapacity"]["CapacityUnits"]
        scanned_count += response["ScannedCount"]
        # We need to turn something like "'event': {'S': 'finish'}" into "'event': 'finish'".
        deserialized = [{key: deserialize(value) for key, value in item.items()} for item in response["Items"]]
        # Lookup "elapsed" or "extra.rowcount" (the latter as ["extra", "rowcount"]).
        items = [{key: fy.get_in(item, key.split(".")) for key in keys} for item in deserialized]
        # Scope down to selected keys and format the columns.
        rows.extend([_format_output_column(key, item[key]) for key in keys] for item in items)

    logger.info("Scan result: scanned count = %d, consumed capacity = %f", scanned_count, consumed_capacity)
    if "timestamp" in keys:
        rows.sort(key=itemgetter(keys.index("timestamp")))
    else:
        rows.sort(key=itemgetter(keys.index("target")))
    print(etl.text.format_lines(rows, header_row=columns))


class EventsQuery:
    def __init__(self, step: Optional[str] = None) -> None:
        self._keys = ["target", "step", "event", "timestamp", "elapsed", "extra.rowcount"]
        values = {
            ":target": None,  # will be set when called
            ":epoch_seconds": None,  # will be set when called
            ":start_event": STEP_START,
        }
        # Only look for finish or fail events
        filter_exp = "event <> :start_event"
        if step is not None:
            values[":step"] = step
            filter_exp += " and step = :step"
        base_query = {
            "ConsistentRead": False,
            "ExpressionAttributeNames": {"#timestamp": "timestamp"},
            "ExpressionAttributeValues": values,
            "KeyConditionExpression": "target = :target and #timestamp > :epoch_seconds",
            "FilterExpression": filter_exp,
            "ProjectionExpression": "target, step, event, #timestamp, elapsed, extra.rowcount",
        }
        self._base_query = base_query

    @property
    def keys(self):
        return self._keys[:]

    def __call__(self, table, target, epoch_seconds):
        query = deepcopy(self._base_query)
        query["ExpressionAttributeValues"][":target"] = target
        query["ExpressionAttributeValues"][":epoch_seconds"] = epoch_seconds
        response = table.query(**query)
        events = [{key: fy.get_in(item, key.split(".")) for key in self.keys} for item in response["Items"]]
        # Return latest event or None
        if events:
            events.sort(key=itemgetter("timestamp"))
            return events[-1]
        return None


class BackgroundQueriesRunner(threading.Thread):
    """
    An instance of this thread will repeatedly try to run queries on a DynamoDB table.

    Every time a query returns a result, this result is sent to a queue and the query will no
    longer be tried.
    """

    def __init__(self, targets, query, consumer_queue, start_time, update_interval, idle_time_out, **kwargs) -> None:
        super().__init__(**kwargs)
        self.targets = list(targets)
        self.query = query
        self.queue = consumer_queue
        self.start_time = start_time
        self.update_interval = update_interval
        self.idle_time_out = idle_time_out

    def run(self):
        ddb = DynamoDBStorage.factory()
        table = ddb.get_table(create_if_not_exists=False)
        targets = self.targets
        start_time = self.start_time
        idle = Timer()
        while targets:
            logger.debug(
                "Waiting for events for %d target(s), start time = '%s'",
                len(targets),
                datetime.utcfromtimestamp(start_time).isoformat(),
            )
            new_start_time = datetime.utcnow() - timedelta(seconds=1)  # avoid rounding errors
            query_loop = Timer()
            retired = set()
            for target in targets:
                latest_event = self.query(table, target, start_time)
                if latest_event:
                    self.queue.put(latest_event)
                    retired.add(latest_event["target"])
            targets = [t for t in targets if t not in retired]
            start_time = timegm(new_start_time.utctimetuple())
            if self.update_interval is None or not targets:
                break
            if retired:
                idle = Timer()
            elif self.idle_time_out and idle.elapsed > self.idle_time_out:
                logger.info(
                    "Idle time-out: Waited for %d seconds but no events arrived, " "%d target(s) remaining",
                    self.idle_time_out,
                    len(targets),
                )
                break
            if query_loop.elapsed < self.update_interval:
                time.sleep(self.update_interval - query_loop.elapsed)
        logger.info("Found events for %d out of %d target(s)", len(self.targets) - len(targets), len(self.targets))
        self.queue.put(None)


def recently_extracted_targets(source_relations, start_time):
    """
    Query the events table for "extract" events on the provided source_relations after start_time.

    Waits for up to an hour, sleeping for 30s between checks.
    Return the set of targets (ie, relation.identifier or event["target"]) with successful extracts.
    """
    targets = [relation.identifier for relation in source_relations]

    query = EventsQuery("extract")
    consumer_queue = queue.Queue()  # type: ignore
    start_as_epoch = timegm(start_time.utctimetuple())
    timeout = 60 * 60
    extract_querying_thread = BackgroundQueriesRunner(
        targets, query, consumer_queue, start_as_epoch, update_interval=30, idle_time_out=timeout, daemon=True
    )
    extract_querying_thread.start()
    extracted_targets = set()

    while True:
        try:
            event = consumer_queue.get(timeout=timeout)
            if event is None:
                break
            if event["event"] == STEP_FINISH:
                extracted_targets.add(event["target"])
        except queue.Empty:
            break
    return extracted_targets


def summarize_events(relations, step: Optional[str] = None) -> None:
    """Summarize latest ETL step for the given relations by showing elapsed time and row count."""
    etl_info = _query_for_etls(step=step, days_ago=7)
    if not len(etl_info):
        logger.warning("Found no ETLs within the last 7 days")
        return
    latest_etl = sorted(etl_info, key=itemgetter("timestamp"))[-1]
    latest_start = latest_etl["timestamp"]
    logger.info("Latest ETL: %s", latest_etl)

    ddb = DynamoDBStorage.factory()
    table = ddb.get_table(create_if_not_exists=False)
    query = EventsQuery(step)

    events = []
    schema_events: Dict[str, Dict[str, Union[str, Decimal]]] = {}
    for relation in tqdm(desc="Querying for events", disable=None, iterable=relations, leave=False, unit="table"):
        event = query(table, relation.identifier, latest_start)
        if event:
            # Make the column for row counts easier to read by dropping "extra.".
            event["rowcount"] = event.pop("extra.rowcount")
            events.append(dict(event, kind=relation.kind))
            schema = relation.target_table_name.schema
            if schema not in schema_events:
                schema_events[schema] = {
                    "target": schema,
                    "kind": "---",
                    "step": event["step"],
                    "timestamp": Decimal(0),
                    "event": "complete",
                    "elapsed": Decimal(0),
                    "rowcount": Decimal(0),
                }
            if event["timestamp"] > schema_events[schema]["timestamp"]:
                schema_events[schema]["timestamp"] = event["timestamp"]
            schema_events[schema]["elapsed"] += event["elapsed"]
            schema_events[schema]["rowcount"] += event["rowcount"] if event["rowcount"] else 0

    # Add pseudo events to show schemas are done.
    events.extend(schema_events.values())

    keys = ["target", "kind", "step", "timestamp", "event", "elapsed", "rowcount"]
    rows = [[_format_output_column(key, info[key]) for key in keys] for info in events]
    rows.sort(key=itemgetter(keys.index("timestamp")))
    print(etl.text.format_lines(rows, header_row=keys))


def tail_events(relations, start_time, update_interval=None, idle_time_out=None, step: Optional[str] = None) -> None:
    """Tail the events table and show latest finish or fail events coming in."""
    targets = [relation.identifier for relation in relations]
    query = EventsQuery(step)
    consumer_queue = queue.Queue()  # type: ignore
    epoch_seconds = timegm(start_time.utctimetuple())

    thread = BackgroundQueriesRunner(
        targets, query, consumer_queue, epoch_seconds, update_interval, idle_time_out, daemon=True
    )
    thread.start()

    events = []
    n_printed = 0
    done = False
    while not done:
        progress = Timer()
        while progress.elapsed < 10:
            try:
                event = consumer_queue.get(timeout=10)
                if event is None:
                    done = True
                    break
                event["timestamp"] = datetime.utcfromtimestamp(event["timestamp"]).isoformat()  # timestamp to isoformat
                events.append(event)
            except queue.Empty:
                break
        # Keep printing tail of table that accumulates the events.
        if len(events) > n_printed:
            lines = etl.text.format_lines(
                [[event[header] for header in query.keys] for event in events], header_row=query.keys
            ).split("\n")
            if n_printed:
                print("\n".join(lines[n_printed + 2 : -1]))  # skip header and final "(x rows)" line
            else:
                print("\n".join(lines[:-1]))  # only skip the "(x rows)" line
            n_printed = len(lines) - 3  # header, separator, final = 3 extra rows
            if done:
                print(lines[-1])


def test_run():
    Monitor.environment = "test"  # type: ignore
    memory = MemoryStorage()
    MonitorPayload.dispatchers.append(memory)

    schema_names = ["auburn", "burgundy", "cardinal", "flame", "fuchsia"]
    table_names = ["apple", "banana", "cantaloupe", "durian", "fig"]
    index = {"current": 0, "final": len(schema_names) * len(table_names)}

    host = MemoryStorage.SERVER_HOST if MemoryStorage.SERVER_HOST else "localhost"
    print("Creating events ... follow along at http://{}:{}/".format(host, MemoryStorage.SERVER_PORT))

    with Monitor("color.fruit", "test", index={"current": 1, "final": 1, "name": "outer"}):
        for i, names in enumerate(itertools.product(schema_names, table_names)):
            try:
                with Monitor(".".join(names), "test", index=dict(index, current=i + 1)):
                    time.sleep(random.uniform(0.5, 2.0))
                    # Create an error on one "table" so that highlighting of errors can be tested:
                    if i == 9:
                        raise RuntimeError("An error occurred!")
            except RuntimeError:
                pass

    input("Press return (or Ctrl-c) to stop server\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_run()
