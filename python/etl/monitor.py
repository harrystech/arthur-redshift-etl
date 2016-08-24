"""
Monitoring (and logging) for ETL steps.

This module provides a context for the ETL that allows to monitor
the start time of an ETL step along with its successful or
unsuccessful completion.  Events for start, finish or failure
may be emitted to a persistence layer.
"""

import logging
import os
import uuid

import simplejson as json

import etl
from etl.timer import utc_now, elapsed_seconds


class Monitor:

    """
    Context manager to monitor ETL steps for some target table

    All monitors of the same ETL run will have the same 'etl_id'.
    """

    shared_trace_key = None
    aws_info = None

    def __init__(self: "Monitor", step: str, target: etl.TableName, **kwargs):
        self._logger = logging.getLogger(__name__)
        self._step = step
        self._target = target
        self._payload = dict(**kwargs)
        self._payload['step'] = step
        self._payload['etl_id'] = Monitor.shared_trace_key
        self._payload['target'] = target.identifier
        self._payload['environment'] = os.environ.get('ETL_ENVIRONMENT', 'local')
        if Monitor.aws_info:
            self._payload['aws'] = Monitor.aws_info

    def __enter__(self):
        self._logger.info("Starting %s step for '%s'", self._step, self._target.identifier)
        self._start_time = utc_now()
        self._payload['timestamp'] = self._start_time.timestamp()
        self._payload['event'] = 'start'
        self._logger.debug("Monitor payload = %s", json.dumps(self._payload, sort_keys=True))
        # XXX Emit event here.
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        seconds = elapsed_seconds(self._start_time)
        self._payload['timestamp'] = utc_now().timestamp()
        self._payload['elapsed'] = seconds if seconds > 1e-3 else .0

        if exc_type is None:
            self._logger.info("Finished %s step for '%s' (after %0.fs)", self._step, self._target.identifier, seconds)
            self._payload['event'] = 'finish'
        else:
            self._logger.warning("Failed %s step for '%s' (after %0.fs)", self._step, self._target.identifier, seconds)
            self._payload['event'] = 'fail'
            self._payload['errors'] = [{'code': str(type(exc_type)), 'message': "%s: %s" % (exc_type, exc_value)}]

        self._logger.debug("Monitor payload = %s", json.dumps(self._payload, sort_keys=True))
        # XXX Emit event here.

    @staticmethod
    def load_aws_info(filename):
        """
        Load instance ID and EMR ID from the local instance info and set the data for the Monitor class

        On EMR instances, the JSON-formatted file is in /mnt/var/lib/info/job-flow.json
        """
        if os.path.exists(filename):
            with open(filename) as f:
                data = json.load(f)
                Monitor.aws_info = {
                    'emr_id': data['jobFlowId'],
                    'instance_id': data['masterInstanceId']
                }


# Setup shared ETL id for all monitor events:
Monitor.shared_trace_key = uuid.uuid4().hex.upper()

# Add EMR and instance info
Monitor.load_aws_info('/mnt/var/lib/info/job-flow.json')
