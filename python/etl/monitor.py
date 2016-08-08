"""
Monitoring (and logging) for ETL steps.

This module provides a context for the ETL that allows to monitor
the start time of an ETL step along with its successful or
unsuccessful completion.  Events for start, finish or failure
may be emitted to a persistence layer.

There is also a much simpler Timer class for when you just
need the delta in seconds.
"""

import datetime
import logging
import uuid

import etl


def utc_now() -> datetime.datetime:
    """
    Return the current time for timezone UTC.

    Unlike datetime.utcnow(), this timestamp is timezone-aware.
    """
    return datetime.datetime.now(datetime.timezone.utc)


def elapsed_seconds(start: datetime.datetime, end: datetime=None) -> float:
    """
    Return number of seconds elapsed between start time and end time
    or between start time and now if end time is not provided
    """
    return ((end or utc_now()) - start).total_seconds()


class Monitor:

    """
    Context manager to monitor ETL steps for some target table

    All monitors of the same ETL run will have the same 'etl_id'.
    """

    shared_trace_key = None

    def __init__(self: "Monitor", step: str, target: etl.TableName, **kwargs):

        # XXX Think about race conditions?
        if Monitor.shared_trace_key is None:
            Monitor.shared_trace_key = uuid.uuid4().hex.upper()

        self._logger = logging.getLogger(__name__)
        self._step = step
        self._target = target
        self._payload = dict(**kwargs)
        self._payload['etl_id'] = Monitor.shared_trace_key
        self._payload['target'] = target.identifier

    def __enter__(self):
        self._logger.info("Starting %s step for '%s'", self._step, self._target.identifier)

        # XXX Emit event here instead of logging it!
        self._payload['timestamp'] = utc_now().timestamp()
        self._payload['event'] = 'start'
        self._logger.debug("Monitor payload: %s", self._payload)

        self._start_time = utc_now()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        seconds = elapsed_seconds(self._start_time)
        self._payload['timestamp'] = utc_now().timestamp()

        if exc_type is None:
            self._logger.info("Finished %s step for '%s' (after %0.fs)", self._step, self._target.identifier, seconds)
            # XXX Emit event here instead of logging it!
            self._payload['event'] = 'finish'
            self._logger.debug("Monitor payload: %s", self._payload)
        else:
            self._logger.warning("Failed %s step for '%s' (after %0.fs)", self._step, self._target.identifier, seconds)
            # XXX Emit event here instead of logging it!
            self._payload['event'] = 'fail'
            self._payload['errors'] = [{'code': str(type(exc_type)), 'message': "%s: %s" % (exc_type, exc_value)}]
            self._logger.debug("Monitor payload: %s", self._payload)


class Timer:

    """
    Context manager class to measure elapsed (wall clock) time in seconds

    >>> with Timer() as t:
    ...     pass
    ...
    >>> str(t)
    '0.00s'
    """

    def __init__(self):
        self._start = utc_now()
        self._end = None

    def __enter__(self):
        """Set start time when entering context"""
        self._start = utc_now()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Set end time when exiting context"""
        self._end = utc_now()

    def __str__(self):
        return "%.2fs" % self.elapsed

    @property
    def elapsed(self):
        """Return elapsed time in seconds (wall clock time, between start and end of context)"""
        return elapsed_seconds(self._start, self._end)
