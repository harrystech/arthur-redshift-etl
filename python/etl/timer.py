"""
Timer class for when you need the time elapsed in seconds.
"""

import datetime


def utc_now() -> datetime.datetime:
    """
    Return the current time for timezone UTC.

    Unlike datetime.utcnow(), this timestamp is timezone-aware.
    """
    return datetime.datetime.now(datetime.timezone.utc)


def elapsed_seconds(start: datetime.datetime, end: datetime.datetime = None) -> float:
    """
    Return number of seconds elapsed between start time and end time
    or between start time and now if end time is not provided
    """
    return ((end or utc_now()) - start).total_seconds()


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
        # Breaking with tradition, the timer instances return the elapsed time, not a description of the instance.
        return "%.2fs" % self.elapsed

    @property
    def elapsed(self):
        """Return elapsed time in seconds (wall clock time, between start and end of context)"""
        return elapsed_seconds(self._start, self._end)
