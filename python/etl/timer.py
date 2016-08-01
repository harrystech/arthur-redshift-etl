from datetime import datetime


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
        self._start = datetime.now()
        self._end = None

    def __enter__(self):
        """Set start time when entering context"""
        self._start = datetime.now()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Set end time when exiting context"""
        self._end = datetime.now()

    def __str__(self):
        return "%.2fs" % self.elapsed

    @property
    def elapsed(self):
        """Return elapsed time in seconds (wall clock time, between start and end of context)"""
        end = self._end or datetime.now()
        return (end - self._start).total_seconds()
