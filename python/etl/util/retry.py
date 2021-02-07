"""Function to retry calls in case of transient errors."""

import logging
import time
from typing import Callable

from etl.errors import RetriesExhaustedError, TransientETLError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def call_with_retry(max_retries: int, func: Callable, *args, **kwargs) -> None:
    """
    Retry a function with a transient issue for upto max_retries times.

    The given func function is only retried if it throws a TransientETLError. Any other error is
    considered permanent, and therefore no retry attempt is made.

    This will sleep for 5 ^ attempt_number seconds between retries. The value of max
    retries should be equal or larger than 0. (See schema for settings.) If it is
    set to 0, then no retries are attempted and the function is called exactly once.
    """
    total_attempts = max_retries + 1
    for remaining_attempts in range(total_attempts, 0, -1):
        if total_attempts == 1:
            logger.debug("Starting one and only attempt")
        elif remaining_attempts == total_attempts:
            logger.debug("Starting on first of %d attempt(s)", total_attempts)
        else:
            logger.debug("There are now only %d of %d attempt(s) left", remaining_attempts, total_attempts)
        try:
            func(*args, **kwargs)
        except TransientETLError as exc:
            # Was this your last chance, Buster?
            if remaining_attempts == 1:
                raise RetriesExhaustedError("reached max number of retries ({:d})".format(max_retries)) from exc
            sleep_time = 5 ** (max_retries - remaining_attempts + 1)
            logger.warning(
                "Encountered the following error, but retrying %s more time(s) after %d second sleep: %s",
                remaining_attempts - 1,
                sleep_time,
                str(exc),
            )
            time.sleep(sleep_time)
        except Exception as exc:
            # We consider all other errors permanent and immediately re-raise without retrying.
            logger.debug("Cowardly refusing to retry this exception: %s", exc)
            raise
        else:
            break
