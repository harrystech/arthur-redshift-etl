import datetime
import sys
import traceback
from typing import Optional

import arrow
from termcolor import colored


def croak(error, exit_code: int) -> None:
    """
    Print first line of exception and then bail out with the exit code.

    When you have a large stack trace, it's easy to miss the trigger and
    so we call it out here again on stderr.
    """
    exception_only = traceback.format_exception_only(type(error), error)[0]
    header = exception_only.splitlines()[0]
    # Make sure to not send random ASCII sequences to a log file.
    if sys.stderr.isatty():
        header = colored(header, color="red", attrs=["bold"])
    print(f"Bailing out: {header}", file=sys.stderr)
    sys.exit(exit_code)


def isoformat_datetime_string(argument: str, relative_to: Optional[str] = None) -> datetime.datetime:
    r"""
    Format the argument into a datetime object.

    Allowed values are strings in ISO 8601 format and "human" relative values like "1 hour ago".
    BTW "isoformat" is used as a verb here. ¯\_(ツ)_/¯

    >>> isoformat_datetime_string("2021-11-11T12:13:29")
    datetime.datetime(2021, 11, 11, 12, 13, 29, tzinfo=tzutc())
    >>> isoformat_datetime_string("1 hour ago", relative_to="2021-10-08T18:13:29")
    datetime.datetime(2021, 10, 8, 17, 13, 29, tzinfo=tzutc())
    >>> isoformat_datetime_string("20 minutes ago", relative_to="2020-06-22T03:00:00")
    datetime.datetime(2020, 6, 22, 2, 40, tzinfo=tzutc())

    Args:
        argument: an ISO 8601 formatted date and time or a description of a time offset
        relative_to: sets what "now" is when relative time offsets are passed in (used for testing)
    """
    try:
        return arrow.get(argument).datetime
    except arrow.parser.ParserError:
        pass
    if relative_to is None:
        now = arrow.now("UTC")
    else:
        now = arrow.get(relative_to)
    # Patch arrow's mishandling of plurals, so "1 hour ago" must be passed in as "1 hours ago".
    argument = (
        argument.replace(" day ", " days ").replace(" hour ", " hours ").replace(" minute ", " minutes ")
    )
    return now.dehumanize(argument).datetime
