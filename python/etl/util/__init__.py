import sys
import traceback

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
