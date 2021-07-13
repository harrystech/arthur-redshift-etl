import logging
import sys
import time
from datetime import datetime, timezone

import simplejson as json
from termcolor import colored


class ColorfulFormatter(logging.Formatter):
    converter = time.gmtime

    palette = {
        logging.DEBUG: "blue",
        logging.INFO: "white",
        logging.WARNING: "cyan",
        logging.ERROR: "red",
        logging.CRITICAL: "magenta",
    }

    is_tty = sys.stderr.isatty()

    def format(self, record):
        s = super().format(record)
        return colored(s, color=self.palette.get(record.levelno)) if self.is_tty else s


class JsonFormatter(logging.Formatter):

    converter = time.gmtime

    def __init__(self, environment: str):
        super().__init__()
        self.environment = environment

    def as_utc_iso8601(self, ts) -> str:
        return (
            datetime.fromtimestamp(ts, timezone.utc)
            .isoformat("T", timespec="milliseconds")
            .replace("+00:00", "Z")
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format log record by creating a JSON-format in a string."""
        values = {
            "environment": self.environment,
            "gmtime": self.as_utc_iso8601(record.created),
            "log_level": record.levelname,
            "log_severity": record.levelno,
            "logger": record.name,
            "message": str(record.getMessage()),
            "process.id": record.process,
            "process.name": record.processName,
            "source.filename": record.filename,
            "source.function": record.funcName,
            "source.line_number": record.lineno,
            "source.module": record.module,
            "source.pathname": record.pathname,
            "thread.name": record.threadName,
            "timestamp": int(record.created * 1000.0),
        }
        # Always add monitor information if present.
        if hasattr(record, "monitor"):
            values["monitor"] = record.monitor  # type: ignore
        # Always add exception information if present.
        if record.exc_text is not None:
            if values["message"] != "\n":
                values["message"] += "\n"  # type: ignore
            values["message"] += record.exc_text  # type: ignore
        return json.dumps(values, default=str, separators=(",", ":"), sort_keys=True)
