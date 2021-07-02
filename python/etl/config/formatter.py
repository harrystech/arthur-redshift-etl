import logging
import sys
import time

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

    def format(self, record: logging.LogRecord) -> str:
        """Format log record by creating a JSON-format in a string."""
        values = {
            "log_level": record.levelname,
            "log_severity": record.levelno,
            "logger": record.name,
            "message": record.getMessage(),
            "process.id": record.process,
            "process.name": record.processName,
            "source.filename": record.filename,
            "source.function": record.funcName,
            "source.line_number": record.lineno,
            "source.module": record.module,
            "source.pathname": record.pathname,
            "thread.name": record.threadName,
        }
        return json.dumps(values, default=str, separators=(",", ":"), sort_keys=True)
