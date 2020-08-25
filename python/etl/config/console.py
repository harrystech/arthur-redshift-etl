import logging
import time

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

    def format(self, record):
        s = super().format(record)
        return colored(s, color=self.palette.get(record.levelno))
