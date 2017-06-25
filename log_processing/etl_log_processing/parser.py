"""
Process log files from ETL into tidy little records.

When called by itself, this will run a set of example log lines through the parser.
"""

import calendar
import datetime
import hashlib
import os.path
import re
import textwrap

LOG_LINE_REGEX = """
                 # Start at beginning of a line
                 ^
                 # Look for date, e.g. 2017-06-09
                 (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s
                 # Look for time (with optional milliseconds), e.g. 06:16:19,350
                 (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(?:,(?P<millisecond>\d{3}))?\s
                 # Look for ETL id, e.g. CD58E5D3C73E4D45
                 (?P<etl_id>[0-9A-Z]{16})\s
                 # Look for log level, e.g. INFO
                 (?P<levelname>[A-Z]+)\s
                 # Look for logger, e.g. etl.config
                 (?P<logger>[.\w]+)\s
                 # Look for thread name, e.g. (MainThread)
                 \((?P<threadname>[^)]+)\)\s
                 # Look for file name and line number, e.g. [__init__.py:90]
                 \[(?P<filename>[^:]+):(?P<linenumber>\d+)\]\s
                 # Now grab the rest as message
                 (?P<message>.*)$
                 """

# https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
INDEX_TYPES = {
    "properties": {
        "year": {"type": "integer"},
        "month": {"type": "integer"},
        "day": {"type": "integer"},
        "hour": {"type": "date", "format": "strict_hour"},
        "minute": {"type": "integer"},
        "second": {"type": "integer"},
        "millisecond": {"type": "integer"},
        "etl_id": {"type": "string", "index": "not_analyzed"},
        "levelname": {"type": "string", "index": "not_analyzed"},
        "logger": {"type": "string", "analyzer": "simple"},
        "threadname": {"type": "string", "index": "not_analyzed"},
        "filename": {"type": "string", "index": "not_analyzed"},
        "linenumber": {"type": "integer"},
        "message": {"type": "string", "analyzer": "english"},
        "timestamp": {"type": "date", "format": "basic_date_time"},
        "date": {"type": "date", "format": "strict_date"},
        "time": {"type": "time", "format": "time_no_millis"},
        "unix_timestamp": {"type": "long"},
        "start_pos": {"type": "long"},
        "end_pos": {"type": "long"},
        "logfile": {"type": "string", "index": "not_analyzed"},
        "source": {"type": "string", "index": "not_analyzed"},
        "sha1": {"type": "string", "index": "not_analyzed"}
    }
}


class LogParser:

    _log_line_re = re.compile(LOG_LINE_REGEX, re.VERBOSE | re.MULTILINE)

    def __init__(self, logfile):
        self.logfile = str(logfile)
        source = os.path.basename(logfile)
        if '.' in source:
            period = source.find('.')
            source = source[:period]
        self.source = source
        self.sha1_hash = hashlib.sha1()
        self.sha1_hash.update(b'v1')
        self.sha1_hash.update(str(self.source).encode())

    @staticmethod
    def index():
        return INDEX_TYPES

    def split_log_lines(self, lines):
        """
        Split the log lines into records.
        """
        record = None
        for match in LogParser._log_line_re.finditer(lines):
            if record:
                if record["end_pos"] < match.start() - 1:
                    record["message"] += lines[record["end_pos"]:match.start() - 1]
                    record["end_pos"] = match.start() - 1
                yield record
            record = dict(match.groupdict())
            record["millisecond"] = record["millisecond"] or "0"
            for key in ('year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'linenumber'):
                record[key] = int(record[key])

            timestamp = datetime.datetime(record["year"], record["month"], record["day"],
                                          record["hour"], record["minute"], record["second"],
                                          record["millisecond"] * 1000,
                                          datetime.timezone.utc)
            record["timestamp"] = timestamp.isoformat()
            record["date"] = str(timestamp.date())
            record["time"] = str(timestamp.time())
            record["unix_timestamp"] = calendar.timegm(timestamp.timetuple())

            record["start_pos"] = match.start()
            record["end_pos"] = match.end()
            record["logfile"] = self.logfile
            record["source"] = self.source
            record["sha1"] = self.calculate_hash(record)
        if record:
            if record["end_pos"] < len(lines):
                trailing_lines = lines[record["end_pos"]:].rstrip('\n')
                record["message"] += trailing_lines
                record["end_pos"] += len(trailing_lines)
            yield record

    def calculate_hash(self, record):
        sha1 = self.sha1_hash.copy()
        for key in ("timestamp", "etl_id", "levelname", "logger", "threadname", "filename", "linenumber", "message"):
            sha1.update(str(record[key]).encode())
        return sha1.hexdigest()


def create_example_records():
    examples = """
        2017-06-26 07:52:45,106 EXAMPLE105754649 INFO etl.config (MainThread) [hello.py:89] Starting log ...
        2017-06-26 07:52:45,106 EXAMPLE105754649 ERROR etl.config (MainThread) [world.py:90] Trouble ahead...
        2017-06-26 07:52:45,107 EXAMPLE105754649 DEBUG etl.config (MainThread) [__init__.py:91] Tracing lines...
    """
    lines = textwrap.dedent(examples)
    parser = LogParser("examples")
    return list(parser.split_log_lines(lines))


def main():
    for record in create_example_records():
        print(record)


if __name__ == "__main__":
    main()
