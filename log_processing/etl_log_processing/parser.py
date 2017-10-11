"""
Process log files from ETL into tidy little records.

When called by itself, this will run a set of example log lines through the parser.
"""

import calendar
import datetime
import copy
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
                 (?P<log_level>[A-Z]+)\s
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
        "application": {"type": "keyword"},
        "environment": {"type": "keyword"},
        "logfile": {"type": "keyword"},
        "log_name": {"type": "keyword"},
        "data_pipeline": {
            "properties": {
                "id": {"type": "keyword"},
                "component": {"type": "string"},
                "instance": {"type": "keyword"},
                "attempt": {"type": "keyword"}
            }
        },
        "emr_cluster": {
            "properties": {
                "id": {"type": "keyword"},
                "step_id": {"type": "keyword"}
            }
        },
        "etl_id": {"type": "keyword"},
        "time": {"type": "time", "format": "time_no_millis"},
        "timestamp": {"type": "date", "format": "basic_date_time"},
        "unix_timestamp": {"type": "long"},
        "year": {"type": "integer"},
        "month": {"type": "integer"},
        "day": {"type": "integer"},
        "date": {"type": "date", "format": "strict_date"},
        "hour": {"type": "date", "format": "strict_hour"},
        "minute": {"type": "integer"},
        "second": {"type": "integer"},
        "millisecond": {"type": "integer"},
        "log_level": {"type": "keyword"},
        "logger": {"type": "string", "analyzer": "simple"},
        "threadname": {"type": "string", "index": "not_analyzed"},
        "filename": {"type": "string", "index": "not_analyzed"},
        "linenumber": {"type": "integer"},
        "message": {"type": "text", "analyzer": "english"},
        "start_pos": {"type": "long"},
        "end_pos": {"type": "long"},
        "sha1": {"type": "keyword"}
    }
}


class LogParser:

    _log_line_re = re.compile(LOG_LINE_REGEX, re.VERBOSE | re.MULTILINE)

    def __init__(self, logfile):
        logfile = str(logfile)
        # Try to find the basename, e.g. "arthur" for the /some/path/arthur.log file
        basename = os.path.basename(logfile)
        log_name, _ = os.path.splitext(basename)
        self.sha1_hash = hashlib.sha1()
        self.sha1_hash.update(b'v1')
        self.sha1_hash.update(str(log_name).encode())
        # Try to find the information for the Data Pipeline or EMR cluster
        data_pipeline = LogParser.extract_data_pipeline_information(logfile)
        emr_cluster = LogParser.extract_emr_cluster_information(logfile)
        # Information that is copied into every record
        self.shared_info = {
            "application": "arthur-redshift-etl",
            "logfile": logfile,
            "log_name": log_name
        }
        environment = data_pipeline.get("environment") or emr_cluster.get("environment")
        if environment:
            self.shared_info["environment"] = environment
        if data_pipeline:
            self.shared_info["data_pipeline"] = data_pipeline
            del data_pipeline["environment"]
        if emr_cluster:
            self.shared_info["emr_cluster"] = emr_cluster
            del emr_cluster["environment"]

    @staticmethod
    def extract_data_pipeline_information(filename):
        """
        Return information related to a data pipeline if it is contained in the filename of the logfile.

        Basically extract from:  s3://<bucket>/<prefix>/logs/<id>/<component>/<instance>/<attempt>/<basename>
        """
        parts = filename.replace("s3://", "", 1).split('/')
        environment = '/'.join(parts[1:-6])
        (sentinel,) = parts[-6:-5] if len(parts) >= 8 else (None,)
        data_pipeline = parts[-5:-1]
        if sentinel == "logs" and len(data_pipeline) == 4 and data_pipeline[0].startswith("df-"):
            return dict(id=data_pipeline[0],
                        component=data_pipeline[1],
                        instance=data_pipeline[2],
                        attempt=data_pipeline[3],
                        environment=environment)
        else:
            return {}

    @staticmethod
    def extract_emr_cluster_information(filename):
        """
        Return information related to an EMR cluster if it is contained in the filename of the logfile.

        Basically extract from either:
            s3://<bucket>/<prefix>/logs/<id>/steps/<step_id>/<basename>
            s3://<bucket>/<prefix>/logs/<id>/node/<node_id>/applications/hadoop/steps/<step_id>/<basename>

        """
        parts = filename.replace("s3://", "", 1).split('/')
        # Use the toplevel steps directory:
        environment = '/'.join(parts[1:-5])
        (sentinel,) = parts[-5:-4] if len(parts) >= 7 else (None,)
        emr_cluster = parts[-4:-1]
        if sentinel == "logs" and len(emr_cluster) == 3 and emr_cluster[1] == "steps":
            return dict(id=emr_cluster[0], step_id=emr_cluster[2], environment=environment)
        # Use the node directory:
        environment = '/'.join(parts[1:-9])
        (sentinel,) = parts[-9:-8] if len(parts) >= 11 else (None,)
        emr_cluster = parts[-8:-1]
        if sentinel == "logs" and len(emr_cluster) == 7 and emr_cluster[5] == "steps":
            return dict(id=emr_cluster[0], step_id=emr_cluster[6], environment=environment)
        # Neither pattern matched
        return {}

    @staticmethod
    def index():
        return copy.deepcopy(INDEX_TYPES)

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
            record.update(self.shared_info)

            record["millisecond"] = record["millisecond"] or "0"
            for key in ('year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'linenumber'):
                record[key] = int(record[key])
            timestamp = datetime.datetime(record["year"], record["month"], record["day"],
                                          record["hour"], record["minute"], record["second"],
                                          record["millisecond"] * 1000,
                                          tzinfo=datetime.timezone.utc)
            record["timestamp"] = timestamp.isoformat()
            record["date"] = str(timestamp.date())
            record["time"] = str(timestamp.time())
            record["unix_timestamp"] = calendar.timegm(timestamp.timetuple())

            record["start_pos"] = match.start()
            record["end_pos"] = match.end()
            record["sha1"] = self.calculate_hash(record)
        if record:
            if record["end_pos"] < len(lines):
                trailing_lines = lines[record["end_pos"]:].rstrip('\n')
                record["message"] += trailing_lines
                record["end_pos"] += len(trailing_lines)
            yield record

    def calculate_hash(self, record):
        sha1 = self.sha1_hash.copy()
        for key in ("timestamp", "etl_id", "log_level", "logger", "threadname", "filename", "linenumber", "message"):
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
