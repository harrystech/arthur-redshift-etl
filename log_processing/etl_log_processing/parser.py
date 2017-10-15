"""
Process log files from ETL into tidy little records.

When called by itself, this will run a set of example log lines through the parser.
"""

import calendar
import collections
import datetime
import copy
import hashlib
import json
import re
import textwrap

INDEX_TYPES = {
    "properties": {
        "application": {"type": "keyword"},
        "environment": {"type": "keyword"},
        "logfile": {
            "type": "keyword",
            "include_in_all": False
        },
        "data_pipeline": {
            "properties": {
                "id": {"type": "keyword"},
                "component": {"type": "keyword"},
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
        "timestamp": {"type": "date", "format": "strict_date_optional_time"},
        "datetime": {
            "properties": {
                "epoch_time": {"type": "long"},  # "format": "epoch_second"
                "year": {"type": "integer"},
                "month": {"type": "integer"},
                "day": {"type": "integer"},
                "hour": {"type": "integer"},
                "minute": {"type": "integer"},
                "second": {"type": "integer"}
            },
        },
        "etl_id": {"type": "keyword"},
        "log_level": {"type": "keyword"},
        "logger": {
            "type": "text",
            "analyzer": "simple",
            "fields": {
                "name": {
                    "type": "keyword"
                }
            }
        },
        "thread_name": {"type": "keyword"},
        "source_code": {
            "properties": {
                "filename": {"type": "string"},
                "line_number": {"type": "integer"},
            }
        },
        "message": {
            "type": "text",
            "analyzer": "standard",
            "fields": {
                "raw": {
                    "type": "keyword"
                },
                "english": {
                    "type": "text",
                    "analyzer": "english"
                }
            }
        },
        "monitor": {
            "properties": {
                "id": {"type": "keyword"},
                "event": {"type": "keyword"},
                "step": {"type": "keyword"},
                "target": {"type": "keyword"},
                "elapsed": {"type": "float"}
            }
        },
        "parser": {
            "properties": {
                "start_pos": {"type": "long"},
                "end_pos": {"type": "long"},
            },
        }
    }
}

# Basic Regex to split up Arthur log lines
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
                 (?P<logger>\w[.\w]+)\s
                 # Look for thread name, e.g. (MainThread)
                 \((?P<thread_name>[^)]+)\)\s
                 # Look for file name and line number, e.g. [__init__.py:90]
                 \[(?P<filename>[^:]+):(?P<line_number>\d+)\]\s
                 # Now grab the rest as message
                 (?P<message>.*)$
                 """


class LogRecord(collections.UserDict):
    """
    Single "document" matching a log line.  Use .data for the dictionary, .id_ for a suitable _id.
    """

    @property
    def id_(self):
        sha1_hash = hashlib.sha1()
        key_values = (
            "v1",
            self["timestamp"],
            self["etl_id"],
            self["log_level"],
            self["logger"],
            self["thread_name"],
            self["source_code"]["filename"],
            self["source_code"]["line_number"],
            self["message"]
        )
        sha1_hash.update(' '.join(map(str, key_values)).encode())
        return sha1_hash.hexdigest()

    def update_from(self, match):
        """
        Copy values from regular expression match into appropriate positions.
        """
        values = match.groupdict()
        values["millisecond"] = values["millisecond"] or "0"
        for key in ("year", "month", "day", "hour", "minute", "second", "millisecond"):
            values[key] = int(values[key])
        timestamp = datetime.datetime(values["year"], values["month"], values["day"],
                                      values["hour"], values["minute"], values["second"],
                                      values["millisecond"] * 1000,
                                      tzinfo=datetime.timezone.utc)
        self.update({
            "timestamp": timestamp.isoformat(),  # Drops milliseconds if value is 0.
            "datetime": {
                "epoch_time": calendar.timegm(timestamp.timetuple()) * 1000 + values["millisecond"],
                "year": values["year"],
                "month": values["month"],
                "day": values["day"],
                "hour": values["hour"],
                "minute": values["minute"],
                "second": values["second"]
            },
            "etl_id": values["etl_id"],
            "log_level": values["log_level"],
            "logger": values["logger"],
            "thread_name": values["thread_name"],
            "source_code": {
                "filename": values["filename"],
                "line_number": int(values["line_number"])
            },
            "parser": {
                "start_pos": match.start(),
                "end_pos": match.end()
            }
        })
        if values["message"].startswith("Monitor payload ="):
            monitor_payload = json.loads(values["message"].replace("Monitor payload = ", "", 1))
            self["monitor"] = {
                "id": monitor_payload["monitor_id"],
                "step": monitor_payload["step"],
                "event": monitor_payload["event"],
                "target": monitor_payload["target"],
            }
            if "elapsed" in monitor_payload:
                self["monitor"]["elapsed"] = monitor_payload["elapsed"]
        self.message = values["message"]

    # Properties to help with updating parser information

    @property
    def message(self):
        return self["message"]

    @message.setter
    def message(self, value):
        self["message"] = value
        self["parser"]["bytes"] = len(value.encode("utf-8", errors="ignore"))

    @property
    def end_pos(self):
        return self["parser"]["end_pos"]

    @end_pos.setter
    def end_pos(self, value):
        self["parser"]["end_pos"] = value


class LogParser:

    _log_line_re = re.compile(LOG_LINE_REGEX, re.VERBOSE | re.MULTILINE)

    @staticmethod
    def index():
        return copy.deepcopy(INDEX_TYPES)

    def __init__(self, logfile):
        logfile = str(logfile)
        # Information that is copied into every record
        self.shared_info = {
            "application": "arthur-redshift-etl",
            "logfile": logfile
        }
        # Try to find the information for the Data Pipeline or EMR cluster
        df_environment, data_pipeline = self.extract_data_pipeline_information()
        j_environment, emr_cluster = self.extract_emr_cluster_information()
        if df_environment or j_environment:
            self.shared_info["environment"] = df_environment or j_environment
        if data_pipeline:
            self.shared_info["data_pipeline"] = data_pipeline
        if emr_cluster:
            self.shared_info["emr_cluster"] = emr_cluster

    def extract_data_pipeline_information(self):
        """
        Return information related to a data pipeline if it is contained in the filename of the logfile.

        Basically extract from:  s3://<bucket>/<prefix>/logs/<id>/<component>/<instance>/<attempt>/<basename>
        """
        parts = self.shared_info["logfile"].replace("s3://", "", 1).split('/')
        environment = '/'.join(parts[1:-6])
        (sentinel,) = parts[-6:-5] if len(parts) >= 8 else (None,)
        data_pipeline = parts[-5:-1]
        if sentinel == "logs" and len(data_pipeline) == 4 and data_pipeline[0].startswith("df-"):
            return environment, dict(id=data_pipeline[0],
                                     component=data_pipeline[1],
                                     instance=data_pipeline[2],
                                     attempt=data_pipeline[3])
        else:
            return None, {}

    def extract_emr_cluster_information(self):
        """
        Return information related to an EMR cluster if it is contained in the filename of the logfile.

        Basically extract from either:
            s3://<bucket>/<prefix>/logs/<id>/steps/<step_id>/<basename>
            s3://<bucket>/<prefix>/logs/<id>/node/<node_id>/applications/hadoop/steps/<step_id>/<basename>

        """
        parts = self.shared_info["logfile"].replace("s3://", "", 1).split('/')
        environment = '/'.join(parts[1:-5])
        (sentinel,) = parts[-5:-4] if len(parts) >= 7 else (None,)
        emr_cluster = parts[-4:-1]
        if sentinel == "logs" and len(emr_cluster) == 3 and emr_cluster[1] == "steps":
            return environment, dict(id=emr_cluster[0], step_id=emr_cluster[2])
        # Use the node directory:
        environment = '/'.join(parts[1:-9])
        (sentinel,) = parts[-9:-8] if len(parts) >= 11 else (None,)
        emr_cluster = parts[-8:-1]
        if sentinel == "logs" and len(emr_cluster) == 7 and emr_cluster[5] == "steps":
            return environment, dict(id=emr_cluster[0], step_id=emr_cluster[6])
        # Neither pattern matched
        return None, {}

    def split_log_lines(self, lines):
        """
        Split the log lines into records.
        """
        record = None
        for match in LogParser._log_line_re.finditer(lines):
            if record:
                # Append interceding lines to latest message
                if record.end_pos < match.start() - 1:
                    record.message += lines[record.end_pos:match.start() - 1]
                    record.end_pos = match.start() - 1
                yield record

            record = LogRecord(self.shared_info)
            record.update_from(match)

        if record:
            # Append remaining lines to last message
            if record.end_pos < len(lines):
                trailing_lines = lines[record.end_pos:].rstrip('\n')
                record.message += trailing_lines
                record.end_pos += len(trailing_lines)
            yield record


def create_example_records():
    monitor = {
        "monitor_id": "MONITOR8D214440B",
        "step": "load",
        "event": "finish",
        "target": "schema.example",
        "elapsed": 21.117434
    }
    examples = """
        2017-06-26 07:52:45,106 EXAMPLE105754649 INFO etl.config (MainThread) [hello.py:89] Starting log ...
        2017-06-26 07:52:59 EXAMPLE105754649 ERROR etl.config (MainThread) [world.py:90] Trouble without millis...
        2017-06-26 07:53:02,107 EXAMPLE105754649 DEBUG etl.monitor (MainThread) [monitor.py:255] Monitor payload = {}
    """.format(json.dumps(monitor))
    lines = textwrap.dedent(examples)
    parser = LogParser("examples")
    return list(parser.split_log_lines(lines))


def main():
    for record in create_example_records():
        print(record)

if __name__ == "__main__":
    main()
