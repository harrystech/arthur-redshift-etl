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


class NoRecordsFoundError(Exception):
    pass


class LogRecord(collections.UserDict):
    """
    Single "document" matching a log line.  Use .data for the dictionary, .id_ for a suitable _id.
    """

    _INDEX_FIELDS = {
        "properties": {
            "application_name": {"type": "keyword"},
            "environment": {"type": "keyword"},
            "logfile": {
                "type": "keyword",
                "include_in_all": False
                # or else you get too many double matches after pulling out the interesting values from the name
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
            "@timestamp": {"type": "date", "format": "strict_date_optional_time"},  # generic ISO datetime parser
            "datetime": {
                "properties": {
                    "epoch_time_in_millis": {"type": "long"},
                    "date": {"type": "date", "format": "strict_date"},  # used to select index during upload
                    "year": {"type": "integer"},
                    "month": {"type": "integer"},
                    "day": {"type": "integer"},
                    "day_of_week": {"type": "integer"},
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
                    "monitor_id": {"type": "keyword"},
                    "step": {"type": "keyword"},
                    "event": {"type": "keyword"},
                    "target": {"type": "keyword"},
                    "elapsed": {"type": "float"},
                    "error_codes": {"type": "text"}
                }
            },
            "parser": {
                "properties": {
                    "start_pos": {"type": "long"},
                    "end_pos": {"type": "long"},
                    "chars": {"type": "long"}
                },
            },
            # These last properties are only used by the Lambda handler:
            "lambda_name": {"type": "keyword"},
            "lambda_version": {"type": "keyword"},
            "context": {
                "properties": {
                    "remaining_time_in_millis": {"type": "long"}
                }
            }
        }
    }

    @staticmethod
    def index_fields():
        return copy.deepcopy(LogRecord._INDEX_FIELDS)

    def __init__(self, d):
        super().__init__(d)
        self.__counter = collections.Counter()

    @property
    def id_(self):
        sha1_hash = hashlib.sha1()
        key_values = (
            "v1",
            self["@timestamp"],
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
        # --- Basic info ---
        self.message = values["message"]
        self.update({k: v for k, v in values.items() if k in ["etl_id", "log_level", "logger", "thread_name"]})
        self["source_code"] = {k: v for k, v in values.items() if k in ["filename", "line_number"]}
        self["parser"] = {"start_pos": match.start(), "end_pos": match.end(), "chars": match.end() - match.start()}
        # --- Timestamp (with pseudo-microseconds so that log lines stay sorted in order of logfile) ---
        ts = values["_timestamp"]
        self.__counter[ts] += 1
        if values["_milliseconds"] is None:
            ts += ",{:06d}".format(self.__counter[ts])  # 04:05:06 -> 04:05:06,000001
        else:
            ts += "{:03d}".format(self.__counter[ts])  # 04:05:06,789 -> 04:05:06,789001
        timestamp = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=datetime.timezone.utc)
        self.update({
            "@timestamp": timestamp.isoformat(),  # Python datetime drops milliseconds if value is 0.
            "datetime": {
                "epoch_time_in_millis": calendar.timegm(timestamp.timetuple()) * 1000 + timestamp.microsecond // 1000,
                "date": timestamp.date().isoformat(),
                "year": timestamp.year,
                "month": timestamp.month,
                "day": timestamp.day,
                "day_of_week": timestamp.date().isoweekday(),
                "hour": timestamp.hour,
                "minute": timestamp.minute,
                "second": timestamp.second
            }
        })
        # --- Monitor payload ---
        if values["message"].startswith("Monitor payload = "):
            payload_text = values["message"].replace("Monitor payload = ", "", 1)
            try:
                monitor_payload = json.loads(payload_text)
            except json.decoder.JSONDecodeError as exc:
                print("Partial monitor payload detected in '{}': {}".format(payload_text, exc))
            else:
                self["monitor"] = {k: v for k, v in monitor_payload.items()
                                   if k in ["monitor_id", "step", "event", "target", "elapsed"]}
                if "errors" in monitor_payload:
                    self["monitor"]["error_codes"] = " ".join(error["code"] for error in monitor_payload["errors"])

    # Properties to help with updating parser information

    @property
    def message(self):
        return self["message"]

    @message.setter
    def message(self, value):
        self["message"] = value

    @property
    def end_pos(self):
        return self["parser"]["end_pos"]

    @end_pos.setter
    def end_pos(self, value):
        self["parser"]["end_pos"] = value
        self["parser"]["chars"] = value - self["parser"]["start_pos"]


class LogParser:

    # Basic Regex to split up Arthur log lines
    _LOG_LINE_REGEX = """
        # Look for timestamp from beginning of line, e.g. 2017-06-09 06:16:19,350 (where msecs are optional)
        ^(?P<_timestamp>\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}(?:,(?P<_milliseconds>\d{3}))?)\s
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

    def __init__(self, logfile):
        logfile = str(logfile)
        # Information that is copied into every record
        self.shared_info = {
            "application_name": "arthur-redshift-etl",
            "logfile": logfile
        }
        # Try to find the information for the Data Pipeline or EMR cluster
        df_environment, data_pipeline = self.extract_data_pipeline_information()
        j_environment, emr_cluster = self.extract_emr_cluster_information()
        if data_pipeline:
            self.shared_info["environment"] = df_environment
            self.shared_info["data_pipeline"] = data_pipeline
        if emr_cluster:
            self.shared_info["environment"] = j_environment
            self.shared_info["emr_cluster"] = emr_cluster
        self._log_line_re = re.compile(LogParser._LOG_LINE_REGEX, re.VERBOSE | re.MULTILINE)

    def extract_data_pipeline_information(self):
        """
        Return information related to a data pipeline if it is contained in the filename of the logfile.

        The examples below have "<prefix>/{number}" as an environment to make them easier to distinguish.
        >>> lp = LogParser("s3://<bucket>/_logs/<prefix>/1/df-<id>/<component>/<instance>/<attempt>/<cluster_id>/"
        ...                "steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/1'
        >>> lp.shared_info["data_pipeline"]["id"]
        'df-<id>'
        >>> lp = LogParser("s3://<bucket>/_logs/<prefix>/2/df-<id>/<component>/<instance>/<attempt>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/2'
        >>> lp.shared_info["data_pipeline"]["id"]
        'df-<id>'
        >>> lp = LogParser("s3://<bucket>/<prefix>/3/logs/df-<id>/<component>/<instance>/<attempt>/<cluster_id>/"
        ...                "steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/3'
        >>> lp.shared_info["data_pipeline"]["id"]
        'df-<id>'
        >>> lp = LogParser("s3://<bucket>/<prefix>/4/logs/df-<id>/<component>/<instance>/<attempt>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/4'
        >>> lp.shared_info["data_pipeline"]["id"]
        'df-<id>'
        """
        parts = self.shared_info["logfile"].replace("s3://", "", 1).split('/')
        if len(parts) >= 8:
            if parts[1] == "_logs":
                if parts[-3] == "steps":
                    environment = '/'.join(parts[2:-8])
                    data_pipeline = parts[-8:-4]
                else:
                    environment = '/'.join(parts[2:-5])
                    data_pipeline = parts[-5:-1]
            elif len(parts) > 11 and parts[-9] == "logs" and parts[-3] == "steps":
                    environment = '/'.join(parts[1:-9])
                    data_pipeline = parts[-8:-3]
            elif parts[-6] == "logs":
                environment = '/'.join(parts[1:-6])
                data_pipeline = parts[-5:-1]
            else:
                environment = None
                data_pipeline = None
            if environment is not None and data_pipeline[0].startswith("df-"):
                return environment, dict(zip(["id", "component", "instance", "attempt"], data_pipeline))
        return None, {}

    def extract_emr_cluster_information(self):
        """
        Return information related to an EMR cluster if it is contained in the filename of the logfile.

        Basically extract from:
        >>> lp = LogParser("s3://<bucket>/_logs/<prefix>/1/j-<id>/node/<node_id>/applications/hadoop/"
        ...                "steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/1'
        >>> lp.shared_info["emr_cluster"]["id"]
        'j-<id>'
        >>> lp = LogParser("s3://<bucket>/_logs/<prefix>/2/j-<id>/steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/2'
        >>> lp.shared_info["emr_cluster"]["id"]
        'j-<id>'


        >>> lp = LogParser("s3://<bucket>/<prefix>/3/logs/j-<id>/node/<node_id>/applications/hadoop/"
        ...                "steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/3'
        >>> lp.shared_info["emr_cluster"]["id"]
        'j-<id>'
        >>> lp = LogParser("s3://<bucket>/<prefix>/4/logs/j-<id>/steps/<step_id>/<basename>")
        >>> lp.shared_info["environment"]
        '<prefix>/4'
        >>> lp.shared_info["emr_cluster"]["id"]
        'j-<id>'
        """
        parts = self.shared_info["logfile"].replace("s3://", "", 1).split('/')
        if len(parts) >= 7 and parts[-3] == "steps":
            long_form = len(parts) >= 11 and parts[-4] == "hadoop"
            if long_form and parts[1] == "_logs":
                environment = '/'.join(parts[2:-8])
            elif parts[1] == "_logs":
                environment = '/'.join(parts[2:-4])
            elif long_form and parts[-9] == "logs":
                environment = '/'.join(parts[1:-9])
            elif parts[-5] == "logs":
                environment = '/'.join(parts[1:-5])
            else:
                environment = None
            if long_form:
                emr_cluster_id = parts[-8]
            else:
                emr_cluster_id = parts[-4]
            step_id = parts[-2]
            if environment and emr_cluster_id.startswith("j-"):
                return environment, dict(zip(["id", "step_id"], [emr_cluster_id, step_id]))
        return None, {}

    def split_log_lines(self, lines):
        """
        Split the log lines (passed as single string) into records.

        An exception is raised if no records are found at all.
        """
        record = None
        for match in self._log_line_re.finditer(lines):
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

        if record is None:
            raise NoRecordsFoundError("found no records")


def create_example_records():
    monitor = {
        "monitor_id": "MONITOR8D214440B",
        "step": "load",
        "event": "finish",
        "target": "schema.example",
        "elapsed": 21.117434
    }
    # The examples are "old" enough for the corresponding index to be "stale" ... see delete_stale_indices
    examples = """
        2016-06-26 07:52:45,106 EXAMPLE105754649 INFO etl.config (MainThread) [hello.py:89] Starting log ...
        2016-06-26 07:52:59 EXAMPLE105754649 ERROR etl.config (MainThread) [world.py:90] Trouble without millis...
        2016-06-26 07:53:02,107 EXAMPLE105754649 DEBUG etl.monitor (MainThread) [monitor.py:255] Monitor payload = {}
    """.format(json.dumps(monitor))
    lines = textwrap.dedent(examples)
    parser = LogParser("examples")
    return list(parser.split_log_lines(lines))


def main():
    for record in create_example_records():
        print(record)


if __name__ == "__main__":
    main()
