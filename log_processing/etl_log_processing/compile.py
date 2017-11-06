"""
Compile log records from multiple files, local or remote on S3.

The "interactive" use is really more of a demonstration  / debugging utility.
The real juice comes from milking lambda connected to S3 events so that any
log file posted by the data pipelines is automatically drained into an
Elasticsearch Service pool. That should quench your thirst for log fluids.
"""

import io
import gzip
import sys
import urllib.parse
from functools import partial

import boto3

# Note that relative imports don't work with Lambda
from etl_log_processing import parse


def load_records(sources):
    for source in sources:
        if source == "examples":
            for record in parse.create_example_records():
                yield record
        else:
            if source.startswith("s3://"):
                for record in load_remote_records(source):
                    yield record
            else:
                for record in _load_records_using(load_local_content, source):
                    yield record


def load_remote_records(file_uri):
    # Used by the lambda handler
    for record in _load_records_using(load_remote_content, file_uri):
        yield record


def _load_records_using(content_opener, content_location):
    print("Parsing '{}'".format(content_location))
    lines = content_opener(content_location)
    log_parser = parse.LogParser(content_location)
    return log_parser.split_log_lines(lines)


def load_local_content(filename):
    if filename.endswith(".gz"):
        with gzip.open(filename, 'rb') as f:
            lines = f.read().decode()
    else:
        with open(filename, 'r') as f:
            lines = f.read()
    return lines


def load_remote_content(uri):
    split_result = urllib.parse.urlsplit(uri)
    if split_result.scheme != "s3":
        raise ValueError("scheme {} not supported".format(split_result.scheme))
    bucket_name, object_key = split_result.netloc, split_result.path.lstrip('/')
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(object_key)
    response = obj.get()['Body']
    if object_key.endswith(".gz"):
        stream = io.BytesIO(response.read())
        lines = gzip.GzipFile(fileobj=stream).read().decode()
    else:
        lines = response.read().decode()
    return lines


def print_message(record):
    """Callback function which simply only prints the timestamp and the message of the log record."""
    print("{0[@timestamp]} {0[etl_id]} {0[log_level]} {0[message]}".format(record))


def filter_record(query, record):
    for key in ("etl_id", "log_level", "message"):
        if query in record[key]:
            return True
    return False


def main():
    if len(sys.argv) < 3:
        print("Usage: {} QUERY LOGFILE [LOGFILE ...]".format(sys.argv[0]))
        exit(1)
    query = str(sys.argv[1])
    processed = load_records(sys.argv[2:])
    matched = filter(partial(filter_record, query), processed)
    for record in sorted(matched, key=lambda r: r["datetime"]["epoch_time_in_millis"]):
        print_message(record)


if __name__ == "__main__":
    main()
