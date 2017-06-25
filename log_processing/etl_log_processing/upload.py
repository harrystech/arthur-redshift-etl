"""
Uploader and Lambda handler -- send log records to Elasticsearch.

From the command line, pick local files or files in S3, parse their content, and send
log records to ES domain.

As a lambda handler, extract the new file in S3 from the event information,
parse that file and send it to ES domain.
"""

import io
import gzip
import urllib.parse
import sys

import boto3
import elasticsearch
import elasticsearch.helpers

from . import config
from . import parser


def load_records(sources):
    for source in sources:
        if source == "examples":
            for record in parser.create_example_records():
                yield record
        else:
            if source.startswith("s3://"):
                for full_name in list_files(source):
                    lines = load_remote_content(full_name)
                    log_parser = parser.LogParser(full_name)
                    for record in log_parser.split_log_lines(lines):
                        yield record
            else:
                lines = load_local_content(source)
                log_parser = parser.LogParser(source)
                for record in log_parser.split_log_lines(lines):
                    yield record


def load_local_content(filename):
    print("Parsing '{}'".format(filename), file=sys.stderr)
    if filename.endswith(".gz"):
        with gzip.open(filename, 'rb') as f:
            lines = f.read().decode()
    else:
        with open(filename, 'r') as f:
            lines = f.read()
    return lines


def _split_uri_for_s3(uri):
    split_result = urllib.parse.urlsplit(uri)
    if split_result.scheme != "s3":
        raise ValueError("scheme {} not supported".format(split_result.scheme))
    return split_result.netloc, split_result.path.lstrip('/')


def load_remote_content(uri):
    print("Parsing '{}'".format(uri), file=sys.stderr)
    bucket_name, object_key = _split_uri_for_s3(uri)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    obj = bucket.Object(object_key)
    response = obj.get()['Body']
    if object_key.endswith(".gz"):
        stream = io.BytesIO(response.read())
        lines = gzip.GzipFile(fileobj=stream).read().decode()
    else:
        lines = response.read()
    return lines


def list_files(uri):
    bucket_name, prefix = _split_uri_for_s3(uri)
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    for summary in bucket.objects.filter(Prefix=prefix):
        yield "s3://{}/{}".format(summary.bucket_name, summary.key)


def lambda_handler(event, context):
    s3_event = event['Records'][0]['s3']
    bucket_name = s3_event['bucket']['name']
    object_key = urllib.parse.unquote_plus(s3_event['object']['key'].encode('utf8'))
    print("bucket_name: {}, object_key: {}", bucket_name, object_key)


def connect_to_es():
    print("Connecting to ES endpoint: {}".format(config.MyConfig.endpoint))
    return elasticsearch.Elasticsearch(
        hosts=[{"host": config.MyConfig.endpoint, "port": 443}],
        use_ssl=True,
        verify_certs=True
    )


def create_index(client):
    print("Creating index {} ({})".format(config.MyConfig.index, config.MyConfig.doc_type))
    client.indices.create(index=config.MyConfig.index, body=parser.LogParser.index(), ignore=400)
    body = dict(mappings={})
    body["mappings"][config.MyConfig.doc_type] = parser.LogParser.index()
    client.indices.create(index=config.MyConfig.index, body=body, ignore=400)


def _build_actions_from(records):
    index = config.MyConfig.index
    doc_type = config.MyConfig.doc_type
    for record in records:
        print("{sha1} {logfile} {start_pos}..{end_pos}".format(**record))
        yield {
            "_op_type": "index",
            "_index": index,
            "_type": doc_type,
            "_id": record["sha1"],
            "_source": record
        }


def index_records(client, records):
    print("Indexing new records")
    ok, errors = elasticsearch.helpers.bulk(client, _build_actions_from(records))
    print("Uploaded successfully: {:d}".format(ok))
    print("Errors: {}".format(errors))


def main():
    if len(sys.argv) < 2:
        print("Usage: {} LOGFILE [LOGFILE ...]".format(sys.argv[0]))
        exit(1)
    client = connect_to_es()
    create_index(client)
    processed = load_records(sys.argv[1:])
    index_records(client, processed)


if __name__ == "__main__":
    main()
