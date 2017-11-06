"""
Uploader and Lambda handler -- send log records to Elasticsearch service.

From the command line, pick local files or files in S3, parse their content, and send
log records to ES domain.

As a lambda handler, extract the new file in S3 from the event information,
parse that file and send it to ES domain.
"""

import hashlib
import itertools
import sys
import urllib.parse

import botocore.exceptions
import elasticsearch
import elasticsearch.helpers

from etl_log_processing import compile, config, parse


def _build_actions_from(index, records):
    for record in records:
        if record["logfile"] == "examples":
            print("Example record ... _id={0.id_} timestamp={0[@timestamp]}".format(record))
        yield {
            "_op_type": "index",
            "_index": index,
            "_type": config.LOG_DOC_TYPE,
            "_id": record.id_,
            "_source": record.data
        }


def _build_meta_doc(context, timestamp, message):
    doc = {
        "lambda_name": context.function_name,
        "lambda_version": context.function_version,
        "logfile": '/'.join((context.log_group_name, context.log_stream_name)),
        "@timestamp": timestamp,
        "context": {
            "remaining_time_in_millis": context.get_remaining_time_in_millis()
        },
        "message": message
    }
    return doc


def index_records(es, records_generator):
    n_ok, n_errors = 0, 0
    for date, records in itertools.groupby(records_generator, key=lambda rec: rec["datetime"]["date"]):
        index = config.log_index(date)
        print("Indexing records ({})".format(index))
        ok, errors = elasticsearch.helpers.bulk(es, _build_actions_from(index, records))
        n_ok += ok
        if errors:
            print("Errors: {}".format(errors))
            n_errors += len(errors)
    print("Indexed successfully: {:d}, unsuccessfully: {:d}".format(n_ok, n_errors))
    return n_ok, n_errors


def lambda_handler(event, context):
    """
    Callback handler for Lambda.

    Expected event structure:
    {
        "Records": [
            {
                "eventTime": "1970-01-01T00:00:00.000Z",
                "eventName": "ObjectCreated:Put",
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": { "name": "source_bucket" },
                    "object": { "key": "StdError.gz" }
                }
            }
        ]
    }
    """
    for i, event_data in enumerate(event['Records']):
        bucket_name = event_data['s3']['bucket']['name']
        object_key = urllib.parse.unquote_plus(event_data['s3']['object']['key'])
        print("Event #{:d}: source={}, event={}, time={}, bucket={}, key={}".format(
            i, event_data['eventSource'], event_data['eventName'], event_data['eventTime'], bucket_name, object_key))

        if not (object_key.startswith("_logs/") or "/logs/" in object_key):
            print("Path is not in log folder ... skipping this file")
            return

        file_uri = "s3://{}/{}".format(bucket_name, object_key)
        try:
            host, port = config.get_es_endpoint(bucket_name=bucket_name)
            es = config.connect_to_es(host, port, use_auth=True)
            if not config.exists_index_template(es):
                config.put_index_template(es)
            processed = compile.load_remote_records(file_uri)
            ok, errors = index_records(es, processed)
        except parse.NoRecordsFoundError:
            print("Failed to find records in '{}'".format(file_uri))
            return
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response['Error']['Code']
            print("Error code {} for object '{}'".format(error_code, file_uri))
            return

        body = _build_meta_doc(context, event_data['eventTime'],
                               "Index result for '{}': ok = {}, errors = {}".format(file_uri, ok, errors))
        sha1_hash = hashlib.sha1()
        sha1_hash.update(file_uri.encode())
        id_ = sha1_hash.hexdigest()
        res = es.index(index=config.log_index(), doc_type=config.LOG_DOC_TYPE, id=id_, body=body)
        print("Sent meta information, result: {}, index: {}".format(res['result'], res['_index']))
        print("Time remaining (ms):", context.get_remaining_time_in_millis())


def main():
    if len(sys.argv) < 3:
        print("Usage: {} env_type LOGFILE [LOGFILE ...]".format(sys.argv[0]))
        exit(1)
    env_type = sys.argv[1]
    processed = compile.load_records(sys.argv[2:])
    host, port = config.get_es_endpoint(env_type=env_type)
    es = config.connect_to_es(host, port)
    index_records(es, processed)


if __name__ == "__main__":
    main()
