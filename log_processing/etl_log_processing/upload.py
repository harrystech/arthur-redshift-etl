"""
Uploader and Lambda handler -- send log records to Elasticsearch service.

From the command line, pick local files or files in S3, parse their content, and send
log records to ES domain.

As a lambda handler, extract the new file in S3 from the event information,
parse that file and send it to ES domain.
"""

import hashlib
import sys
import urllib.parse

import botocore.exceptions
import elasticsearch
import elasticsearch.helpers

from etl_log_processing import compile, config


def _build_actions_from(index, records):
    for record in records:
        if record["logfile"] == "examples":
            print("Example record ... _id={0.id_} timestamp={0[timestamp]}".format(record))
        yield {
            "_op_type": "index",
            "_index": index,
            "_type": config.LOG_DOC_TYPE,
            "_id": record.id_,
            "_source": record.data
        }


def _build_meta_doc(context, environment, path, timestamp):
    try:
        resource = path[:path.index('/')]
    except ValueError:
        resource = ""
    doc = {
        "application": context.function_name,
        "environment": environment,
        "logfile": '/'.join((context.log_group_name, context.log_stream_name)),
        "timestamp": timestamp,
        "log_level": "INFO"
    }
    if resource.startswith("df-"):
        doc["data_pipeline"] = {"id": resource}
    elif resource.startswith("j-"):
        doc["emr_cluster"] = {"id": resource}
    return doc


def index_records(es, records_generator):
    index = config.current_log_index()
    print("Indexing records ({})".format(index))
    ok, errors = elasticsearch.helpers.bulk(es, _build_actions_from(index, records_generator))
    if ok:
        print("Indexed successfully: {:d}".format(ok))
    else:
        print("No new records were indexed")
    if errors:
        print("Errors: {}".format(errors))
    return ok, errors


def lambda_handler(event, context):
    event_data = event['Records'][0]
    bucket_name = event_data['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event_data['s3']['object']['key'])
    object_size = event_data['s3']['object']['size']
    print("Event: eventSource={}, eventName={}, eventTime={} bucket_name={}, object_key={} object_size={}".format(
        event_data['eventSource'], event_data['eventName'], event_data['eventTime'],
        bucket_name, object_key, object_size))
    try:
        environment, path = object_key.split("/logs/", 1)
    except ValueError:
        print("Path does not contain '/logs/' ... skipping this file")
        return

    file_uri = "s3://{}/{}".format(bucket_name, object_key)
    try:
        processed = list(compile.load_remote_records(file_uri))
        print("Time remaining (ms) after processing:", context.get_remaining_time_in_millis())
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response['Error']['Code']
        print("Error code {} for object '{}'".format(error_code, file_uri))
        return
    # TODO Raise exception when no records are found so that we don't have to materialize the list
    if not processed:
        print("No log records found in '{}'".format(file_uri))
        return

    host, port = config.get_es_endpoint(bucket_name=bucket_name)
    es = config.connect_to_es(host, port, use_auth=True)
    ok, errors = index_records(es, processed)
    print("Time remaining (ms) after indexing:", context.get_remaining_time_in_millis())

    doc = _build_meta_doc(context, environment, path, event_data['eventTime'])
    doc["message"] = "Index result for '{}': ok = {}, errors = {}".format(file_uri, ok, errors)
    # TODO Log every invocation?
    sha1_hash = hashlib.sha1()
    sha1_hash.update(file_uri.encode())
    id_ = sha1_hash.hexdigest()
    res = es.index(index=config.current_log_index(), doc_type=config.LOG_DOC_TYPE, id=id_, body=doc)
    print("Sent meta information: ", res)


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
