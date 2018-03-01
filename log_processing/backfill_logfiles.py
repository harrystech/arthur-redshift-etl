#! /usr/bin/env python3

"""
Load log files into Elasticsearch domain
"""

import json
import logging
import os.path
import sys
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import partial

import boto3


def list_objects(bucket_name, prefix):
    client = boto3.client("s3")
    paginator = client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for response in response_iterator:
        for info in response['Contents']:
            yield info['Key']


def invoke_log_parser(function_name, bucket_name, object_key):
    logging.info("Invoking log parser for s3://{}/{}".format(bucket_name, object_key))
    payload = {
        'Records': [
            {
                "eventTime": datetime.utcnow().isoformat(),
                "eventName": "ObjectCreated:Put",
                "eventSource": "aws:s3",
                "s3": {
                    "bucket": {
                        "name": bucket_name
                    },
                    "object": {
                        "key": urllib.parse.quote_plus(object_key)
                    }
                }
            }
        ]
    }
    try:
        client = boto3.client("lambda")
        response = client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            LogType='None',
            Payload=json.dumps(payload).encode()
        )
        status_code = response['StatusCode']
        if status_code == 200:
            logging.info("Finished parsing of 's3://{}/{}' successfully".format(bucket_name, object_key))
        else:
            logging.warning("Finished parsing of 's3://{}/{}' with status code {}".format(bucket_name, object_key,
                                                                                          status_code))
    except Exception:
        logging.exception("Failed to upload 's3://{}/{}':".format(bucket_name, object_key))
        raise


def main(bucket, prefix, function_name):
    logging.info("Attempting to load log files from s3://{}/{} using {}".format(bucket, prefix, function_name))
    objects = list_objects(bucket, prefix)
    caller = partial(invoke_log_parser, function_name, bucket)
    with ThreadPoolExecutor(max_workers=20, thread_name_prefix='lambda_caller') as executor:
        executor.map(caller, objects)


if __name__ == "__main__":
    if len(sys.argv) != 4 or (len(sys.argv) > 0 and sys.argv[0] == "-h"):
        name = os.path.basename(sys.argv[0])
        print("Usage: {} <bucket_name> <prefix> <function_name>".format(name))
        sys.exit(0)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s (%(threadName)s) %(message)s")
    main(*sys.argv[1:4])
