"""
Common code around interacting with AWS S3
"""

import boto3
import botocore.exceptions
import botocore.response
import logging
import simplejson as json
import tempfile
import threading

from typing import Iterator, List, Union, Tuple
from datetime import datetime

import etl
from etl.json_encoder import FancyJsonEncoder


_resources_for_thread = threading.local()


class S3ServiceError(etl.ETLError):
    pass


def _get_s3_bucket(bucket_name: str):
    """
    Return new Bucket object for a bucket that does exist (waits until it does)

    This bucket is a tied to a per-thread session with S3.
    """
    s3 = getattr(_resources_for_thread, 's3', None)
    if s3 is None:
        # When multi-threaded, we can't use the default session. So keep one per thread.
        session = boto3.session.Session()
        s3 = session.resource("s3")
        setattr(_resources_for_thread, 's3', s3)
    return s3.Bucket(bucket_name)


def upload_to_s3(filename: str, bucket_name: str, object_key: str) -> None:
    """
    Upload the contents of a file to the given keyspace in an S3 bucket
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Uploading '%s' to 's3://%s/%s'", filename, bucket_name, object_key)
        bucket = _get_s3_bucket(bucket_name)
        bucket.upload_file(filename, object_key)
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response['Error']['Code']
        logger.error("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
        raise


def upload_empty_object(bucket_name: str, object_key: str) -> None:
    """
    Create a key in an S3 bucket with no content
    """
    logger = logging.getLogger(__name__)
    try:
        logger.debug("Creating empty 's3://%s/%s'", bucket_name, object_key)
        bucket = _get_s3_bucket(bucket_name)
        obj = bucket.Object(object_key)
        obj.put()
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response['Error']['Code']
        logger.error("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
        raise


def upload_data_to_s3(data: dict, bucket_name: str, object_key: str) -> None:
    """
    Write data object (formatted as JSON, readable as YAML) into an S3 object.
    """
    with tempfile.NamedTemporaryFile(mode="w+") as local_file:
        json.dump(data, local_file, indent="    ", sort_keys=True, cls=FancyJsonEncoder)
        local_file.write('\n')
        local_file.flush()
        upload_to_s3(local_file.name, bucket_name, object_key)


def delete_objects(bucket_name: str, object_keys: List[str], _retry: bool=True) -> None:
    """
    For each object key in object_keys, attempt to delete the key and it's content from an S3 bucket
    """
    logger = logging.getLogger(__name__)
    bucket = _get_s3_bucket(bucket_name)
    keys = [{'Key': key} for key in object_keys]
    failed = []
    chunk_size = 1000
    while len(keys) > 0:
        result = bucket.delete_objects(Delete={'Objects': keys[:chunk_size]})
        del keys[:chunk_size]
        for deleted in sorted(obj['Key'] for obj in result.get('Deleted', [])):
            logger.info("Deleted 's3://%s/%s'", bucket_name, deleted)
        for error in result.get('Errors', []):
            logger.error("Failed to delete 's3://%s/%s' with %s: %s", bucket_name, error['Key'],
                         error['Code'], error['Message'])
            failed.append(error['Key'])
    if failed:
        if _retry:
            logger.warning("Failed to delete %d objects, trying one more time", len(failed))
            delete_objects(bucket_name, failed, _retry=False)
        else:
            raise S3ServiceError("Failed to delete %d files" % len(failed))


def get_s3_object_last_modified(bucket_name: str, object_key: str, wait: bool=True) -> Union[datetime, None]:
    """
    Return the last_modified datetime timestamp for an S3 Object
    If the call errors out, return None
    """
    logger = logging.getLogger(__name__)
    try:
        bucket = _get_s3_bucket(bucket_name)
        s3_object = bucket.Object(object_key)
        if wait:
            s3_object.wait_until_exists()
        timestamp = s3_object.last_modified
        logger.debug("Object in 's3://%s/%s' was last modified %s", bucket_name, object_key, timestamp)
    except botocore.exceptions.WaiterError:
        logger.debug("Waiting for object in 's3://%s/%s' failed", bucket_name, object_key)
        timestamp = None
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response['Error']['Code']
        # FIXME We're mixing codes and statuses here
        if error_code == "404" or error_code == "NoSuchKey":
            logger.debug("Object 's3://%s/%s' does not exist", bucket_name, object_key)
            timestamp = None
        else:
            logger.warning("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
            raise
    return timestamp


def object_stat(bucket_name: str, object_key: str) -> Tuple[int, datetime]:
    """
    Return content_length and last_modified timestamp from the object.
    It is an error if the object does not exist.
    """
    bucket = _get_s3_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    return s3_object.content_length, s3_object.last_modified


def get_s3_object_content(bucket_name: str, object_key: str) -> botocore.response.StreamingBody:
    """
    Return stream for content of s3://bucket_name/object_key

    You must close the stream when you're done with it.
    """
    logger = logging.getLogger(__name__)
    logger.debug("Downloading 's3://%s/%s'", bucket_name, object_key)
    bucket = _get_s3_bucket(bucket_name)
    try:
        s3_object = bucket.Object(object_key)
        response = s3_object.get()
        logger.debug("Received response from S3: last modified: %s, content length: %s, content type: %s",
                     response['LastModified'], response['ContentLength'], response['ContentType'])
        return response['Body']
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response['Error']['Code']
        logger.error("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
        raise


def list_objects_for_prefix(bucket_name: str, *prefixes: str) -> Iterator[str]:
    """
    List all the files in "s3://{bucket_name}/{prefix}" for each given prefix
    (where prefix is probably a path and not an object key).
    """
    logger = logging.getLogger(__name__)
    if not prefixes:
        raise ValueError("List of prefixes may not be empty")
    bucket = _get_s3_bucket(bucket_name)
    for prefix in prefixes:
        logger.info("Looking for files at 's3://%s/%s'", bucket_name, prefix)
        for obj in bucket.objects.filter(Prefix=prefix):
            yield obj.key
