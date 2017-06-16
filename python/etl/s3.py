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
import time

from typing import Iterator, List, Union, Tuple
from datetime import datetime

from etl.json_encoder import FancyJsonEncoder
from etl.errors import S3ServiceError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_resources_for_thread = threading.local()


def _get_s3_bucket(bucket_name: str):
    """
    Return new Bucket object for a bucket that does exist.

    This bucket is a tied to a per-thread session with S3.
    """
    s3 = getattr(_resources_for_thread, 's3', None)
    if s3 is None:
        # When multi-threaded, we can't use the default session. So keep one per thread.
        session = boto3.session.Session()
        s3 = session.resource("s3")
        setattr(_resources_for_thread, 's3', s3)
    return s3.Bucket(bucket_name)


class S3Uploader:

    """
    Upload files from local filesystem into the given S3 folder.

    Note that the current implementation is NOT thread safe since
    the bucket resource is tied to the s3 resource of one thread.
    """

    def __init__(self, bucket_name: str, dry_run=False) -> None:
        self.logger = logging.getLogger(__name__)
        self.bucket_name = bucket_name
        self._bucket = _get_s3_bucket(self.bucket_name)
        if dry_run:
            self._call = self.skip_upload
        else:
            self._call = self.do_upload

    def skip_upload(self, filename: str, object_key: str) -> None:
        self.logger.info("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", filename, self.bucket_name, object_key)

    def do_upload(self, filename: str, object_key: str) -> None:
        try:
            self.logger.info("Uploading '%s' to 's3://%s/%s'", filename, self.bucket_name, object_key)
            self._bucket.upload_file(filename, object_key)
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response['Error']['Code']
            self.logger.error("Error code %s for object 's3://%s/%s'", error_code, self.bucket_name, object_key)
            raise

    def __call__(self, filename: str, object_key: str) -> None:
        self._call(filename, object_key)


def upload_empty_object(bucket_name: str, object_key: str) -> None:
    """
    Create a key in an S3 bucket with no content
    """
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

    Although we generally support YAML because it allows adding comments, we prefer
    the format and formatting of JSON.
    """
    uploader = S3Uploader(bucket_name)
    with tempfile.NamedTemporaryFile(mode="w+") as local_file:
        json.dump(data, local_file, indent="    ", sort_keys=True, cls=FancyJsonEncoder)
        local_file.write('\n')  # type: ignore
        local_file.flush()
        uploader(local_file.name, object_key)


def delete_objects(bucket_name: str, object_keys: List[str], wait=False, _retry=True) -> None:
    """
    For each object key in object_keys, attempt to delete the key and its content from an S3 bucket.

    If the optional parameter "wait" is true, then we'll wait until the object has actually been deleted.
    """
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
            logger.warning("Failed to delete %d object(s), trying one more time in 5s", len(failed))
            time.sleep(5)
            delete_objects(bucket_name, failed, _retry=False)
        else:
            raise S3ServiceError("Failed to delete %d file(s)" % len(failed))
    if wait and _retry:  # check only in initial call
        for key in object_keys:
            bucket.Object(key).wait_until_not_exists()


def get_s3_object_last_modified(bucket_name: str, object_key: str, wait=True) -> Union[datetime, None]:
    """
    Return the last_modified datetime timestamp for an S3 Object.
    If the call errors out, return None.
    """
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
        if error_code == "404":
            logger.debug("Object 's3://%s/%s' does not exist", bucket_name, object_key)
            timestamp = None
        else:
            logger.warning("Failed to find 's3://%s/%s' (%s)", bucket_name, object_key, error_code)
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
    if not prefixes:
        raise ValueError("List of prefixes may not be empty")
    bucket = _get_s3_bucket(bucket_name)
    for prefix in prefixes:
        logger.info("Looking for files at 's3://%s/%s'", bucket_name, prefix)
        for obj in bucket.objects.filter(Prefix=prefix):
            yield obj.key


def test_object_creation(bucket_name: str, prefix: str) -> None:
    object_key = "{}/_s3_test".format(prefix.rstrip('/'))
    logger.info("Testing object creation and deletion using 's3://%s/%s'", bucket_name, object_key)
    upload_empty_object(bucket_name, object_key)
    get_s3_object_last_modified(bucket_name, object_key, wait=True)
    delete_objects(bucket_name, [object_key], wait=True)


if __name__ == "__main__":
    import sys
    import etl.config

    if len(sys.argv) != 3:
        print("Usage: {} bucket_name prefix".format(sys.argv[0]))
        print("This will create a test object under s3://[bucket_name]/[prefix] and delete afterwards.")
        sys.exit(1)

    etl.config.configure_logging(log_level="DEBUG")
    test_object_creation(sys.argv[1], sys.argv[2])
