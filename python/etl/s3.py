"""Common code around interacting with AWS S3."""

import concurrent.futures
import logging
import tempfile
import threading
import time
from datetime import datetime
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple, Union

import boto3
import botocore.exceptions
import botocore.response
import funcy
import simplejson as json
from tqdm import tqdm

import etl.timer
from etl.errors import ETLRuntimeError, S3ServiceError
from etl.json_encoder import FancyJsonEncoder

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_resources_for_thread = threading.local()


def _get_s3_bucket(bucket_name: str):
    """
    Return new Bucket object for a bucket that does exist.

    This bucket is a tied to a per-thread session with S3.
    """
    s3 = getattr(_resources_for_thread, "s3", None)
    if s3 is None:
        # When multi-threaded, we can't use the default session. So keep one per thread.
        session = boto3.session.Session()
        s3 = session.resource("s3")
        setattr(_resources_for_thread, "s3", s3)
    return s3.Bucket(bucket_name)


def upload_empty_object(bucket_name: str, object_key: str) -> None:
    """Create a key in an S3 bucket with no content."""
    try:
        logger.debug("Creating empty object in 's3://%s/%s'", bucket_name, object_key)
        bucket = _get_s3_bucket(bucket_name)
        obj = bucket.Object(object_key)
        obj.put()
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
        raise


class S3Uploader:
    """Upload files from local filesystem into the given S3 folder."""

    def __init__(self, bucket_name: str, callback=None, dry_run=False) -> None:
        self.bucket_name = bucket_name
        self.callback = callback or self._no_op
        self.dry_run = dry_run

    def _no_op(self):
        pass

    def __call__(self, filename: str, object_key: str) -> None:
        if self.dry_run:
            logger.debug("Dry-run: Skipping upload of '%s' to 's3://%s/%s'", filename, self.bucket_name, object_key)
            self.callback()
            return
        logger.debug("Uploading '%s' to 's3://%s/%s'", filename, self.bucket_name, object_key)
        try:
            bucket = _get_s3_bucket(self.bucket_name)
            bucket.upload_file(filename, object_key)
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            logger.error("Error code %s for object 's3://%s/%s'", error_code, self.bucket_name, object_key)
            raise
        except Exception:
            logger.error("Unknown error occurred during upload", exc_info=True)
            raise
        finally:
            self.callback()


def upload_data_to_s3(data: dict, bucket_name: str, object_key: str) -> None:
    """
    Write data object (formatted as JSON, readable as YAML) into an S3 object.

    Although we generally support YAML because it allows adding comments, we prefer
    the format and formatting of JSON.
    """
    uploader = S3Uploader(bucket_name)
    with tempfile.NamedTemporaryFile(mode="w+") as local_file:
        json.dump(data, local_file, cls=FancyJsonEncoder, indent="    ", sort_keys=True)
        local_file.write("\n")
        local_file.flush()
        uploader(local_file.name, object_key)


def _keep_common_path(paths: Iterable[str]) -> str:
    """
    Return common path shared in the object keys' path, formatted as a prefix (with a "/").

    When the list of paths is empty, an empty string is returned.

    >>> _keep_common_path(["production/schemas/dw/fact_order.sql"])
    'production/schemas/dw/fact_order.sql'
    >>> _keep_common_path(["production/schemas/dw/fact_order.sql", "production/schemas/dw/fact_order.yaml"])
    'production/schemas/dw/'
    >>> _keep_common_path(["production/schemas/dw/fact_order.yaml", "production/schemas/web_app/public-orders.yaml"])
    'production/schemas/'
    >>> _keep_common_path(["production/schemas/oops/longer_than_path/", "production/schemas/oops/lo"])
    'production/schemas/oops/'
    """
    common_path: Optional[str] = None
    for path in paths:
        if common_path is None:
            common_path = path
            continue
        if path.startswith(common_path):
            continue
        for i, c in enumerate(common_path):
            if i == len(path) or path[i] != c:
                common_prefix = common_path[:i]
                slash_index = common_prefix.rfind("/")
                common_path = common_prefix[: slash_index + 1]
                break
    return common_path or ""


def upload_files(files: Sequence[Tuple[str, str]], bucket_name: str, prefix: str, dry_run=False) -> None:
    """
    Upload local files to S3 from "local_name" to "s3://bucket_name/prefix/remote_name".

    The sequence of files must consist of tuples of ("local_name", "remote_name").
    """
    max_workers = min(len(files), 10)
    timer = etl.timer.Timer()

    common_path = _keep_common_path([object_key for _, object_key in files])
    description = "Uploading files to S3" if not dry_run else "Dry-run: Uploading files to S3"
    tqdm_bar = tqdm(desc=description, disable=None, leave=False, total=len(files), unit="file")
    uploader = S3Uploader(bucket_name, callback=tqdm_bar.update, dry_run=dry_run)

    # We break out the futures to be able to easily tally up errors.
    futures: List[concurrent.futures.Future] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="sync-parallel") as executor:
        for local_filename, remote_filename in files:
            futures.append(executor.submit(uploader.__call__, local_filename, f"{prefix}/{remote_filename}"))

    errors = 0
    for future in concurrent.futures.as_completed(futures):
        exception = future.exception()
        if exception is not None:
            logger.error("Failed to upload file: %s", exception)
            errors += 1

    tqdm_bar.close()
    what_happened = "Uploaded" if not dry_run else "Dry-run: Skipped uploading"
    logger.info(
        f"{what_happened} %d of %d file(s) to 's3://%s/%s/%s' using %d threads (%s)",
        len(files) - errors,
        len(files),
        bucket_name,
        prefix,
        common_path,
        max_workers,
        timer,
    )
    if errors:
        raise ETLRuntimeError(f"There were {errors} error(s) during upload")


def delete_objects(bucket_name: str, object_keys: Sequence[str], wait=False, _retry=True, dry_run=False) -> None:
    """
    For each object key in object_keys, attempt to delete the key and its content from an S3 bucket.

    If the optional parameter "wait" is true, then we'll wait until the object has actually been
    deleted.
    """
    bucket = _get_s3_bucket(bucket_name)
    chunk_size = min(100, len(object_keys))  # seemed reasonable at the time
    timer = etl.timer.Timer()

    common_prefix = _keep_common_path(object_keys)
    description = "Deleting files in S3" if not dry_run else "Dry-run: Deleting files in S3"
    tqdm_bar = tqdm(desc=description, disable=None, leave=False, total=len(object_keys), unit="file")

    failed: List[str] = []
    for this_chunk in funcy.chunks(chunk_size, object_keys):
        if dry_run:
            for key in this_chunk:
                logger.debug("Dry-run: Skipped deleting 's3://%s/%s", bucket_name, key)
                tqdm_bar.update()
            continue

        result = bucket.delete_objects(Delete={"Objects": [{"Key": key} for key in this_chunk]})
        for deleted in sorted(obj["Key"] for obj in result.get("Deleted", [])):
            logger.debug("Deleted 's3://%s/%s'", bucket_name, deleted)
            tqdm_bar.update()
        for error in result.get("Errors", []):
            logger.error(
                "Failed to delete 's3://%s/%s' with %s: %s", bucket_name, error["Key"], error["Code"], error["Message"]
            )
            failed.append(error["Key"])
            tqdm_bar.update()
    tqdm_bar.close()

    what_happened = "Deleted" if not dry_run else "Dry-run: Skipped deleting"
    logger.info(
        f"{what_happened} %d of %d file(s) in 's3://%s/%s' (%s)",
        len(object_keys) - len(failed),
        len(object_keys),
        bucket_name,
        common_prefix,
        timer,
    )
    if failed:
        if not _retry:
            raise S3ServiceError("Failed to delete %d file(s)" % len(failed))
        logger.warning("Failed to delete %d object(s), trying one more time in 5s", len(failed))
        time.sleep(5)
        delete_objects(bucket_name, failed, _retry=False)

    if wait and _retry:  # wait only in initial call
        logger.info("Waiting for %d object(s) to no longer exist", len(object_keys))
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
        error_code = exc.response["Error"]["Code"]
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


def wait_until_object_exists(bucket_name: str, object_key: str) -> None:
    bucket = _get_s3_bucket(bucket_name)
    bucket.Object(object_key).wait_until_exists()


def get_s3_object_content(bucket_name: str, object_key: str) -> botocore.response.StreamingBody:
    """
    Return stream for content of s3://bucket_name/object_key .

    You must close the stream when you're done with it.
    """
    bucket = _get_s3_bucket(bucket_name)
    s3_object = bucket.Object(object_key)
    try:
        response = s3_object.get()
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error("Error code %s for object 's3://%s/%s'", error_code, bucket_name, object_key)
        raise
    except Exception:
        logger.error("Failed to download 's3://%s/%s'", bucket_name, object_key)
        raise

    logger.debug(
        "Received object: 's3://%s/%s', last modified: %s, content length: %s",
        bucket_name,
        object_key,
        response["LastModified"],
        response["ContentLength"],
    )
    return response["Body"]


def list_objects_for_prefix(bucket_name: str, *prefixes: str) -> Iterator[str]:
    """
    List all the files in "s3://{bucket_name}/{prefix}" for each given prefix.

    The prefix is probably a path and not an object key.
    """
    if not prefixes:
        raise ValueError("List of prefixes may not be empty")
    bucket = _get_s3_bucket(bucket_name)
    for prefix in prefixes:
        logger.info("Looking for files in 's3://%s/%s/'", bucket_name, prefix)
        for obj in bucket.objects.filter(Prefix=prefix):
            yield obj.key


def test_object_creation(bucket_name: str, prefix: str) -> None:
    object_key = "{}/_s3_test".format(prefix.rstrip("/"))
    logger.info("Testing object creation and deletion using 's3://%s/%s'", bucket_name, object_key)
    upload_empty_object(bucket_name, object_key)
    wait_until_object_exists(bucket_name, object_key)
    get_s3_object_last_modified(bucket_name, object_key, wait=True)
    delete_objects(bucket_name, [object_key], wait=True)


if __name__ == "__main__":
    import sys

    import etl.config.log

    if len(sys.argv) != 3:
        print("Usage: {} bucket_name prefix".format(sys.argv[0]))
        print("This will create a test object under s3://[bucket_name]/[prefix] and delete afterwards.")
        sys.exit(1)

    etl.config.log.configure_logging(log_level="DEBUG")
    test_object_creation(sys.argv[1], sys.argv[2])
