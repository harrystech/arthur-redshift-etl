"""Implement commands that interact with AWS Data Pipeline."""

import fnmatch
import logging
import os
from datetime import datetime, timedelta
from operator import attrgetter
from typing import List

import boto3
import funcy
import simplejson as json

import etl.text
from etl.text import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


class DataPipelineObject:
    def __init__(self, description):
        self.object_id = description["id"]
        self.name = description.get("name")
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }
        self.ref_values = {field["key"]: field["refValue"] for field in description["fields"] if "refValue" in field}

    def __lt__(self, other):
        if self.actual_end_time != other.actual_end_time:
            if self.actual_end_time is None:
                return False
            if other.actual_end_time is None:
                return True
            return self.actual_end_time < other.actual_end_time
        if self.actual_start_time != other.actual_start_time:
            if self.actual_start_time is None:
                return False
            if other.actual_start_time is None:
                return True
            return self.actual_start_time < other.actual_start_time
        if self.status != other.status:
            return self.status < other.status
        return self.name < other.name

    @property
    def actual_end_time(self):
        return self.string_values.get("@actualEndTime")

    @property
    def actual_start_time(self):
        return self.string_values.get("@actualStartTime")

    @property
    def attempt_count(self):
        return self.string_values.get("@attemptCount")

    @property
    def object_type(self):
        return self.string_values.get("type")

    @property
    def status(self):
        return self.string_values.get("@status")


class DataPipeline:
    client = boto3.client("datapipeline")

    def __init__(self, description):
        self.pipeline_id = description["pipelineId"]
        self.name = description["name"]
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }
        self.tags = description["tags"]

    def __str__(self):
        return "DataPipeline('{}','{}')".format(self.pipeline_id, self.name)

    def describe_objects(self) -> dict:
        """Desribe all objects (components, instances, and attempts) for this pipeline."""
        return {
            sphere.lower(): {
                object.object_id: {
                    "string_values": object.string_values.copy(),
                    "name": object.name,
                    "ref_values": object.ref_values.copy(),
                }
                for object in self.objects(sphere)
            }
            for sphere in ("COMPONENT", "INSTANCE", "ATTEMPT")
        }

    @staticmethod
    def as_json(pipelines) -> str:
        """Return information about pipelines in JSON-formatted string."""
        obj = {
            "pipelines": {
                pipeline.pipeline_id: {
                    "name": pipeline.name,
                    "string_values": pipeline.string_values.copy(),
                    "tags": pipeline.tags.copy(),
                    "objects": pipeline.describe_objects(),
                }
                for pipeline in pipelines
            }
        }
        return json.dumps(obj, indent="    ", sort_keys=True)

    @classmethod
    def delete_pipeline(cls, pipeline_id) -> str:
        response = cls.client.delete_pipeline(pipelineId=pipeline_id)
        return response["ResponseMetadata"]["RequestId"]

    @property
    def finished_time(self):
        return self.string_values.get("@finishedTime")

    @property
    def health_status(self):
        return self.string_values.get("@healthStatus")

    @property
    def state(self):
        return self.string_values.get("@pipelineState")

    @classmethod
    def list_all_pipelines(cls):
        paginator = cls.client.get_paginator("list_pipelines")
        response_iterator = paginator.paginate()
        return response_iterator.search("pipelineIdList[].id")

    @classmethod
    def describe_pipelines(cls, pipeline_ids: List[str]):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 pipelines
        for ids_chunk in funcy.chunks(chunk_size, pipeline_ids):
            resp = cls.client.describe_pipelines(pipelineIds=ids_chunk)
            for description in resp["pipelineDescriptionList"]:
                yield description

    def _instance_ids(self, sphere):
        paginator = self.client.get_paginator("query_objects")
        response_iterator = paginator.paginate(pipelineId=self.pipeline_id, sphere=sphere)
        for response in response_iterator:
            for id in response["ids"]:
                yield id

    def objects(self, sphere):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 objects
        object_ids = self._instance_ids(sphere)
        paginator = self.client.get_paginator("describe_objects")
        for ids_chunk in funcy.chunks(chunk_size, object_ids):
            response_iterator = paginator.paginate(pipelineId=self.pipeline_id, objectIds=ids_chunk)
            for pipeline_object in response_iterator.search("pipelineObjects[]"):
                yield DataPipelineObject(pipeline_object)


def list_pipelines(selection: List[str]) -> List[DataPipeline]:
    """
    Return list of pipelines related to this project (which must have the tag for our project set).

    The :selection should be a list of glob patterns to select specific pipelines by their ID.
    If the selection is an empty list, then all pipelines are used.
    """
    all_pipeline_ids = DataPipeline.list_all_pipelines()
    if selection:
        selected_pipeline_ids = [
            pipeline_id for pipeline_id in all_pipeline_ids for glob in selection if fnmatch.fnmatch(pipeline_id, glob)
        ]
    else:
        selected_pipeline_ids = list(all_pipeline_ids)

    pipelines = []
    for description in DataPipeline.describe_pipelines(selected_pipeline_ids):
        for tag in description["tags"]:
            if tag["key"] == "user:project" and tag["value"] == "data-warehouse":
                pipelines.append(DataPipeline(description))
                break
    return sorted(pipelines, key=attrgetter("name"))


def show_pipelines(selection: List[str], as_json=False) -> None:
    """
    List the currently installed pipelines, possibly using a subset based on the selection pattern.

    If "as json" is chosen, then the output is JSON-formatted
    and includes all fields, not just the ones shown in the tables.

    Without a selection, prints an overview of the pipelines.
    With selection of a single pipeline, digs into details of that selected pipeline.
    """
    pipelines = list_pipelines(selection)

    if not pipelines:
        if selection:
            logger.warning("Found no pipelines matching glob pattern")
        else:
            logger.warning("Found no pipelines")
        if not as_json:
            print("*** No pipelines found ***")
        return

    if selection:
        if len(pipelines) > 1:
            logger.warning("Selection matched more than one pipeline")
        logger.info(
            "Currently selected pipelines: %s",
            join_with_quotes(pipeline.pipeline_id for pipeline in pipelines),
        )
    else:
        logger.info(
            "Available pipelines: %s",
            join_with_quotes(pipeline.pipeline_id for pipeline in pipelines),
        )
    if as_json:
        print(DataPipeline.as_json(pipelines))
        return

    print(
        etl.text.format_lines(
            [
                (pipeline.pipeline_id, pipeline.name, pipeline.health_status or "---", pipeline.state or "---")
                for pipeline in pipelines
            ],
            header_row=["Pipeline ID", "Name", "Health", "State"],
            max_column_width=80,
        )
    )
    # Show additional details only if we're looking at a single pipeline.
    if len(pipelines) != 1:
        return

    pipeline = pipelines.pop()
    print()
    print(
        etl.text.format_lines(
            [
                [key, pipeline.string_values[key]]
                for key in sorted(
                    funcy.project(
                        pipeline.string_values,
                        [
                            "@creationTime",
                            "@healthStatus",
                            "@healthStatusUpdatedTime",
                            "@id",
                            "@lastActivationTime",
                            "@latestRunTime",
                            "@pipelineState",
                            "@scheduledEndTime",
                            "@scheduledStartTime",
                            "uniqueId",
                        ],
                    )
                )
            ],
            header_row=["Key", "Value"],
        )
    )
    print()
    print(
        etl.text.format_lines(
            [
                (
                    instance.name,
                    instance.object_type,
                    instance.status,
                    instance.actual_start_time or "---",
                    instance.actual_end_time or "---",
                )
                for instance in sorted(pipeline.objects("INSTANCE"))
            ],
            header_row=["Instance Name", "Type", "Status", "Actual Start Time", "Actual End Time"],
            max_column_width=80,
        )
    )


def delete_finished_pipelines(selection: List[str], dry_run=False) -> None:
    """
    Delete pipelines that finished more than 24 hours ago.

    You can use this to easily clean up (validation or pizza) pipelines.
    The 24-hour cool off period suggests not to delete pipelines that are still needed for
    inspection.
    """
    yesterday_or_before = (datetime.utcnow() - timedelta(days=1)).replace(microsecond=0)
    earliest_finished_time = yesterday_or_before.isoformat()

    pipelines = [
        pipeline
        for pipeline in list_pipelines(selection)
        if pipeline.state == "FINISHED" and pipeline.finished_time < earliest_finished_time
    ]
    if not pipelines:
        logger.warning("Found no finished pipelines (older than %s)", earliest_finished_time)
        return

    for pipeline in pipelines:
        if dry_run:
            logger.info("Skipping deletion of pipeline '%s': %s", pipeline.pipeline_id, pipeline.name)
        else:
            logger.info("Trying to delete pipeline '%s': %s", pipeline.pipeline_id, pipeline.name)
            response = DataPipeline.delete_pipeline(pipeline.pipeline_id)
            logger.info(
                "Succeeded to delete '%s' (request_id=%s)",
                pipeline.pipeline_id,
                response,
            )
