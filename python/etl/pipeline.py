"""Implement commands that interact with AWS Data Pipeline."""

import fnmatch
import logging
from datetime import datetime, timedelta
from operator import attrgetter
from typing import List

import boto3
import funcy

import etl.text
from etl.text import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataPipelineInstance:
    def __init__(self, description):
        self.instance_id = description["id"]
        self.name = description["name"]
        self.fields = {field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field}

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
        return self.fields.get("@actualEndTime")

    @property
    def actual_start_time(self):
        return self.fields.get("@actualStartTime")

    @property
    def attempt_count(self):
        return self.fields.get("@attemptCount")

    @property
    def instance_type(self):
        return self.fields.get("type")

    @property
    def status(self):
        return self.fields.get("@status")


class DataPipeline:
    client = boto3.client("datapipeline")

    def __init__(self, description):
        self.pipeline_id = description["pipelineId"]
        self.name = description["name"]
        self.fields = {
            field["key"]: field["stringValue"]
            for field in description["fields"]
            if field["key"] != "*tags"  # tags are an ugly list of dicts
        }

    def __str__(self):
        return "DataPipeline('{}','{}')".format(self.pipeline_id, self.name)

    @property
    def health_status(self):
        return self.fields.get("@healthStatus", "---")

    @property
    def state(self):
        return self.fields.get("@pipelineState", "---")

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

    def _instance_ids(self):
        paginator = self.client.get_paginator("query_objects")
        response_iterator = paginator.paginate(pipelineId=self.pipeline_id, sphere="INSTANCE")
        for response in response_iterator:
            for id in response["ids"]:
                yield id

    def instances(self):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 objects
        object_ids = self._instance_ids()
        paginator = self.client.get_paginator("describe_objects")
        for ids_chunk in funcy.chunks(chunk_size, object_ids):
            response_iterator = paginator.paginate(pipelineId=self.pipeline_id, objectIds=ids_chunk)
            for pipeline_object in response_iterator.search("pipelineObjects[]"):
                yield DataPipelineInstance(pipeline_object)


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


def show_pipelines(selection: List[str]) -> None:
    """
    List the currently installed pipelines, possibly using a subset based on the selection pattern.

    Without a selection, prints an overview of the pipelines.
    With a selection, digs into details of each selected pipeline.
    """
    pipelines = list_pipelines(selection)

    if not pipelines:
        if selection:
            logger.warning("Found no pipelines matching glob pattern")
        else:
            logger.warning("Found no pipelines")
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

    print(
        etl.text.format_lines(
            [(pipeline.pipeline_id, pipeline.name, pipeline.health_status, pipeline.state) for pipeline in pipelines],
            header_row=["Pipeline ID", "Name", "Health", "State"],
            max_column_width=80,
        )
    )
    # Show additional details only if we're looking at a single pipeline.
    if len(pipelines) == 1:
        pipeline = pipelines[0]
        print()
        print(
            etl.text.format_lines(
                [
                    [key, pipeline.fields[key]]
                    for key in sorted(
                        funcy.project(
                            pipeline.fields,
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
                        instance.instance_type,
                        instance.status,
                        instance.actual_start_time or "---",
                        instance.actual_end_time or "---",
                    )
                    for instance in sorted(pipeline.instances())
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
        if pipeline.fields["@pipelineState"] == "FINISHED"
        and pipeline.fields.get("@finishedTime") < earliest_finished_time
    ]
    if not pipelines:
        logger.warning("Found no finished pipelines (older than %s)", earliest_finished_time)
        return

    client = boto3.client("datapipeline")
    for pipeline in pipelines:
        if dry_run:
            logger.info("Skipping deletion of pipeline '%s': %s", pipeline.pipeline_id, pipeline.name)
        else:
            logger.info("Trying to delete pipeline '%s': %s", pipeline.pipeline_id, pipeline.name)
            response = client.delete_pipeline(pipelineId=pipeline.pipeline_id)
            logger.info(
                "Succeeded to delete '%s' (request_id=%s)",
                pipeline.pipeline_id,
                response["ResponseMetadata"]["RequestId"],
            )
