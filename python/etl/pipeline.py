"""Implement commands that interact with AWS Data Pipeline."""

import fnmatch
import logging
import os
import textwrap
from datetime import datetime, timedelta
from operator import attrgetter
from typing import Dict, List

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
        self.name = description.get("name")
        self.object_id = description["id"]
        self.ref_values = {field["key"]: field["refValue"] for field in description["fields"] if "refValue" in field}
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }

    def __lt__(self, other):
        """Sort by actual end time, then start time, then status, finally on name."""
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
    def component_parent(self):
        return self.ref_values.get("@componentParent")

    @property
    def error_id(self):
        return self.string_values.get("errorId")

    @property
    def error_message(
        self,
    ):
        return self.string_values.get("errorMessage")

    @property
    def object_type(self):
        return self.string_values.get("type")

    @property
    def scheduled_start_time(self):
        return self.string_values.get("@scheduledStartTime")

    @property
    def status(self):
        return self.string_values.get("@status")


class DataPipeline:
    client = boto3.client("datapipeline")

    def __init__(self, description):
        self.name = description["name"]
        self.pipeline_id = description["pipelineId"]
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }
        self.tags = description["tags"]

    def __str__(self):
        return "DataPipeline('{}','{}')".format(self.pipeline_id, self.name)

    def describe_objects(self, evaluate_expressions=False) -> dict:
        """Desribe all objects (components, instances, and attempts) for this pipeline."""
        return {
            sphere.lower(): {
                object.object_id: {
                    "name": object.name,
                    "ref_values": object.ref_values.copy(),
                    "string_values": object.string_values.copy(),
                }
                for object in self.objects(sphere, evaluate_expressions)
            }
            for sphere in ("COMPONENT", "INSTANCE", "ATTEMPT")
        }

    def find_attempts_with_errors(self, instances):
        """Return head attempts for given instances when there was an error."""
        attempts = self.objects("ATTEMPT")
        head_attempts = frozenset(
            instance.ref_values["@headAttempt"] for instance in instances if "@headAttempt" in instance.ref_values
        )
        for attempt in attempts:
            if attempt.object_id in head_attempts and "errorId" in attempt.string_values:
                yield attempt

    def latest_instances(self):
        """Group instances by their component and and return latest within each group."""
        instances = self.objects("INSTANCE")
        grouped = funcy.group_by(attrgetter("component_parent"), instances)
        logger.info(
            "Pipeline '%s' has %d components and %d instances, looking for latest instances",
            self.pipeline_id,
            len(grouped),
            sum(map(len, grouped.values())),  # sum based on "grouped" b/c "instances" is generator
        )
        for component_parent in sorted(grouped):
            yield sorted(grouped[component_parent], key=attrgetter("scheduled_start_time"))[0]

    def objects(self, sphere, evaluate_expressions=False):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 objects
        object_ids = self._instance_ids(sphere)
        paginator = self.client.get_paginator("describe_objects")
        # Evaluation fails for components so block the flag here.
        evaluate_expressions = evaluate_expressions and sphere in ("ATTEMPT", "INSTANCE")
        for ids_chunk in funcy.chunks(chunk_size, object_ids):
            response_iterator = paginator.paginate(
                pipelineId=self.pipeline_id, objectIds=ids_chunk, evaluateExpressions=evaluate_expressions
            )
            for pipeline_object in response_iterator.search("pipelineObjects[]"):
                yield DataPipelineObject(pipeline_object)

    @property
    def finished_time(self):
        return self.string_values.get("@finishedTime")

    @property
    def health_status(self):
        return self.string_values.get("@healthStatus")

    @property
    def state(self):
        return self.string_values.get("@pipelineState")

    def _instance_ids(self, sphere):
        paginator = self.client.get_paginator("query_objects")
        response_iterator = paginator.paginate(pipelineId=self.pipeline_id, sphere=sphere)
        for response in response_iterator:
            for id in response["ids"]:
                yield id

    @staticmethod
    def as_json(pipelines) -> str:
        """Return information about pipelines in JSON-formatted string."""
        # We avoid a dict comprehension here so that we can log per pipeline.
        obj: Dict[str, List] = {"pipelines": []}
        for pipeline in pipelines:
            logger.info("Collecting all objects belonging to pipeline '%s'", pipeline.pipeline_id)
            obj["pipelines"].append(
                {
                    pipeline.pipeline_id: {
                        "name": pipeline.name,
                        "objects": pipeline.describe_objects(evaluate_expressions=True),
                        "string_values": pipeline.string_values,
                        "tags": pipeline.tags,
                    }
                }
            )
        return json.dumps(obj, indent="    ", sort_keys=True)

    @classmethod
    def delete_pipeline(cls, pipeline_id) -> str:
        response = cls.client.delete_pipeline(pipelineId=pipeline_id)
        return response["ResponseMetadata"]["RequestId"]

    @classmethod
    def describe_pipelines(cls, pipeline_ids: List[str]):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 pipelines
        for ids_chunk in funcy.chunks(chunk_size, pipeline_ids):
            resp = cls.client.describe_pipelines(pipelineIds=ids_chunk)
            for description in resp["pipelineDescriptionList"]:
                yield description

    @classmethod
    def list_all_pipelines(cls):
        paginator = cls.client.get_paginator("list_pipelines")
        response_iterator = paginator.paginate()
        return response_iterator.search("pipelineIdList[].id")


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
    instances = sorted(pipeline.latest_instances())
    attempts_with_errors = sorted(pipeline.find_attempts_with_errors(instances))

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
                for instance in sorted(instances)
            ],
            header_row=["Instance Name", "Type", "Status", "Actual Start Time", "Actual End Time"],
            max_column_width=80,
        )
    )

    for i, attempt in enumerate(attempts_with_errors):
        if i == 0:
            print()
        print("*** {}: {} ***".format(attempt.name, attempt.error_id))
        print(textwrap.indent(attempt.error_message, "| ", lambda line: True))


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
