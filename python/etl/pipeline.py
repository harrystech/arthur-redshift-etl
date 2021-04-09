"""
Pipelines allow us to coordinate the steps of an ETL.

ETL steps can be run in series, as necessary, or in parallel, when possible,
while also tracking failures, logs, etc.
Pipelines are currently based on AWS Data Pipeline.

Starting pipelines:
  Use one of the installation scripts to put a pipeline on a schedule. Usually, they run on a
  24-hour cycle. When in doubt, use "--help" on the script to see usage information.

Observing pipelines:
  Use the "show_pipelines" command in Arthur to see a list of currently defined pipelines.
  If you filter down to just one (by giving the command the pipeline id), you will see
  more details from instances and latest attempts.

Stopping, re-starting, etc.
  One-shot pipelines should be 'garbage-collected' periodically using the
  "delete_finished_pipelines" command in Arthur.

  To delete a pipeline that finished but failed or one that is no longer needed, use the AWS CLI
  directly:
      aws datapipeline delete-pipeline --pipeline-id df-0T9R7A0PDPWERD5QXJN4

  If you need to briefly pause a pipeline, you can also fall back the AWS CLI:
      aws datapipeline deactivate-pipeline --pipeline-id df-02F3R2A1N7C5EZRXR3NR
      aws datapipeline activate-pipeline --pipeline-id df-02F3R2A1N7C5EZRXR3NR

  The "show_pipelines" command will give you the pipeline ids needed here.
"""

import fnmatch
import logging
import textwrap
from datetime import datetime, timedelta
from operator import attrgetter
from typing import Iterable, List, Optional, Sequence

import arrow
import boto3
import funcy
import simplejson as json

import etl.text
from etl.text import join_with_single_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataPipelineObject:
    def __init__(self, description) -> None:
        self.name = description.get("name")
        self.object_id = description["id"]
        self.ref_values = {field["key"]: field["refValue"] for field in description["fields"] if "refValue" in field}
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }
        self.parent_object: Optional["DataPipelineObject"] = None

    STATUS_ORDER = {
        status: index
        for index, status in enumerate(
            [
                "FINISHED",
                "FAILED",
                "CASCADE_FAILED",
                "CANCELED",
                "RUNNING",
                "CREATING",
                "WAITING_FOR_RUNNER",
                "WAITING_ON_DEPENDENCIES",
                "TIMEDOUT",
            ]
        )
    }

    def __lt__(self, other):
        """Sort by status, actual end time, then actual start time, finally on name."""
        if self.status != other.status:
            return self.STATUS_ORDER.get(self.status, 9) < self.STATUS_ORDER.get(other.status, 9)
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
        return self.name < other.name

    @property
    def actual_end_time(self):
        return self.string_values.get("@actualEndTime")

    @property
    def actual_start_time(self):
        return self.string_values.get("@actualStartTime")

    @property
    def actual_elapsed(self):
        if self.string_values.get("@status") in ("CREATING", "RUNNING"):
            end_time = arrow.now()
        else:
            end_time = self.actual_end_time
        start_time = self.actual_start_time
        if not (end_time and start_time):
            return None

        parsed_end_time = arrow.get(end_time)
        parsed_start_time = arrow.get(start_time)
        elapsed_hours = (parsed_end_time - parsed_start_time).total_seconds() // 60 // 60
        if elapsed_hours < 1:
            # TODO(tom): Once we're on Python 3.8, we should add 'Final["minute"]' etc.
            return parsed_end_time.humanize(parsed_start_time, granularity=["minute"], only_distance=True)
        if elapsed_hours < 12:
            return parsed_end_time.humanize(parsed_start_time, granularity=["hour", "minute"], only_distance=True)
        return parsed_end_time.humanize(parsed_start_time, granularity=["hour"], only_distance=True)

    @property
    def attempt_count(self):
        return self.string_values.get("@attemptCount")

    @property
    def command(self):
        """Return command of the component (or the parent component for instances and attempts)."""
        if self.string_values.get("@sphere") in ("ATTEMPT", "INSTANCE"):
            return self.parent_object.command
        return self.string_values.get("command")

    @property
    def component_parent(self):
        return self.ref_values.get("@componentParent")

    @property
    def error_stack_trace(self):
        return self.string_values.get("errorStackTrace")

    @property
    def log_location(self):
        return self.string_values.get("@logLocation")

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
    def __init__(self, client, description) -> None:
        """
        Initialize an instance that describes a Data Pipeline in AWS.

        We keep a reference to the client here so that we can pull more information
        from AWS as needed.
        """
        self.name = description["name"]
        self.pipeline_id = description["pipelineId"]
        self.string_values = {
            field["key"]: field["stringValue"] for field in description["fields"] if "stringValue" in field
        }
        self.tags = description["tags"]
        self.client = client

    def delete_pipeline(self) -> str:
        response = self.client.delete_pipeline(pipelineId=self.pipeline_id)
        return response["ResponseMetadata"]["RequestId"]

    def describe_objects(self, evaluate_expressions=False) -> dict:
        """Describe all objects (components, instances, and attempts) for this pipeline."""
        return {
            sphere.lower(): {
                object_.object_id: {
                    "name": object_.name,
                    "ref_values": object_.ref_values,
                    "string_values": object_.string_values,
                }
                for object_ in self.objects(sphere, evaluate_expressions)
            }
            for sphere in ("COMPONENT", "INSTANCE", "ATTEMPT")
        }

    def describe_pipeline(self, evaluate_expressions=False) -> dict:
        logger.info("Collecting all objects belonging to pipeline '%s'", self.pipeline_id)
        return {
            self.pipeline_id: {
                "name": self.name,
                "objects": self.describe_objects(evaluate_expressions=evaluate_expressions),
                "string_values": self.string_values,
                "tags": self.tags,
            }
        }

    def find_attempts_with_errors(self, instances: Iterable[DataPipelineObject]):
        """Return head attempts for given instances when there was an error."""
        instance_for_head_attempt = {
            instance.ref_values["@headAttempt"]: instance
            for instance in instances
            if "@headAttempt" in instance.ref_values
        }
        for attempt in self.objects("ATTEMPT"):
            if attempt.object_id in instance_for_head_attempt and attempt.status != "FINISHED":
                attempt.parent_object = instance_for_head_attempt[attempt.object_id]
                yield attempt

    def instance_ids(self, sphere: str):
        paginator = self.client.get_paginator("query_objects")
        response_iterator = paginator.paginate(pipelineId=self.pipeline_id, sphere=sphere)
        for response in response_iterator:
            for id_ in response["ids"]:
                yield id_

    def latest_instances(self):
        """Group instances by their component and and return latest within each group."""
        component_lookup = {component.object_id: component for component in self.objects("COMPONENT")}
        grouped_instances = funcy.group_by(attrgetter("component_parent"), self.objects("INSTANCE"))
        logger.info(
            "Pipeline '%s' has %d components and %d instances, looking for latest instances",
            self.pipeline_id,
            len(grouped_instances),
            sum(map(len, grouped_instances.values())),
        )
        for component_parent in sorted(grouped_instances):
            latest_instance = sorted(grouped_instances[component_parent], key=attrgetter("scheduled_start_time"))[-1]
            latest_instance.parent_object = component_lookup[component_parent]
            yield latest_instance

    def objects(self, sphere, evaluate_expressions=False):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 objects
        object_ids = self.instance_ids(sphere)
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
    def latest_run_time(self):
        value = self.string_values.get("@latestRunTime")
        if not value:
            return value
        return self.human_delta(value)

    @property
    def next_run_time(self):
        # TODO(tom): This is the next schedule run time. So if you de-activate a pipeline and
        #     and then activate it again, this really should point to the latest re-start time.
        value = self.string_values.get("@nextRunTime")
        if not value:
            return value
        return self.human_delta(value)

    @property
    def state(self):
        return self.string_values.get("@pipelineState")

    @staticmethod
    def as_json(pipelines, evaluate_expressions=True) -> str:
        """
        Return information about pipelines in JSON-formatted string.

        If evaluate_expressions is True, then expressions are evaluated in the objects, e.g.
        attempts gain referenced values from their instances.
        If evaluate_expressions is False, you will have to resolve references yourself.
        """
        obj = {"pipelines": [pipeline.describe_pipeline(evaluate_expressions) for pipeline in pipelines]}
        return json.dumps(obj, indent="    ", sort_keys=True)

    @staticmethod
    def human_delta(value):
        when = arrow.get(value)
        now = arrow.now()
        delta_minutes = (when - now).total_seconds() // 60
        if 0 < delta_minutes < 5:
            return "about to start"
        if -60 <= delta_minutes <= 60:
            return when.humanize(now, granularity=["minute"])
        if -120 <= delta_minutes <= 120:
            return when.humanize(now, granularity=["hour", "minute"])
        return when.naive.isoformat()

    @staticmethod
    def describe_pipelines(client, pipeline_ids: Sequence[str]):
        chunk_size = 25  # Per AWS documentation, need to go in pages of 25 pipelines
        for ids_chunk in funcy.chunks(chunk_size, pipeline_ids):
            resp = client.describe_pipelines(pipelineIds=ids_chunk)
            for description in resp["pipelineDescriptionList"]:
                yield description

    @staticmethod
    def list_all_pipelines(client):
        paginator = client.get_paginator("list_pipelines")
        response_iterator = paginator.paginate()
        return response_iterator.search("pipelineIdList[].id")


def list_pipelines(selection: Sequence[str]) -> List[DataPipeline]:
    """
    Return list of pipelines related to this project (which must have the tag for our project set).

    The :selection should be a list of glob patterns to select specific pipelines by their ID.
    If the selection is an empty list, then all pipelines are used.
    """
    client = boto3.client("datapipeline")
    all_pipeline_ids = DataPipeline.list_all_pipelines(client)
    if selection:
        selected_pipeline_ids = [
            pipeline_id for pipeline_id in all_pipeline_ids for glob in selection if fnmatch.fnmatch(pipeline_id, glob)
        ]
    else:
        selected_pipeline_ids = list(all_pipeline_ids)

    pipelines = []
    for description in DataPipeline.describe_pipelines(client, selected_pipeline_ids):
        for tag in description["tags"]:
            if tag["key"] == "user:project" and tag["value"] == "data-warehouse":
                pipelines.append(DataPipeline(client, description))
                break
    return sorted(pipelines, key=attrgetter("name"))


def show_pipelines(selection: Sequence[str], as_json=False) -> None:
    """
    List the currently installed pipelines, possibly using a subset based on the selection pattern.

    Without a selection, prints an overview of the pipelines.
    With selection of a single pipeline, digs into details of that selected pipeline.

    If "as json" is chosen, then the output is JSON-formatted
    and includes all fields, not just the ones shown in the tables.
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
            join_with_single_quotes(pipeline.pipeline_id for pipeline in pipelines),
        )
    else:
        logger.info(
            "Available pipelines: %s",
            join_with_single_quotes(pipeline.pipeline_id for pipeline in pipelines),
        )
    if as_json:
        print(DataPipeline.as_json(pipelines))
        return

    print(
        etl.text.format_lines(
            [
                (
                    pipeline.pipeline_id,
                    pipeline.name,
                    pipeline.health_status or "---",
                    pipeline.state or "---",
                    pipeline.latest_run_time or "---",
                    pipeline.next_run_time or "---",
                )
                for pipeline in pipelines
            ],
            header_row=["Pipeline ID", "Name", "Health", "State", "Latest Run Time", "Next Run Time"],
            max_column_width=80,
        )
    )
    if len(pipelines) == 1:
        _show_pipeline_details(pipelines.pop())


def _show_pipeline_details(pipeline) -> None:
    """Print details for a specific pipeline (object)."""
    instances = sorted(pipeline.latest_instances())
    attempts_with_errors = sorted(pipeline.find_attempts_with_errors(instances))

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
                            "@nextRunTime",
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
                    instance.actual_elapsed or "---",
                )
                for instance in sorted(instances)
            ],
            header_row=["Instance Name", "Type", "Status", "Actual Start Time", "Actual End Time", "Elapsed"],
            max_column_width=80,
        )
    )
    for attempt in attempts_with_errors:
        print()
        print(f"*** {attempt.name}: {attempt.status} ***")
        if attempt.error_stack_trace:
            print(textwrap.indent(attempt.error_stack_trace, "|  ", lambda line: True))
        print(f"Command: {attempt.command or 'N/A'}")
        print(f"Log location: {attempt.log_location}")


def delete_finished_pipelines(selection: Sequence[str], dry_run=False) -> None:
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
            response = pipeline.delete_pipeline()
            logger.info(
                "Succeeded to delete '%s' (request_id=%s)",
                pipeline.pipeline_id,
                response,
            )
