#! /usr/bin/env python3

import sys
from operator import itemgetter

import boto3
import funcy
import jmespath


def get_etl_pipeline_ids(client):
    """Return a dict mapping pipeline ids to their names, filtering on ETL pipelines."""
    paginator = client.get_paginator("list_pipelines")
    response_iterator = paginator.paginate()
    filtered_iterator = response_iterator.search("pipelineIdList[?contains(@.name, 'ETL') == `true`].id")
    return list(filtered_iterator)


def get_pipeline_status(client, pipeline_ids):
    """Return dicts describing the current status of the pipelines."""
    extract_fields = jmespath.compile(
        """
        pipelineDescriptionList[].{
            pipelineId: pipelineId,
            name: name,
            pipelineState: fields[?key == '@pipelineState'].stringValue|[0],
            healthStatus: fields[?key == '@healthStatus'].stringValue|[0],
            latestRunTime: fields[?key == '@latestRunTime'].stringValue|[0]
        }
        """
    )
    chunk_size = 25  # Per AWS documentation, need to go in pages of 25 pipelines
    for ids_chunk in funcy.chunks(chunk_size, pipeline_ids):
        response = client.describe_pipelines(pipelineIds=ids_chunk)
        values = extract_fields.search(response)
        for value in values:
            yield value


def get_scheduled_component_ids(client, pipeline_id):
    """Return ids of component objects of the pipeline which are in "SCHEDULED" state."""
    paginator = client.get_paginator("query_objects")
    response_iterator = paginator.paginate(
        pipelineId=pipeline_id,
        query={
            "selectors": [{"fieldName": "@status", "operator": {"type": "EQ", "values": ["SCHEDULED"]}}]
        },
        sphere="COMPONENT",
    )
    return list(funcy.cat(response["ids"] for response in response_iterator))


def get_shell_activity_status(client, pipeline_id, object_ids):
    """Return generator for status of objects which are ShellCommandActivity objects."""
    paginator = client.get_paginator("describe_objects")
    response_iterator = paginator.paginate(pipelineId=pipeline_id, objectIds=object_ids)
    filtered_iterator = response_iterator.search(
        "pipelineObjects[?fields[?key == 'type'].stringValue|[0] == 'ShellCommandActivity']"
    )
    extract_fields = jmespath.compile(
        """
        {
            id: id,
            name: name,
            healthStatus: fields[?key == '@healthStatus'].stringValue|[0],
            healthStatusFromInstanceId:
                fields[?key == '@healthStatusFromInstanceId'].stringValue|[0]
        }
        """
    )
    for response in filtered_iterator:
        yield extract_fields.search(response)


def set_status_to_rerun(client, pipeline_id, object_ids):
    response = client.set_status(pipelineId=pipeline_id, objectIds=object_ids, status="RERUN")
    print(response["ResponseMetadata"]["HTTPStatusCode"])


def change_status_to_rerun(pipeline_id):
    """Set status of most recent instances of pipeline objects to re-run."""
    client = boto3.client("datapipeline")
    [pipeline_status] = get_pipeline_status(client, [pipeline_id])

    print("Found ETL pipeline:\n")
    for name in ["name", "pipelineId", "latestRunTime", "healthStatus"]:
        print("  {} = {}".format(name, pipeline_status[name]))
    print()

    object_ids = get_scheduled_component_ids(client, pipeline_id)
    if not object_ids:
        print("Found no scheduled objects in pipeline: {}".format(pipeline_id))
        sys.exit(1)

    object_statuses = list(get_shell_activity_status(client, pipeline_id, object_ids))
    print("  Found objects (of type 'ShellCommandActivity'):\n")
    for status in object_statuses:
        for name in ["name", "id", "healthStatus"]:
            print("    {} = {}".format(name, status[name]))
        print()

    ans = input("Do you want to continue and set status of theses objects to 'RERUN'? [y/N] ")
    if ans.lower() == "y":
        print("Ok, proceeding")
        instance_ids = [status["healthStatusFromInstanceId"] for status in object_statuses]
        set_status_to_rerun(client, pipeline_id, instance_ids)


def list_pipelines():
    """List all ETL pipelines that are currently scheduled."""
    client = boto3.client("datapipeline")
    pipeline_ids = get_etl_pipeline_ids(client)
    statuses = [
        status
        for status in get_pipeline_status(client, pipeline_ids)
        if status["pipelineState"] == "SCHEDULED"
    ]

    if not statuses:
        print("Found no scheduled ETL pipelines")
        print(
            "(A pipeline must have a name containing 'ETL' and be in SCHEDULED state to be picked up here.)"
        )
        return
    for status in sorted(statuses, key=itemgetter("name")):
        print("{pipelineId:24s} - {name} - ({healthStatus}, {latestRunTime})".format(**status))


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "-h":
        print("Usage: {} [--list | '<pipeline-id>']".format(sys.argv[0]))
        sys.exit(0)
    if sys.argv[1] in ("-l", "--list"):
        list_pipelines()
    else:
        change_status_to_rerun(sys.argv[1])
