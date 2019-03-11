#! /usr/bin/env python3

import sys
from operator import itemgetter

import boto3
import jmespath


def get_etl_pipeline_ids(client):
    """
    Return a dict mapping pipeline ids to their names, filtering on ETL pipelines.
    """
    paginator = client.get_paginator('list_pipelines')
    response_iterator = paginator.paginate()
    filtered_iterator = response_iterator.search(
        "pipelineIdList[?contains(@.name, 'ETL') == `true`].id"
    )
    return list(filtered_iterator)


def get_pipeline_status(client, pipeline_id):
    """
    Return a dict describing the current status of the pipeline.
    """
    response = client.describe_pipelines(pipelineIds=[pipeline_id])
    names = ["pipelineId", "name", "@pipelineState", "@healthStatus", "@latestRunTime"]
    values = jmespath.search("""
        pipelineDescriptionList[0].[pipelineId,
                                    name,
                                    fields[?key == '@pipelineState'].stringValue|[0],
                                    fields[?key == '@healthStatus'].stringValue|[0],
                                    fields[?key == '@latestRunTime'].stringValue|[0]]
        """, response)
    return dict(zip(names, values))


def get_scheduled_component_ids(client, pipeline_id):
    """
    Return ids of component objects of the pipeline which are in "SCHEDULED" state.
    """
    paginator = client.get_paginator('query_objects')
    object_ids = []
    response_iterator = paginator.paginate(
        pipelineId=pipeline_id,
        query={
            "selectors": [
                {"fieldName": "@status", "operator": {"type": "EQ", "values": ["SCHEDULED"]}}
            ]
        },
        sphere="COMPONENT"
    )
    for response in response_iterator:
        object_ids.extend(response["ids"])
    return object_ids


def get_shell_activity_status(client, pipeline_id, object_ids):
    """
    Return status of objects which are ShellCommandActivity objects.
    """
    paginator = client.get_paginator('describe_objects')
    response_iterator = paginator.paginate(pipelineId=pipeline_id, objectIds=object_ids)
    filtered_iterator = response_iterator.search(
        "pipelineObjects[?fields[?key == 'type'].stringValue|[0] == 'ShellCommandActivity']"
    )
    names = ["id", "name", "@healthStatus", "@healthStatusFromInstanceId"]
    statuses = []
    for response in filtered_iterator:
        values = jmespath.search("""
            [
                id,
                name,
                fields[?key == '@healthStatus'].stringValue|[0],
                fields[?key == '@healthStatusFromInstanceId'].stringValue|[0]
            ]
            """, response)
        statuses.append(dict(zip(names, values)))
    return statuses


def set_status_to_rerun(client, pipeline_id, object_ids):
    response = client.set_status(pipelineId=pipeline_id,
                                 objectIds=object_ids,
                                 status="RERUN")
    print(response['ResponseMetadata']['HTTPStatusCode'])


def change_status_to_rerun(pipeline_id):
    """
    Set status of most recent instances of pipeline objects to re-run.
    """
    client = boto3.client("datapipeline")
    pipeline_status = get_pipeline_status(client, pipeline_id)

    print("Found ETL pipeline:\n")
    for name in ["name", "pipelineId", "@latestRunTime", "@healthStatus"]:
        print("  {} = {}".format(name, pipeline_status[name]))
    print()

    object_ids = get_scheduled_component_ids(client, pipeline_id)
    if not object_ids:
        print("Found no scheduled objects in pipeline: {}".format(pipeline_id))
        sys.exit(1)

    object_statuses = get_shell_activity_status(client, pipeline_id, object_ids)
    print("  Found objects (of type 'ShellCommandActivity'):\n")
    for status in object_statuses:
        for name in ["name", "id", "@healthStatus"]:
            print("    {} = {}".format(name, status[name]))
        print()

    ans = input("Do you want to continue and set status of theses objects to 'RERUN'? [y/N] ")
    if ans.lower() == 'y':
        print("Ok, proceeding")
        instance_ids = [status["@healthStatusFromInstanceId"] for status in object_statuses]
        set_status_to_rerun(client, pipeline_id, instance_ids)


def list_pipelines():
    """
    List all ETL pipelines that are currently scheduled.
    """
    client = boto3.client("datapipeline")
    pipeline_ids = get_etl_pipeline_ids(client)
    statuses = []
    for pipeline_id in pipeline_ids:
        status = get_pipeline_status(client, pipeline_id)
        if status["@pipelineState"] == "SCHEDULED":
            statuses.append(status)
    if not statuses:
        print("Found no scheduled ETL pipelines")
        print("Note that a pipeline must have a name containing 'ETL' and be in SCHEDULED state to be picked up here.")
        return
    for status in sorted(statuses, key=itemgetter("name")):
        print("{pipelineId:24s} - {name} - ({@healthStatus}, {@latestRunTime})".format(**status))


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "-h":
        print("Usage: {} [--list | '<pipeline-id>']".format(sys.argv[0]))
        sys.exit(0)
    if sys.argv[1] in ("-l", "--list"):
        list_pipelines()
    else:
        change_status_to_rerun(sys.argv[1])
