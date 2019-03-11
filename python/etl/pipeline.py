"""
Implement commands that interact with AWS Data Pipeline
"""

import fnmatch
import logging
from operator import attrgetter
from typing import List

import boto3

import etl.text
from etl.text import join_with_quotes

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DataPipeline:

    def __init__(self, description):
        self.pipeline_id = description['pipelineId']
        self.name = description['name']
        self.fields = {
            field['key']: field['stringValue']
            for field in description['fields']
            if field['key'] != '*tags'  # tags are an ugly list of dicts
        }

    def __str__(self):
        return "DataPipeline('{}','{}')".format(self.pipeline_id, self.name)

    @property
    def health_status(self):
        return self.fields.get('@healthStatus', '---')


def list_pipelines(selection: List[str]) -> List[DataPipeline]:
    """
    List pipelines related to this project (which must have the tag for our project set)

    The :selection should be a list of glob patterns to select specific pipelines by their ID.
    If the selection is an empty list, then all pipelines are used.
    """
    client = boto3.client('datapipeline')

    all_pipeline_ids = []  # type: List[str]
    resp = client.list_pipelines()
    while True:
        all_pipeline_ids.extend(id_['id'] for id_ in resp['pipelineIdList'])
        if resp['hasMoreResults']:
            resp = client.list_pipelines(marker=resp['marker'])
        else:
            break

    if selection:
        selected_pipeline_ids = [pipeline_id
                                 for pipeline_id in all_pipeline_ids
                                 for glob in selection
                                 if fnmatch.fnmatch(pipeline_id, glob)]
    else:
        selected_pipeline_ids = all_pipeline_ids

    dw_pipelines = []
    chunk_size = 25  # Per AWS documentation, need to go in pages of 25 pipelines
    for block in range(0, len(selected_pipeline_ids), chunk_size):
        resp = client.describe_pipelines(pipelineIds=selected_pipeline_ids[block:block + chunk_size])
        for description in resp['pipelineDescriptionList']:
            for tag in description['tags']:
                if tag['key'] == 'user:project' and tag['value'] == 'data-warehouse':
                    dw_pipelines.append(DataPipeline(description))
    return sorted(dw_pipelines, key=attrgetter("name"))


def show_pipelines(selection: List[str]) -> None:
    """
    List the currently installed pipelines, possibly using a subset based on the selection pattern.

    Without a selection, prints an overview of the pipelines.
    With a selection, digs into details of each selected pipeline.
    """
    pipelines = list_pipelines(selection)

    if not pipelines:
        logger.warning("Found no pipelines")
        print("*** No pipelines found ***")
    elif selection and len(pipelines) > 1:
        logger.warning("Selection matches more than one pipeline")
    if pipelines:
        if selection:
            logger.info("Currently active and selected pipelines: %s",
                        join_with_quotes(pipeline.pipeline_id for pipeline in pipelines))
        else:
            logger.info("Currently active pipelines: %s",
                        join_with_quotes(pipeline.pipeline_id for pipeline in pipelines))
        print(etl.text.format_lines([(pipeline.pipeline_id, pipeline.name, pipeline.health_status)
                                     for pipeline in pipelines],
                                    header_row=["Pipeline ID", "Name", "Health"], max_column_width=80))
    if selection and len(pipelines) == 1:
        pipeline = pipelines[0]
        print(etl.text.format_lines([[key, pipeline.fields[key]] for key in sorted(pipeline.fields)],
                                    header_row=["Key", "Value"]))
