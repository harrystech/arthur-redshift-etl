#!/usr/bin/env bash

CLUSTER_BUCKET="s3://${1?'Missing bucket name'}"
CLUSTER_ENVIRONMENT="${2?'Missing name of environment'}"

PIPELINE_ID_FILE="/var/tmp/pipeline_id_${USER}_$$.json"

set -e -x

aws datapipeline create-pipeline --name "ETL Pipeline ($CLUSTER_ENVIRONMENT)" --unique-id etl_pipeline | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$CLUSTER_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition --pipeline-definition file://./data_pipeline.json --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
