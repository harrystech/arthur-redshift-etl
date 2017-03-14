#!/usr/bin/env bash

if [[ $# -lt 3 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <startdatetime> <occurrences>"
    echo "      Start time should take format like: `date +"%Y-%m-%dT%H:%M:%S"`"
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"
STARTDATETIME="$3"
OCCURRENCES="${4-1}"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 1
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -x

aws datapipeline create-pipeline \
    --name "Validation Pipeline ($CLUSTER_ENVIRONMENT)" \
    --unique-id validation_pipeline \
    --tags key=DataWarehouseEnvironment,value=Production \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/validation_pipeline.json \
    --parameter-values myS3Bucket="$CLUSTER_BUCKET" myEtlEnvironment="$CLUSTER_ENVIRONMENT" myStartDateTime="$STARTDATETIME" myOccurrences="$OCCURRENCES" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
