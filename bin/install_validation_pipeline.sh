#!/usr/bin/env bash

if [[ $# -lt 3 || $# -gt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <startdatetime> [<occurrences>]"
    echo "      Start time should take the ISO8601 format like: `date +"%Y-%m-%dT%H:%M:%S"`"
    echo "      The number of occurrences defaults to 1."
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"
START_DATE_TIME="$3"
OCCURRENCES="${4:-1}"

# Verify that this bucket/environment pair is set up on s3 with credentials
VALIDATION_CREDENTIALS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/validation/config/credentials_validation.sh"
if ! aws s3 ls "$VALIDATION_CREDENTIALS" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist with credentials_validation.sh!"
    exit 1
fi

# N.B. This assumes you are in the directory with your warehouse definition (schemas, config, ...)
if ! GIT_BRANCH=$(git symbolic-ref --short -q HEAD); then
  GIT_BRANCH="(detached head)"
fi

if [[ "$CLUSTER_ENVIRONMENT" =~ "production" ]]; then
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Production"
    PIPELINE_NAME="Validation Pipeline ($CLUSTER_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
else
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Development"
    PIPELINE_NAME="Validation Pipeline ($USER:$GIT_BRANCH $CLUSTER_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -u -x

aws datapipeline create-pipeline \
    --unique-id validation_pipeline \
    --name "$PIPELINE_NAME" \
    --tags "$PIPELINE_TAGS" \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/validation_pipeline.json \
    --parameter-values \
        myS3Bucket="$CLUSTER_BUCKET" \
        myEtlEnvironment="$CLUSTER_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
