#!/usr/bin/env bash

START_NOW=`date -u +"%Y-%m-%dT%H:%M:%S"`

if [[ $# -lt 1 || $# -gt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> [<environment> [<startdatetime> [<occurrences>]]]"
    echo "      The environment defaults to your user name, $USER."
    echo "      Start time should take the ISO8601 format, defaults to \"$START_NOW\" (now)."
    echo "      The number of occurrences defaults to 1."
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="${2:-$USER}"
START_DATE_TIME="${3:-$START_NOW}"
OCCURRENCES="${4:-1}"

BINDIR=`dirname $0`
TOPDIR=`\cd $BINDIR/.. && \pwd`
CONFIG_SOURCE="$TOPDIR/aws_config"

if [[ ! -d "$CONFIG_SOURCE" ]]; then
    echo "Cannot find configuration files (aws_config)"
    exit 1
else
    echo "Using local configuration files in $CONFIG_SOURCE"
fi

# Verify that this bucket/environment pair is set up on s3 with credentials
VALIDATION_CREDENTIALS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/validation/config/credentials_validation.sh"
if ! aws s3 ls "$VALIDATION_CREDENTIALS" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist,"
    echo "whether you have access to it, and whether credentials_validation.sh was copied into validation/config!"
    exit 1
fi

# N.B. This assumes you are in the directory with your warehouse definition (schemas, config, ...)
if ! GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD); then
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
    --pipeline-definition file://${CONFIG_SOURCE}/validation_pipeline.json \
    --parameter-values \
        myS3Bucket="$CLUSTER_BUCKET" \
        myEtlEnvironment="$CLUSTER_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
