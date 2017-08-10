#!/usr/bin/env bash

if [[ $# -ne 3 || "$1" = "-h" ]]; then
    echo "Pizza delivery! Right on time or it's free! Runs once, starting now."
    echo "Expects prefix to already have all necessary manifests for source data."
    echo "Usage: `basename $0` <bucket_name> <environment> <wlm-slots>"
    exit 0
fi

set -e -u

PROJ_BUCKET="$1"
PROJ_ENVIRONMENT="$2"
WLM_SLOTS="$3"

START_DATE_TIME=`date -u +"%Y-%m-%dT%H:%M:%S"`

BINDIR=`dirname $0`
TOPDIR=`\cd $BINDIR/.. && \pwd`
CONFIG_SOURCE="$TOPDIR/aws_config"

if [[ ! -d "$CONFIG_SOURCE" ]]; then
    echo "Cannot find configuration files (aws_config)"
    exit 1
else
    echo "Using local configuration files in $CONFIG_SOURCE"
fi

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 1
fi

set -x

if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Production"
else
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Development"
fi
PIPELINE_NAME="ETL Pizza Loader Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME)"

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

aws datapipeline create-pipeline \
    --unique-id pizza-etl-pipeline \
    --name "$PIPELINE_NAME" \
    --tags "$PIPELINE_TAGS" \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

if [[ -z "$PIPELINE_ID" ]]; then
    set +x
    echo "Failed to find pipeline id in output -- pipeline probably wasn't created. Check your VPN etc."
    exit 1
fi

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://${CONFIG_SOURCE}/pizza_load_pipeline.json \
    --parameter-values \
        myS3Bucket="$PROJ_BUCKET" \
        myEtlEnvironment="$PROJ_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myMaxConcurrency="4" \
        myWlmQuerySlots="$WLM_SLOTS" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
