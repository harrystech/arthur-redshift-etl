#!/usr/bin/env bash

if [[ $# -lt 1 || $# -gt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <start_time> <source target selection>"
    echo "Provide start_time in UTC and selection as space-delimited arthur pattern globs"
    exit 0
fi

function join_by { local IFS="$1"; shift; echo "$*"; }

CLUSTER_BUCKET="${1?'Missing bucket name'}"
CLUSTER_ENVIRONMENT="${2-production}"
START_TIME="${3?'Missing update source selection'}"
SELECTION="${4?'Missing update source selection'}"

DATE=`date +%Y-%m-%d`
START_DATE_TIME="$(join_by T $DATE $START_TIME)"
C_S_SELECTION="$(join_by , $SELECTION)"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 1
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -x

aws datapipeline create-pipeline \
    --name "ETL Midday Pipeline ($CLUSTER_ENVIRONMENT @ $START_TIME)" \
    --unique-id redshift_etl_pipeline \
    --tags key=DataWarehouseEnvironment,value=Production \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/midday_data_pipeline.json \
    --parameter-values myS3Bucket="$CLUSTER_BUCKET" myEtlEnvironment="$CLUSTER_ENVIRONMENT" myStartDateTime="$START_DATE_TIME" mySelection="$SELECTION" myCommaSeparatedSelection="$C_S_SELECTION" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
