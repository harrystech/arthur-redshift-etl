#!/usr/bin/env bash

if [[ $# -lt 5 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <startdatetime> <occurrences> <source table selection> [<source table selection> ...]"
    echo "      Start time should take the ISO8601 format like: `date -u +"%Y-%m-%dT%H:%M:%S"`"
    echo "      Specify source tables using space-delimited arthur pattern globs."
    exit 0
fi

function join_by { local IFS="$1"; shift; echo "$*"; }

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"
START_DATE_TIME="$3"
OCCURRENCES="$4"

shift 4
SELECTION="$@"
C_S_SELECTION="$(join_by ',' $SELECTION)"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/current/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 1
fi

if [[ "$CLUSTER_ENVIRONMENT" =~ "production" ]]; then
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Production"
else
    PIPELINE_TAGS="key=DataWarehouseEnvironment,value=Development"
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -u -x

aws datapipeline create-pipeline \
    --unique-id redshift_etl_pipeline \
    --name "ETL Refresh Pipeline ($CLUSTER_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)" \
    --tags "$PIPELINE_TAGS" \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/refresh_pipeline.json \
    --parameter-values \
        myS3Bucket="$CLUSTER_BUCKET" \
        myEtlEnvironment="$CLUSTER_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
        mySelection="$SELECTION" \
        myCommaSeparatedSelection="$C_S_SELECTION" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
