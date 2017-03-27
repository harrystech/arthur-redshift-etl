#!/usr/bin/env bash

if [[ $# -ne 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <startdatetime> <occurrences>"
    echo "      Start time should take the ISO8601 format like: `date +"%Y-%m-%dT%H:%M:%S"`"
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"
START_DATE_TIME="$3"
OCCURRENCES="$4"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
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
    --name "ETL Rebuild Pipeline ($CLUSTER_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)" \
    --tags "$PIPELINE_TAGS" \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/rebuild_pipeline.json \
    --parameter-values \
        myS3Bucket="$CLUSTER_BUCKET" \
        myEtlEnvironment="$CLUSTER_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
