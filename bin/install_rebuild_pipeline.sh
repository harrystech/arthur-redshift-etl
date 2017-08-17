#!/usr/bin/env bash

if [[ $# -ne 5 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> <startdatetime> <occurrences> <wlm-slots>"
    echo "      Start time should take the ISO8601 format like: `date -u +"%Y-%m-%dT%H:%M:%S"`"
    exit 0
fi

set -e -u

PROJ_BUCKET="$1"
PROJ_ENVIRONMENT="$2"

START_DATE_TIME="$3"
OCCURRENCES="$4"
WLM_SLOTS="$5"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 1
fi

set -x

if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
  ENV_NAME="production"
else
  ENV_NAME="development"
fi
# Note: key/value are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:env,value=$ENV_NAME"

PIPELINE_NAME="ETL Rebuild Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"

PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER}_$$.json"
arthur.py render_template --prefix "$PROJ_ENVIRONMENT" rebuild_pipeline > "$PIPELINE_DEFINITION_FILE"

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

aws datapipeline create-pipeline \
    --unique-id rebuild-etl-pipeline \
    --name "$PIPELINE_NAME" \
    --tags $AWS_TAGS \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

if [[ -z "$PIPELINE_ID" ]]; then
    set +x
    echo "Failed to find pipeline id in output -- pipeline probably wasn't created. Check your VPN etc."
    exit 1
fi

aws datapipeline put-pipeline-definition \
    --pipeline-definition "file://$PIPELINE_DEFINITION_FILE" \
    --parameter-values \
        myS3Bucket="$PROJ_BUCKET" \
        myEtlEnvironment="$PROJ_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
        myMaxPartitions="16" \
        myMaxConcurrency="4" \
        myWlmQuerySlots="$WLM_SLOTS" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
