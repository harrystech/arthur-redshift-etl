#!/usr/bin/env bash

if [[ $# -lt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <environment> <startdatetime> <occurrences> <source table selection> [<source table selection> ...]"
    echo "      Start time should take the ISO8601 format like: `date -u +"%Y-%m-%dT%H:%M:%S"`"
    echo "      Specify source tables using space-delimited arthur pattern globs."
    exit 0
fi

set -e -u

function join_by { local IFS="$1"; shift; echo "$*"; }

PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
PROJ_ENVIRONMENT="$1"

START_DATE_TIME="$2"
OCCURRENCES="$3"

shift 3
SELECTION="$@"
C_S_SELECTION="$(join_by ',' $SELECTION)"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/current/bin/bootstrap.sh"
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

PIPELINE_NAME="ETL Refresh Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"

PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER}_$$.json"
arthur.py render_template --prefix "$PROJ_ENVIRONMENT" refresh_pipeline > "$PIPELINE_DEFINITION_FILE"

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

aws datapipeline create-pipeline \
    --unique-id refresh-etl-pipeline \
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
        mySelection="$SELECTION" \
        myCommaSeparatedSelection="$C_S_SELECTION" \
        myMaxPartitions="16" \
        myMaxConcurrency="4" \
        myWlmQuerySlots="3" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
