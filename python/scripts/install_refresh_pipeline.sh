#!/usr/bin/env bash

if [[ $# -lt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <environment> <startdatetime> <occurrences> <source table selection> [<source table selection> ...]"
    echo "      Start time should be 'now' or take the ISO8601 format like: `date -u +"%Y-%m-%dT%H:%M:%S"`"
    echo "      Specify source tables using space-delimited arthur pattern globs."
    exit 0
fi

set -e -u

# Verify that there is a local configuration directory
DEFAULT_CONFIG="${DATA_WAREHOUSE_CONFIG:-./config}"
if [[ ! -d "$DEFAULT_CONFIG" ]]; then
    echo "Failed to find \'$DEFAULT_CONFIG\' directory."
    echo "Make sure you are in the directory with your data warehouse setup or have DATA_WAREHOUSE_CONFIG set."
    exit 1
fi

function join_by { local IFS="$1"; shift; echo "$*"; }

PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
PROJ_ENVIRONMENT="$1"

if [[ "$2" == "now" ]]; then
    START_DATE_TIME="`date -u +'%Y-%m-%dT%H:%M:%S'`"
else
    START_DATE_TIME="$2"
fi
OCCURRENCES="$3"

shift 3
SELECTION="$@"
C_S_SELECTION="$(join_by ',' $SELECTION)"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/current/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT/current\" exist!"
    exit 1
fi

set -x

# Note: "key" and "value" are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=ETL"

PIPELINE_NAME="ETL Refresh Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" refresh_pipeline > "$PIPELINE_DEFINITION_FILE"

aws datapipeline create-pipeline \
    --unique-id dw-etl-refresh-pipeline \
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
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
        mySelection="$SELECTION" \
        myCommaSeparatedSelection="$C_S_SELECTION" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
