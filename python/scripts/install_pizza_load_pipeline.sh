#!/usr/bin/env bash

if [[ $# -lt 1 || $# -gt 2 || "$1" = "-h" ]]; then

    cat <<EOF

Pizza delivery! Right on time or it's free! Runs once, starting now.
Expects S3 folder under prefix to already have all necessary manifests for source data.

Usage: `basename $0` <environment> [<continue-from>]

The loader will pick up from the "continue-from" relation if specified.

EOF
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

PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
PROJ_ENVIRONMENT="$1"
CONTINUE_FROM_RELATION="${2:-*}"

START_DATE_TIME=`date -u +"%Y-%m-%dT%H:%M:%S"`

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 1
fi

set -x

# Note: "key" and "value" are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=dw-etl"

PIPELINE_NAME="ETL Pizza Loader Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME)"
PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER-nobody}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER-nobody}_$$.json"
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" pizza_load_pipeline > "$PIPELINE_DEFINITION_FILE"

aws datapipeline create-pipeline \
    --unique-id dw-etl-pizza-pipeline \
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
        mySelection="$CONTINUE_FROM_RELATION" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"

set +x
echo
echo "You can monitor the status of this pizza pipeline using:"
echo "  watch --interval=5 arthur.py show_pipelines -q '$PIPELINE_ID'"
