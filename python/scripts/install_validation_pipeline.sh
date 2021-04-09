#!/usr/bin/env bash

START_NOW=$(date -u +"%Y-%m-%dT%H:%M:%S")
USER="${USER-nobody}"
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

if [[ $# -gt 3 || "$1" = "-h" ]]; then
    cat <<USAGE

Usage: $(basename "$0") [<environment> [<startdatetime> [<occurrences>]]]

The environment defaults to "$DEFAULT_PREFIX".
Start time should take the ISO8601 format, defaults to "$START_NOW" (now).
The number of occurrences defaults to 1.

USAGE
    exit 0
fi

set -o errexit -o nounset

# Verify that there is a local configuration directory
DEFAULT_CONFIG="${DATA_WAREHOUSE_CONFIG:-./config}"
if [[ ! -d "$DEFAULT_CONFIG" ]]; then
    echo "Failed to find \'$DEFAULT_CONFIG\' directory."
    echo "Make sure you are in the directory with your data warehouse setup or have DATA_WAREHOUSE_CONFIG set."
    exit 1
fi

PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
PROJ_ENVIRONMENT="${1:-$DEFAULT_PREFIX}"
START_DATE_TIME="${2:-$START_NOW}"
OCCURRENCES="${3:-1}"

# Verify that this bucket/environment pair is set up on S3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 1
fi
# NOTE we don't test for the validation folder since that gets created automatically.

set -o xtrace

# N.B. This assumes you are in the directory with your warehouse definition (schemas, config, ...)
GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD 2>/dev/null || true)

if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
    PIPELINE_NAME="ETL Validation Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
elif [[ -n "$GIT_BRANCH" ]]; then
    PIPELINE_NAME="Validation Pipeline ($DEFAULT_PREFIX:$GIT_BRANCH $PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
else
    PIPELINE_NAME="Validation Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
fi

# Note: "key" and "value" are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=dw-etl key=user:data-pipeline-type,value=validation"

PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER-nobody}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER-nobody}_$$.json"
# shellcheck disable=SC2064
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" validation_pipeline > "$PIPELINE_DEFINITION_FILE"

# shellcheck disable=SC2086
aws datapipeline create-pipeline \
    --unique-id validation-pipeline \
    --name "$PIPELINE_NAME" \
    --tags $AWS_TAGS \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=$(jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId')

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
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"

set +x
echo
echo "You can monitor the status of this validation pipeline using:"
echo "  watch --interval=5 arthur.py show_pipelines -q '$PIPELINE_ID'"
