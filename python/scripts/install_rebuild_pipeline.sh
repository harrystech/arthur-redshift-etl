#!/usr/bin/env bash

START_NOW=$(date -u +"%Y-%m-%dT%H:%M:%S")
DEFAULT_TIMEOUT=6

if [[ $# -lt 3 || $# -gt 4 || "$1" = "-h" ]]; then
  cat <<USAGE

Rebuild ETL to extract, load (including transforms), and unload data.

Usage: $(basename "$0") <environment> <startdatetime> <occurrences> [timeout]

Start time should be 'now' or take the ISO8601 format like: $START_NOW
Optional timeout should be the number of hours pipeline is allowed to run. Defaults to $DEFAULT_TIMEOUT.

USAGE
  exit 0
fi

set -o errexit -o nounset

# Verify that there is a local configuration directory
DEFAULT_CONFIG="${DATA_WAREHOUSE_CONFIG:-./config}"
if [[ ! -d "$DEFAULT_CONFIG" ]]; then
  echo 1>&2 "Failed to find \'$DEFAULT_CONFIG\' directory."
  echo 1>&2 "Make sure you are in the directory with your data warehouse setup or have DATA_WAREHOUSE_CONFIG set."
  exit 1
fi

PROJ_BUCKET=$(arthur.py show_value object_store.s3.bucket_name)
PROJ_ENVIRONMENT="$1"

if [[ "$2" == "now" ]]; then
  START_DATE_TIME="$START_NOW"
else
  START_DATE_TIME="$2"
fi
OCCURRENCES="$3"
TIMEOUT="${4:-$DEFAULT_TIMEOUT}"

# Verify that this bucket/environment pair is set up on S3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
  echo 1>&2 "Failed to access \"$BOOTSTRAP\"!"
  echo 1>&2 "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist,"
  echo 1>&2 "whether you have the correct access permissions, and"
  echo 1>&2 "whether you have uploaded the Arthur environment."
  exit 1
fi

set -o xtrace

# Note: "key" and "value" are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=dw-etl key=user:data-pipeline-type,value=rebuild"

PIPELINE_NAME="ETL Rebuild Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER-nobody}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER-nobody}_$$.json"

# shellcheck disable=SC2064
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" rebuild_pipeline > "$PIPELINE_DEFINITION_FILE"

# shellcheck disable=SC2086
aws datapipeline create-pipeline \
    --unique-id dw-etl-rebuild-pipeline \
    --name "$PIPELINE_NAME" \
    --tags $AWS_TAGS \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=$(jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId')

if [[ -z "$PIPELINE_ID" ]]; then
  set +o xtrace
  echo 1>&2 "Failed to find pipeline id in output -- pipeline probably wasn't created. Check your VPN etc."
  exit 1
fi

aws datapipeline put-pipeline-definition \
    --pipeline-definition "file://$PIPELINE_DEFINITION_FILE" \
    --parameter-values \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
        myTimeout="$TIMEOUT" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"

set +o xtrace
echo
echo "You can monitor the status of this rebuild pipeline using:"
echo "  watch --interval=5 arthur.py show_pipelines -q '$PIPELINE_ID'"
