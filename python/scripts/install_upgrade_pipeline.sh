#!/usr/bin/env bash

START_NOW=`date -u +"%Y-%m-%dT%H:%M:%S"`
USER="${USER-nobody}"
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

if [[ $# -lt 1 || "$1" = "-h" ]]; then

    cat <<EOF

Single-shot upgrade pipeline. You get to pick the arguments to 'upgrade' command.

Usage: `basename $0` upgrade_arg [upgrade_arg ...]

The remaining arguments will be passed to 'upgrade'.
The environment defaults to "$DEFAULT_PREFIX".

Example: `basename $0` --continue-from :transformations

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
PROJ_ENVIRONMENT="$DEFAULT_PREFIX"

UPGRADE_ARGUMENTS="$@"

START_DATE_TIME="$START_NOW"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 1
fi

set -x

# Note: "key" and "value" are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=dw-etl"

PIPELINE_NAME="Upgrade Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME)"
PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER-nobody}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER-nobody}_$$.json"
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" upgrade_pipeline > "$PIPELINE_DEFINITION_FILE"

aws datapipeline create-pipeline \
    --unique-id dw-etl-upgrade-pipeline \
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
        myUpgradeArguments="$UPGRADE_ARGUMENTS" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"

set +x
echo
echo "You can monitor the status of this upgrade pipeline using:"
echo "  watch --interval=5 arthur.py show_pipelines -q '$PIPELINE_ID'"
