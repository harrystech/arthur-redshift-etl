#!/usr/bin/env bash

START_NOW=`date -u +"%Y-%m-%dT%H:%M:%S"`
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

if [[ $# -lt 1 || $# -gt 4 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> [<environment> [<startdatetime> [<occurrences>]]]"
    echo "      The environment defaults to \"$DEFAULT_PREFIX\"."
    echo "      Start time should take the ISO8601 format, defaults to \"$START_NOW\" (now)."
    echo "      The number of occurrences defaults to 1."
    exit 0
fi

set -e -u

PROJ_BUCKET="$1"
PROJ_ENVIRONMENT="${2:-$DEFAULT_PREFIX}"

START_DATE_TIME="${3:-$START_NOW}"
OCCURRENCES="${4:-1}"

# Verify that this bucket/environment pair is set up on s3 with credentials
VALIDATION_CREDENTIALS="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/validation/config/credentials_validation.sh"
if ! aws s3 ls "$VALIDATION_CREDENTIALS" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist,"
    echo "whether you have access to it, and whether credentials_validation.sh was copied into validation/config!"
    exit 1
fi

set -x

# N.B. This assumes you are in the directory with your warehouse definition (schemas, config, ...)
if ! GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD); then
  GIT_BRANCH="(detached head)"
fi

if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
  ENV_NAME="production"
  PIPELINE_NAME="ETL Validation Pipeline ($PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
else
  ENV_NAME="development"
  PIPELINE_NAME="Validation Pipeline ($USER:$GIT_BRANCH $PROJ_ENVIRONMENT @ $START_DATE_TIME, N=$OCCURRENCES)"
fi
# Note: key/value are lower-case keywords here.
AWS_TAGS="key=user:project,value=data-warehouse key=user:env,value=$ENV_NAME"

PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER}_$$.json"
arthur.py render_template --prefix "$PROJ_ENVIRONMENT" validation_pipeline > "$PIPELINE_DEFINITION_FILE"

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

aws datapipeline create-pipeline \
    --unique-id validation_pipeline \
    --name "$PIPELINE_NAME" \
    --tags "$AWS_TAGS" \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

if [[ -z "$PIPELINE_ID" ]]; then
    set +x
    echo "Failed to find pipeline id in output -- pipeline probably wasn't created. Check your VPN etc."
    exit 1
fi

aws datapipeline put-pipeline-definition \
    --pipeline-definition "file://${PIPELINE_DEFINITION_FILE}" \
    --parameter-values \
        myS3Bucket="$PROJ_BUCKET" \
        myEtlEnvironment="$PROJ_ENVIRONMENT" \
        myStartDateTime="$START_DATE_TIME" \
        myOccurrences="$OCCURRENCES" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
