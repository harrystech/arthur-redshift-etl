#!/usr/bin/env bash

CURRENT_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
DEFAULT_TIMEOUT=6

show_usage_and_exit() {
  cat <<USAGE

Rebuild ETL to extract, load (including transforms), and unload data.

Usage: $(basename "$0") <environment> [timeout] [myExtraLoadFlags]
Optional timeout should be the number of hours pipeline is allowed to run. Defaults to $DEFAULT_TIMEOUT.
Optional myExtraLoadFlags are passed to the rebuild load command

USAGE
  exit 0
}

set -o errexit -o nounset

# Verify that there is a local configuration directory
DEFAULT_CONFIG="${DATA_WAREHOUSE_CONFIG:-./config}"
if [[ ! -d "$DEFAULT_CONFIG" ]]; then
  echo 1>&2 "Failed to find \'$DEFAULT_CONFIG\' directory."
  echo 1>&2 "Make sure you are in the directory with your data warehouse setup or have DATA_WAREHOUSE_CONFIG set."
  exit 1
fi

POSITIONAL_ARGS=()
EXTRA_LOAD_FLAGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -h)
      show_usage_and_exit
      ;;
    -*)
      EXTRA_LOAD_FLAGS+=("$1") # save flag
      shift # past argument
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters

PROJ_BUCKET=$(arthur.py show_value object_store.s3.bucket_name)
PROJ_ENVIRONMENT="$1"
TIMEOUT="${2:-$DEFAULT_TIMEOUT}"

EXTRA_LOAD_FLAGS_STR="${EXTRA_LOAD_FLAGS[*]:-""}"

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
AWS_TAGS="key=user:project,value=data-warehouse key=user:sub-project,value=dw-etl key=user:data-pipeline-type,value=on-demand-rebuild"

PIPELINE_NAME="ETL On-Demand Rebuild Pipeline ($PROJ_ENVIRONMENT @ $CURRENT_TIMESTAMP)"
PIPELINE_DEFINITION_FILE="/tmp/pipeline_definition_${USER-nobody}_$$.json"
PIPELINE_ID_FILE="/tmp/pipeline_id_${USER-nobody}_$$.json"

# shellcheck disable=SC2064
trap "rm -f \"$PIPELINE_ID_FILE\"" EXIT

arthur.py render_template --prefix "$PROJ_ENVIRONMENT" ondemand_rebuild_pipeline > "$PIPELINE_DEFINITION_FILE"

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
        myTimeout="$TIMEOUT" \
        myExtraLoadFlags="${EXTRA_LOAD_FLAGS_STR}" \
    --pipeline-id "$PIPELINE_ID"

set +o xtrace
echo
echo "Your On-Demand Rebuild Pipeline ('$PIPELINE_ID') has been created!"
echo "You can start the pipeline by running the following command:"
echo "    aws datapipeline activate-pipeline --pipeline-id \"$PIPELINE_ID\""
