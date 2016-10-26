#!/usr/bin/env bash

if [[ $# -lt 1 || $# -gt 3 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> [<environment>] [<loader_environment>]"
    echo "The environment defaults to 'production'. The loader environment defaults to 'development'"
    exit 0
fi

CLUSTER_BUCKET="${1?'Missing bucket name'}"
CLUSTER_ENVIRONMENT="${2-production}"
LOADER_ENVIRONMENT="${3-development}"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 1
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -x

aws datapipeline create-pipeline \
    --name "ETL Pipeline ($CLUSTER_ENVIRONMENT & $LOADER_ENVIRONMENT)" \
    --unique-id redshift_etl_pipeline \
    --tags key=HarrysDataWarehouse,value=Production \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/data_pipeline.json \
    --parameter-values myS3Bucket="$CLUSTER_BUCKET" myEtlEnvironment="$CLUSTER_ENVIRONMENT" mySecondaryLoadEnvironment="$LOADER_ENVIRONMENT" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
