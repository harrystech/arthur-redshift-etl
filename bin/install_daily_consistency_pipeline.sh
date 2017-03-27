#!/usr/bin/env bash

if [[ $# -ne 2 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment>"
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"

echo "** DEPRECATION WARNING **"
echo "This script has been deprecated!"
echo "You should be using: install_validation_pipeline.sh $CLUSTER_BUCKET $CLUSTER_ENVIRONMENT <DATETIME> <OCCURRENCES>"
echo "** DEPRECATION WARNING **"

# Verify that this bucket/environment pair is set up on s3
BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 1
fi

PIPELINE_ID_FILE="/tmp/pipeline_id_${USER}_$$.json"

set -e -x

aws datapipeline create-pipeline \
    --name "ETL Validation Pipeline ($CLUSTER_ENVIRONMENT)" \
    --unique-id daily_consistency_pipeline \
    --tags key=DataWarehouseEnvironment,value=Production \
    | tee "$PIPELINE_ID_FILE"

PIPELINE_ID=`jq --raw-output < "$PIPELINE_ID_FILE" '.pipelineId'`

aws datapipeline put-pipeline-definition \
    --pipeline-definition file://./aws_config/daily_consistency_pipeline.json \
    --parameter-values myS3Bucket="$CLUSTER_BUCKET" myEtlEnvironment="$CLUSTER_ENVIRONMENT" \
    --pipeline-id "$PIPELINE_ID"

aws datapipeline activate-pipeline --pipeline-id "$PIPELINE_ID"
