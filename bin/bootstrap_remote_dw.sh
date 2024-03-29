#! /bin/bash

# Use the environment previously synced to S3 to setup the DW configuration.

set -o errexit -o nounset

DATA_WAREHOUSE_CONFIG='./config/'
SCHEMAS="./schemas"

aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ARTHUR_DEFAULT_PREFIX/config/" $DATA_WAREHOUSE_CONFIG
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ARTHUR_DEFAULT_PREFIX/schemas/" $SCHEMAS
