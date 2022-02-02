#! /bin/bash

set -o errexit -o nounset
alias arthur.py=/opt/local/redshift_etl/venv/bin/arthur.py
shopt -s expand_aliases

export DATA_WAREHOUSE_CONFIG='./config/'
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ARTHUR_DEFAULT_PREFIX/config/" $DATA_WAREHOUSE_CONFIG
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ARTHUR_DEFAULT_PREFIX/schemas/" ./schemas

exec "$@"
