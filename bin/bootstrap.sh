#!/usr/bin/env bash

if [[ ${1-"-h"} = "-h" ]]; then
    echo "Usage: $0 <bucket_name> <environment>"
    echo
    echo "Download files from the S3 bucket into the <environment>/jars folder"
    echo "and install the Python code in a virtual environment."
    exit 0
fi

if [[ $# -ne 2 ]]; then
    echo "Missing arguments!  See $0 -h"
    exit 1
fi

# Fail if any install step fails
set -e -x

BUCKET_NAME="$1"
ETL_ENVIRONMENT="$2"
S3_PATH="s3://$BUCKET_NAME/$ETL_ENVIRONMENT"

# Download code to all nodes
aws s3 cp --recursive "$S3_PATH/jars/" ./jars

# Install requirements into virtual environment
aws s3 cp "$S3_PATH/config/requirements.txt" ./requirements.txt

mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --requirement ./requirements.txt --disable-pip-version-check
pip3 install ./jars/redshift
