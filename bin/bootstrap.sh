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

BUCKET_NAME="$1"
ETL_ENVIRONMENT="$2"
REDSHIFT_ETL_HOME="/tmp/redshift_etl"

# Fail if any install step fails
set -e -x

# Set creation mask to: u=rwx,g=rx,o=
umask 0027

# PostgreSQL headers are needed to build psycopg2
sudo yum install -y postgresql94-devel

# Send all files to temp directory
test -d "$REDSHIFT_ETL_HOME" || mkdir -p "$REDSHIFT_ETL_HOME"
cd "$REDSHIFT_ETL_HOME"

# Download code to all nodes, this includes Python code and requirements.txt
aws s3 cp --recursive "s3://$BUCKET_NAME/$ETL_ENVIRONMENT/jars/" ./jars/

# Download configuration and credentials
aws s3 cp --recursive "s3://$BUCKET_NAME/$ETL_ENVIRONMENT/config/" ./config/

# Write file for Sqoop to be able to connect using SSL to upstream sources
test -d sqoop || mkdir sqoop
cat > ./sqoop/ssl.props <<EOF
ssl=true
sslfactory=org.postgresql.ssl.NonValidatingFactory
EOF

# Create virtual env for ETL
test -d venv || mkdir venv
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --requirement ./jars/requirements.txt --disable-pip-version-check
pip3 install --upgrade ./jars/redshift-etl-0.8.2.tar.gz --disable-pip-version-check
