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

# Install dependencies for Psycopg2, Arthur, and AWS shell commands we may run via datapipeline
sudo yum install -y postgresql94-devel python34 python34-pip python34-virtualenv aws-cli gcc

# Send all files to temp directory
test -d "$REDSHIFT_ETL_HOME" || mkdir -p "$REDSHIFT_ETL_HOME"
cd "$REDSHIFT_ETL_HOME"

# Download code to all nodes, this includes Python code and requirements.txt
aws s3 cp --recursive "s3://$BUCKET_NAME/$ETL_ENVIRONMENT/jars/" ./jars/

# Download configuration and credentials
aws s3 cp --recursive "s3://$BUCKET_NAME/$ETL_ENVIRONMENT/config/" ./config/

# Add custom hosts to EMR
if ls ./config/*.hosts >/dev/null; then
    cat /etc/hosts ./config/*.hosts > /tmp/etc_hosts
    sudo cp /tmp/etc_hosts /etc/hosts
fi

# Write file for Sqoop to be able to connect using SSL to upstream sources
test -d sqoop || mkdir sqoop
cat > ./sqoop/ssl.props <<EOF
ssl=true
sslfactory=org.postgresql.ssl.NonValidatingFactory
EOF

# Create virtual env for ETL
test -d venv || mkdir venv
for VIRTUALENV in "virtualenv-3.4" "virtualenv"; do
	if hash $VIRTUALENV 2> /dev/null; then
		break
	fi
done

$VIRTUALENV --python=python3 venv

source venv/bin/activate
pip3 install --requirement ./jars/requirements.txt --disable-pip-version-check

# This trick with sed transforms project-<dotted version>.tar.gz into project.<dotted_version>.tar.gz
# so that the sort command can split correctly on '.' with the -t option.
LATEST_TAR_FILE=`ls -1 ./jars/redshift-etl*tar.gz |
    sed 's,redshift-etl-,redshift-etl.,' |
    sort -t. -n -k 2,2 -k 3,3 -k 4,4 -s -r |
    sed 's,redshift-etl\.,redshift-etl-,' |
    head -1`
pip3 install --upgrade "$LATEST_TAR_FILE" --disable-pip-version-check
