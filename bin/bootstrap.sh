#!/usr/bin/env bash

PROJ_TEMP="/tmp/redshift_etl"

if [[ ${1-"-h"} = "-h" ]]; then
    echo "Usage: $0 <bucket_name> <environment> [--local]"
    echo
    echo "Download files from the S3 bucket into $PROJ_TEMP on the instance"
    echo "and install the Python code in a virtual environment."
    echo "Unless the option --local is used, this also runs yum and updates hosts file."
    exit 0
fi

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Missing arguments!  See $0 -h"
    exit 1
fi

# Fail if any install step fails
set -e

BUCKET_NAME="$1"
ENVIRONMENT="$2"

case ${3-"--ec2"} in
    --local)
        RUNNING_LOCAL="yes"
        ;;

    --ec2)
        RUNNING_LOCAL="no"
        ;;
    *)
        echo "Bad option ([--local|--ec2]): $3"
        exit 1
        ;;
esac

log () {
    echo "`date '+%Y-%m-%d %H:%M:%S %Z'`: $*"
}

log "Starting \"$0 $BUCKET_NAME $ENVIRONMENT\""
set -x

# Install dependencies for Psycopg2, Arthur, and AWS shell commands we may run via datapipeline
if [[ "$RUNNING_LOCAL" = "no" ]]; then
    sudo yum install -y postgresql95-devel python35 python35-pip python35-devel aws-cli gcc libyaml-devel tmux
fi

# Install virtualenv using pip since the python35-virtualenv packages are out of date.
sudo pip-3.5 install --upgrade --disable-pip-version-check virtualenv

# Set creation mask to: u=rwx,g=rx,o=
umask 0027

# Send all files to temp directory
test -d "$PROJ_TEMP" || mkdir -p "$PROJ_TEMP"
cd "$PROJ_TEMP"

# Download code to all nodes, this includes Python code and its requirements.txt
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/jars/" ./jars/
aws s3 cp --only-show-errors --exclude '*' --include ping_cronut.sh --include bootstrap.sh --include sync_env.sh \
    --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/bin/" ./bin/
chmod +x ./bin/*.sh

# Download configuration (except for credentials when not in EC2)
if [[ "$RUNNING_LOCAL" = "no" ]]; then
    aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/
else
    aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/ --exclude credentials*.sh
fi

# Add custom hosts to EMR
if [[ "$RUNNING_LOCAL" = "no" ]]; then
    if ls ./config/*.hosts >/dev/null; then
        cat /etc/hosts ./config/*.hosts > /tmp/etc_hosts
        sudo cp /tmp/etc_hosts /etc/hosts
    fi
fi

# Create virtual env for ETL -- Make sure these steps match the description in the INSTALL.md!
virtualenv --python=python3 venv
source venv/bin/activate
pip3 install --upgrade pip --disable-pip-version-check
pip3 install --requirement ./jars/requirements.txt

# This trick with sed transforms project-<dotted version>.tar.gz into project.<dotted_version>.tar.gz
# so that the sort command can split correctly on '.' with the -t option.
# We then use the major (3), minor (4) and patch (5) version to sort numerically in reverse order.
LATEST_TAR_FILE=`ls -1 ./jars/redshift_etl*tar.gz |
    sed 's,redshift_etl-,redshift_etl.,' |
    sort -t. -n -r -k 3,3 -k 4,4 -k 5,5 |
    sed 's,redshift_etl\.,redshift_etl-,' |
    head -1`
pip3 install --upgrade "$LATEST_TAR_FILE"

set +x
log "Finished \"$0 $BUCKET_NAME $ENVIRONMENT\""
