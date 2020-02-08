#!/usr/bin/env bash

# Setup the environment to run Arthur on an EC2 instance.
# Make sure to keep this in sync with our Dockerfile.

PROJ_NAME="redshift_etl"
PROJ_PACKAGES="aws-cli gcc jq libyaml-devel postgresql95-devel python35 python35-devel python35-pip tmux"

PROJ_TEMP="/tmp/$PROJ_NAME"

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` <bucket_name> <environment>

Download files from the S3 bucket into $PROJ_TEMP on this instance
and install the Python code in a virtual environment.
We will also try to set tags for this instance.
EOF
    exit ${1-0}
}

log () {
    set +o xtrace
    echo "`date '+%Y-%m-%d %H:%M:%S %Z'`: $*"
}

if [[ ${1-"-h"} = "-h" ]]; then
    show_usage_and_exit
fi

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Missing arguments!" 1>&2
    show_usage_and_exit 1
fi

# Fail if any install step fails or variables are not set before use.
set -o errexit -o nounset

BUCKET_NAME="$1"
ENVIRONMENT="$2"
log "Starting $PROJ_NAME bootstrap from bucket \"$BUCKET_NAME\" and prefix \"$ENVIRONMENT\""

set -o xtrace

sudo yum install --assumeyes $PROJ_PACKAGES

# Install virtualenv using pip since the python35-virtualenv packages are out of date.
sudo pip-3.5 install --upgrade --disable-pip-version-check virtualenv

# Set creation mask to: u=rwx,g=rx,o=
umask 0027

# Download code config to all nodes. This includes Python code and its requirements.txt
log "Downloading files from s3://$BUCKET_NAME/$ENVIRONMENT/ to $PROJ_TEMP"
test -d "$PROJ_TEMP" || mkdir -p "$PROJ_TEMP"
cd "$PROJ_TEMP"

aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/jars/" ./jars/
aws s3 cp --only-show-errors --recursive \
    --exclude '*' --include bootstrap.sh --include ping_cronut.sh --include sync_env.sh \
    "s3://$BUCKET_NAME/$ENVIRONMENT/bin/" ./bin/
chmod +x ./bin/*.sh

log "Creating virtual environment in \"$PROJ_TEMP/venv\""
test -x deactivate && deactivate || echo "Already outside virtual environment"

virtualenv --python=python3 venv

# Work around this error: "_OLD_VIRTUAL_PATH: unbound variable"
set +o nounset
source venv/bin/activate
set -o nounset

pip3 install --upgrade pip --disable-pip-version-check
pip3 install --requirement ./jars/requirements.txt

# This trick with sed transforms project-<dotted version>.tar.gz into project.<dotted_version>.tar.gz
# so that the sort command can split correctly on '.' with the -t option.
# We then use the major (3), minor (4) and patch (5) version to sort numerically in reverse order.
LATEST_TAR_FILE=`
    ls -1 ./jars/ |
    sed -n "s:${PROJ_NAME}-:${PROJ_NAME}.:p" |
    sort -t. -n -r -k 3,3 -k 4,4 -k 5,5 |
    sed "s:${PROJ_NAME}\.:${PROJ_NAME}-:" |
    head -1`
pip3 install --upgrade "./jars/$LATEST_TAR_FILE"

# Update instance tags
TMP_DOCUMENT="$PROJ_TEMP/document"
trap "rm -f '$TMP_DOCUMENT'" EXIT
curl --silent --show-error http://169.254.169.254/latest/dynamic/instance-identity/document | tee "$TMP_DOCUMENT"
echo
INSTANCE_ID=`jq -r ".instanceId" "$TMP_DOCUMENT"`
REGION=`jq -r ".region" "$TMP_DOCUMENT"`
JOB_FLOW_ID=$( aws ec2 describe-tags --region "$REGION" \
                   --filters "Name=resource-id,Values=$INSTANCE_ID" "Name=key,Values=aws:elasticmapreduce:job-flow-id" |
               jq -r ".Tags[0].Value // empty" )

if [[ -n "$INSTANCE_ID" ]]; then
    if [[ -n "$JOB_FLOW_ID" ]]; then
        INSTANCE_NAME="$PROJ_NAME ($BUCKET_NAME\, $ENVIRONMENT\, $JOB_FLOW_ID)"
    else
        INSTANCE_NAME="$PROJ_NAME ($BUCKET_NAME\, $ENVIRONMENT)"
    fi
    aws ec2 create-tags --resources "$INSTANCE_ID" --region "$REGION" --tags Key=Name,Value="$INSTANCE_NAME"
    aws ec2 create-tags --resources "$INSTANCE_ID" --region "$REGION" --tags Key=user:name,Value="$INSTANCE_NAME"
fi

set +o xtrace
log "Finished \"$0 $BUCKET_NAME $ENVIRONMENT\""
