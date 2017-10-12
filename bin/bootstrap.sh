#!/usr/bin/env bash

PROJ_NAME="redshift_etl"
PROJ_PACKAGES="postgresql95-devel python35 python35-pip python35-devel aws-cli gcc libyaml-devel tmux jq"

PROJ_TEMP="/tmp/$PROJ_NAME"

if [[ ${1-"-h"} = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <environment> [--local]"
    echo
    echo "Download files from the S3 bucket into $PROJ_TEMP on the instance"
    echo "and install the Python code in a virtual environment."
    echo "Unless the option '--local' is used, this also runs yum to install packages:"
    echo "  $PROJ_PACKAGES"
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

log "Starting $PROJ_NAME bootstrap from bucket \"$BUCKET_NAME\" and prefix \"$ENVIRONMENT\""
set -x

if [[ "$RUNNING_LOCAL" = "no" ]]; then
    sudo yum install -y $PROJ_PACKAGES
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
aws s3 cp --only-show-errors --recursive \
    --exclude '*' --include ping_cronut.sh --include bootstrap.sh --include sync_env.sh \
    "s3://$BUCKET_NAME/$ENVIRONMENT/bin/" ./bin/
chmod +x ./bin/*.sh

# Download configuration (except for credentials when not in EC2)
if [[ "$RUNNING_LOCAL" = "no" ]]; then
    aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/
else
    aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/ --exclude credentials*.sh
fi

log "Creating virtual environment in \"$PROJ_TEMP/venv\""

# Create virtual env for ETL -- Make sure these steps match the description in the INSTALL.md!
virtualenv --python=python3 venv
source venv/bin/activate
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

if [[ "$RUNNING_LOCAL" = "no" ]]; then
    TMP_DOCUMENT="$PROJ_TEMP/document"
    trap "rm -f '$TMP_DOCUMENT'" EXIT
    curl --silent --show-error http://169.254.169.254/latest/dynamic/instance-identity/document | tee "$TMP_DOCUMENT"
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
fi

set +x
log "Finished \"$0 $BUCKET_NAME $ENVIRONMENT\""
