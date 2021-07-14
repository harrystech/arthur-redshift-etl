#!/usr/bin/env bash
# MIT License
# 
# Copyright (c) 2016 Harry's, Inc.
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Setup the environment to run Arthur on an EC2 instance.
# Make sure to keep this in sync with our Dockerfile.

PROJ_NAME="redshift_etl"
PROJ_PACKAGES="aws-cli gcc jq libyaml-devel tmux"

PROJ_TEMP="/home/hadoop/${PROJ_NAME}"  # Should be rendered from arthur_settings.etl_temp_dir

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

# Set creation mask to: u=rwx,g=rx,o=
umask 0027

# Download code config to all nodes. This includes Python code and its requirements.txt
log "Downloading files from s3://$BUCKET_NAME/$ENVIRONMENT/ to $PROJ_TEMP"
test -d "$PROJ_TEMP" || mkdir -p "$PROJ_TEMP"
cd "$PROJ_TEMP"

aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/config/" ./config/
aws s3 cp --only-show-errors --recursive "s3://$BUCKET_NAME/$ENVIRONMENT/jars/" ./jars/
aws s3 cp --only-show-errors --recursive \
	--exclude '*' --include bootstrap.sh --include send_health_check.sh --include sync_env.sh \
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
