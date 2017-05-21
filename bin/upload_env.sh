#!/usr/bin/env bash

# This will create a new distribution locally and upload everything into S3.

if [[ "$0" =~ "setup_env" ]]; then
    echo "DEPRECATED Use instead: upload_env.sh $*"
    echo
fi

DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e

if [[ $# -lt 1 || $# -gt 2 || $1 = "-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> [<target_env>]"
    echo "    The <target_env> defaults to $DEFAULT_PREFIX."
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_TARGET_ENVIRONMENT="${2-$DEFAULT_PREFIX}"

ask_to_confirm () {
    while true; do
        read -r -p "$1 (y/[n]) " ANSWER
        case "$ANSWER" in
            y|Y)
                echo "Proceeding"
                break
                ;;
            *)
                echo "Bailing out"
                exit 0
                ;;
        esac
    done
}

ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_TARGET_ENVIRONMENT'?"

if [[ -z "$DATA_WAREHOUSE_CONFIG" ]]; then
    echo "Cannot find configuration files.  Please set DATA_WAREHOUSE_CONFIG to a directory."
    exit 2
elif [[ ! -d "$DATA_WAREHOUSE_CONFIG" ]]; then
    echo "Expected DATA_WAREHOUSE_CONFIG to point to a directory"
    exit 2
elif [[ -d "$DATA_WAREHOUSE_CONFIG/config" ]]; then
    echo "Expected DATA_WAREHOUSE_CONFIG to point to a config directory, not the root directory."
    echo "(Found directory $DATA_WAREHOUSE_CONFIG/config which is unexpected.)"
    exit 2
fi

set -u

if ! aws s3 ls "s3://$CLUSTER_BUCKET/" > /dev/null; then
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" exists and you have access to it!"
    exit 2
fi

if [[ ! -r setup.py ]]; then
    echo "Failed to find 'setup.py' file"
    exit 2
fi

echo "Creating Python dist file, then uploading files (including configuration, excluding credentials) to S3"
set -x

# Collect release information
RELEASE_FILE="/tmp/setup_env_release_${USER}$$.txt"
> "$RELEASE_FILE"
trap "rm \"$RELEASE_FILE\"" EXIT

# python3 setup.py --fullname >> "$RELEASE_FILE"
git rev-parse --show-toplevel >> "$RELEASE_FILE"
git rev-parse HEAD >> "$RELEASE_FILE"
date "+%Y-%m-%d %H:%M:%S%z" >> "$RELEASE_FILE"
aws s3 cp "$RELEASE_FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config/release.txt"

python3 setup.py sdist
LATEST_TAR_FILE=`ls -1t dist/redshift-etl*tar.gz | head -1`
for FILE in requirements.txt "$LATEST_TAR_FILE"
do
    aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars/"
done

aws s3 sync --delete bin "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/bin"
aws s3 sync --delete \
    --exclude "*" \
    --include "*.yaml" \
    --include "*.sh" \
    --include "*.hosts" \
    --exclude "release.txt" \
    --exclude "credentials*.sh" \
    "$DATA_WAREHOUSE_CONFIG" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config"
# Users who don't intend to use Spark may not have the jars directory.
if [[ -d "jars" ]]; then
    aws s3 sync --delete \
        --exclude "*" \
        --include postgresql-9.4.1208.jar \
        --include RedshiftJDBC41-1.2.1.1001.jar \
        jars "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars"
fi

set +x
echo
echo "You should *now* run:"
echo "arthur.py sync --prefix \"$CLUSTER_TARGET_ENVIRONMENT\""
