#!/usr/bin/env bash

# This will create a new distribution locally and upload everything into S3.

DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e

if [[ $# -gt 2 || "$1" = "-h" ]]; then
    echo "Usage: `basename $0` [[<bucket_name>] <target_env>]"
    echo "    The <target_env> defaults to \"$DEFAULT_PREFIX\"."
    echo "    The <bucket_name> defaults to your object store setting."
    echo "    If the bucket name is not specified, variable DATA_WAREHOUSE_CONFIG must be set."
    exit 0
fi

if [[ $# -lt 2 && -z "$DATA_WAREHOUSE_CONFIG" ]]; then
    echo "You must set DATA_WAREHOUSE_CONFIG when not specifying the bucket name."
    exit 1
fi

if [[ $# -eq 2 ]]; then
    PROJ_BUCKET="$1"
    PROJ_TARGET_ENVIRONMENT="$2"
elif [[ $# -eq 1 ]]; then
    echo "Finding bucket name in configuration..."
    PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
    PROJ_TARGET_ENVIRONMENT="$1"
else
    echo "Finding bucket name and prefix in configuration..."
    PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
    PROJ_TARGET_ENVIRONMENT=$( arthur.py show_value --prefix "$DEFAULT_PREFIX" object_store.s3.prefix )
fi

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

if [[ ! -r ./setup.py ]]; then
    echo "Failed to find 'setup.py' file in the local directory."
    exit 2
fi

if ! aws s3 ls "s3://$PROJ_BUCKET/" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" exists and you have access to it!"
    echo "(Hint: If you spelled the name correctly, is your VPN connected?)"
    exit 2
fi

if aws s3 ls "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars" > /dev/null; then
    ask_to_confirm "Are you sure you want to overwrite 's3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT'?"
else
    ask_to_confirm "Are you sure you want to create 's3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT'?"
fi

echo "Creating Python dist file, then uploading files (including configuration, excluding credentials) to S3"

set -x -u

# Collect release information
RELEASE_FILE="/tmp/upload_env_release_${USER}$$.txt"
> "$RELEASE_FILE"
trap "rm \"$RELEASE_FILE\"" EXIT

echo "toplevel=`git rev-parse --show-toplevel`" >> "$RELEASE_FILE"
GIT_COMMIT_HASH=$(git rev-parse HEAD)
if GIT_LATEST_TAG=$(git describe --exact-match --tags HEAD); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_LATEST_TAG)" >> "$RELEASE_FILE"
elif GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_BRANCH)" >> "$RELEASE_FILE"
else
    echo "commit=$GIT_COMMIT_HASH" >> "$RELEASE_FILE"
fi
echo "date=`date '+%Y-%m-%d %H:%M:%S%z'`" >> "$RELEASE_FILE"
cat "$RELEASE_FILE" > "python/etl/config/release.txt"

python3 setup.py sdist
LATEST_TAR_FILE=`ls -1t dist/redshift_etl*tar.gz | head -1`
for FILE in requirements.txt "$LATEST_TAR_FILE"
do
    aws s3 cp "$FILE" "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars/"
done

aws s3 sync --delete bin "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/bin"

# Users who don't intend to use Spark may not have the jars directory.
if [[ -d "jars" ]]; then
    aws s3 sync --delete \
        --exclude "*" \
        --include postgresql-9.4.1208.jar \
        --include RedshiftJDBC41-1.2.1.1001.jar \
        jars "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars"
fi

set +x
echo
echo "# You should *now* run inside your warehouse repo directory:"
echo "arthur.py sync --deploy --prefix \"$PROJ_TARGET_ENVIRONMENT\""
