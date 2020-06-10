#!/usr/bin/env bash

# This will create a new distribution locally and upload everything into S3.
#
# You'll need to set AWS_PROFILE to something with appropriate permissions
# if your default profile doesn't have the needed S3 access.
#
# Example:
#   AWS_PROFILE=my-prof bin/upload_env.sh my-warehouse-bucket my-env

set -o errexit

USER="${USER-nobody}"
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

show_usage_and_exit () {
    cat <<USAGE

Usage: `basename $0` [-y] [[<bucket_name>] <target_env>]

This creates a new distribution and uploads it into S3.
The <target_env> defaults to "$DEFAULT_PREFIX".
The <bucket_name> defaults to your object store setting.
If the bucket name is not specified, the variable DATA_WAREHOUSE_CONFIG must be set.

You can specify "-y" to skip the confirmation question.

USAGE

    exit ${1-0}
}

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

assume_yes="NO"
while getopts ":hq" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      y)
        assume_yes="YES"
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        show_usage_and_exit 1
      ;;
    esac
done
shift $((OPTIND -1))

if [[ $# -gt 2 ]]; then
    echo "Too many arguments"
    show_usage_and_exit 1
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

set -o nounset

DIR_NAME=`dirname $0`
BIN_PATH=$(\cd "$DIR_NAME" && \pwd)
TOP_PATH=`dirname "$BIN_PATH"`
DOCKER_IMAGE_PATH=/arthur-redshift-etl

if [[ ! -r ./setup.py ]]; then
    echo "Failed to find 'setup.py' file in the local directory."
    if [[ ! -r "$TOP_PATH/setup.py" ]]; then
        echo "Failed to find '$TOP_PATH/setup.py' file."
        if [[ ! -r "$DOCKER_IMAGE_PATH/setup.py" ]]; then
            echo "Failed to find '$DOCKER_IMAGE_PATH/setup.py' file."
            echo "Bailing out: Cannot find distribution."
            exit 2
        else
            echo "OK, found '$DOCKER_IMAGE_PATH/setup.py' instead."
            cd "$DOCKER_IMAGE_PATH"
        fi
    else
        echo "OK, found '$TOP_PATH/setup.py' instead."
        cd "$TOP_PATH"
    fi
fi

if ! aws s3 ls "s3://$PROJ_BUCKET/" > /dev/null; then
    echo "Check whether the bucket \"$PROJ_BUCKET\" exists and you have access to it!"
    echo "(Hint: If you spelled the name correctly, is your VPN connected?)"
    exit 2
fi

if [[ "$assume_yes" = "YES" ]]; then
    true
elif aws s3 ls "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars" > /dev/null; then
    ask_to_confirm "Are you sure you want to overwrite 's3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT'?"
else
    ask_to_confirm "Are you sure you want to create 's3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT'?"
fi

# Collect release information
bin/release_version.sh || echo "File with release information was not updated!"

echo "Creating Python dist file, then uploading files (including configuration, excluding credentials) to S3"

set -o xtrace

python3 setup.py sdist
LATEST_TAR_FILE=`ls -1t dist/redshift_etl*tar.gz | head -1`
for FILE in requirements.txt "$LATEST_TAR_FILE"
do
    aws s3 cp "$FILE" "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars/"
done

aws s3 sync --delete \
    --exclude '*' --include bootstrap.sh --include send_health_check.sh --include sync_env.sh \
    bin "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/bin"

# Users who don't intend to use Spark may not have the jars directory.
if [[ -d "jars" ]]; then
    aws s3 sync --delete \
        --exclude '*' \
        --include postgresql-9.4.1208.jar \
        --include RedshiftJDBC41-1.2.1.1001.jar \
        jars "s3://$PROJ_BUCKET/$PROJ_TARGET_ENVIRONMENT/jars"
fi

set +o xtrace

# If you're confident enough to use "-y", you should know already about next steps.
if [[ "$assume_yes" != "YES" ]]; then
    echo
    echo "# You should *now* sync your data warehouse::"
    echo "arthur.py sync --deploy --prefix \"$PROJ_TARGET_ENVIRONMENT\""
fi
