#! /bin/bash

# Create a credentials file for the validation pipeline.

set -o errexit -o nounset

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-y] <bucket_name> <prefix>

This will create an additional credentials file needed for the validation pipeline.
By changning the target database, we make sure that validation runs on an empty
and separate database.

EOF
    exit ${1-0}
}

while getopts ":h" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

if [[ $# -ne 2 ]]; then
    show_usage_and_exit 1
fi

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="$2"
S3_SOURCE_CREDENTIALS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/credentials.sh"
S3_TARGET_CREDENTIALS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/credentials_validation.sh"

if ! aws s3 ls "$S3_SOURCE_CREDENTIALS" > /dev/null; then
    echo "Check whether you have access to \"$S3_SOURCE_CREDENTIALS\"!"
    exit 2
fi

set -o xtrace

TMP_CREDENTIALS_FILE="/tmp/update_validation_credentials_${USER-nobody}_$$"
trap "rm -f \"$TMP_CREDENTIALS_FILE\"" EXIT

# Step 1: Download credentials
aws s3 cp "$S3_SOURCE_CREDENTIALS" "$TMP_CREDENTIALS_FILE"
# Step 2: ???
sed < "$TMP_CREDENTIALS_FILE"  -n "/DATA_WAREHOUSE_ETL=/s/development/validation_$ARTHUR_DEFAULT_PREFIX/p"
# Step 3: Profit
aws s3 cp "$TMP_CREDENTIALS_FILE" "$S3_TARGET_CREDENTIALS"
