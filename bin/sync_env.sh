#! /bin/bash

# Sync one environment over another.

if [[ $# -ne 3 ]]; then
    echo "Usage: `basename $0` <bucket_name> <source_env> <target_env>"
    exit 0
fi

CLUSTER_BUCKET="$1"
CLUSTER_SOURCE_ENVIRONMENT="$2"
CLUSTER_TARGET_ENVIRONMENT="$3"

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

set -e -x

for FOLDER in bin config data jars schemas; do
    aws s3 sync --delete --exclude 'credentials*.sh' "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER"  "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/$FOLDER"
done
