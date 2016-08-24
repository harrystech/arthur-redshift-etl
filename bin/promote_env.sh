#!/usr/bin/env bash

# Copy staging setup to production env

CLUSTER_BUCKET="${1?'Missing bucket name'}"
CLUSTER_ENVIRONMENT=staging

ask_to_confirm () {
    while true; do
        read -r -p "$1 [y/N] " ANSWER
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

ask_to_confirm "Are you sure you want to promote staging to production?'"

set -e -x

for FOLDER in config jars bootstrap schemas; do
    aws s3 cp --recursive "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/$FOLDER/" "s3://$CLUSTER_BUCKET/production/$FOLDER/"
done
