#!/usr/bin/env bash

# Copy production setup to new env

if [[ "X$1" = "X-h" ]]; then
    echo "Usage: `basename $0` <bucket_name> <target_env> [<source_env>]"
    exit 0
fi

CLUSTER_BUCKET="${1?'Missing bucket name'}"
CLUSTER_ENVIRONMENT="${2?'Missing name of new environment'}"
CLUSTER_SOURCE_ENVIRONMENT="${3-production}"

if [[ "$CLUSTER_ENVIRONMENT" = "production" ]]; then
    echo >&2 "Cannot overwrite production setup (maybe try promote_env.sh instead?)"
    exit 1
fi

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

ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_ENVIRONMENT?'"

set -e -x

for FOLDER in config jars bootstrap; do
    aws s3 cp --recursive "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER/" "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/$FOLDER/"
done
