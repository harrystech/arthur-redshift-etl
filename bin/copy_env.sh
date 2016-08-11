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

ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_ENVIRONMENT' (from '$CLUSTER_SOURCE_ENVIRONMENT')?"

set -e -x

if [[ "$CLUSTER_SOURCE_ENVIRONMENT" = "local" ]]; then
    aws s3 cp "bin/bootstrap.sh" "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bootstrap/"
    for FILE in config/*; do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/config/"
    done
    for FILE in python/requirements.txt \
                python/dist/redshift-etl-0.6.5.tar.gz \
                jars/commons-csv-1.4.jar \
                jars/postgresql-9.4.1208.jar \
                jars/RedshiftJDBC41-1.1.10.1010.jar \
                jars/spark-csv_2.10-1.4.0.jar
    do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/jars/"
    done
else
    for FOLDER in config jars bootstrap; do
        aws s3 cp --recursive "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER/" "s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/$FOLDER/"
    done
fi
