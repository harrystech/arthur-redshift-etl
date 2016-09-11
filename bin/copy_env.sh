#!/usr/bin/env bash

# Copy production setup to new env

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: `basename $0` <bucket_name> [<source_env>] <target_env>"
    echo "If the source_env is 'local', then copy files from your local files, not another folder."
    echo "The source_env defaults to 'production'."
    exit 0
fi

CLUSTER_BUCKET="$1"
if [[ $# -eq 3 ]]; then
    CLUSTER_SOURCE_ENVIRONMENT="$2"
    CLUSTER_TARGET_ENVIRONMENT="$3"
else
    CLUSTER_SOURCE_ENVIRONMENT="production"
    CLUSTER_TARGET_ENVIRONMENT="$2"
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

ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_TARGET_ENVIRONMENT'?"


if [[ "$CLUSTER_SOURCE_ENVIRONMENT" = "local" ]]; then

    if [[ -z "$DATA_WAREHOUSE_CONFIG" ]]; then
        echo "Cannot find configuration files.  Please set DATA_WAREHOUSE_CONFIG."
        exit 2
    elif [[ ! -d "$DATA_WAREHOUSE_CONFIG" ]]; then
        echo "Expected DATA_WAREHOUSE_CONFIG to point to a directory"
        exit 2
    fi

    echo "Creating Python dist file, then uploading files (including configuration, excluding credentials) to s3"
    set -e -x

    python3 setup.py sdist
    LATEST_TAR_FILE=`ls -1t dist/redshift-etl*tar.gz | head -1`
    for FILE in requirements.txt "$LATEST_TAR_FILE"
    do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars/"
    done

    aws s3 sync --delete bootstrap "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/bootstrap"
    aws s3 sync --delete \
        --include "*.yaml" \
        --include "*.sh" \
        --include "*.hosts" \
        --exclude "credentials*" \
        "$DATA_WAREHOUSE_CONFIG" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config"
    aws s3 sync --delete \
        --exclude "*" \
        --include commons-csv-1.4.jar \
        --include postgresql-9.4.1208.jar \
        --include RedshiftJDBC41-1.1.10.1010.jar \
        --include spark-csv_2.10-1.4.0.jar \
        jars "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars"

else
    set -e -x
    for FOLDER in bootstrap config jars schemas; do
        aws s3 sync --delete --exclude "credentials*" \
            "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER" \
            "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/$FOLDER"
    done
fi
