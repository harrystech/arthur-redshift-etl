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
        echo "Expected $DATA_WAREHOUSE_CONFIG to be a directory"
        exit 2
    fi
    echo "Creating Python dist file, then uploading files (including configuration) to s3"

    set -e -x

    python3 setup.py sdist

    for FILE in requirements.txt \
                dist/redshift-etl-0.8.1.tar.gz
    do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars/"
    done

    aws s3 cp "bin/bootstrap.sh" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/bootstrap/"

    for FILE in "$DATA_WAREHOUSE_CONFIG"/*; do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config/"
    done

    for FILE in jars/commons-csv-1.4.jar \
                jars/postgresql-9.4.1208.jar \
                jars/RedshiftJDBC41-1.1.10.1010.jar \
                jars/spark-csv_2.10-1.4.0.jar
    do
        aws s3 cp "$FILE" "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/jars/"
    done

else
    set -e -x
    for FOLDER in bootstrap config jars schemas; do
        aws s3 sync --delete --exclude "credentials*" \
            "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER" \
            "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/$FOLDER"
    done
fi
