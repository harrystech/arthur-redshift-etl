#! /bin/bash

# Sync (meaning: overwrite) one environment with another.

set -eu

show_usage_and_exit () {
    cat <<EOF
Usage: `basename $0` [-y] <bucket_name> <source_env> <target_env>

This will sync the target environmnet with the source environment.
You normally specify environments by their prefix in S3.

Unless you pass in '-y', you will have to confirm the sync operation since
it is potentially destructive.
EOF
    exit ${1-0}
}

CONFIRMED_YES=no

while getopts ":hy" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      y)
        CONFIRMED_YES=yes
        shift
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

if [[ $# -ne 3 ]]; then
    show_usage_and_exit 1
fi

CLUSTER_BUCKET="$1"
CLUSTER_SOURCE_ENVIRONMENT="$2"
CLUSTER_TARGET_ENVIRONMENT="$3"

if ! aws s3 ls "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/" > /dev/null; then
    echo "Check whether the folder \"$CLUSTER_SOURCE_ENVIRONMENT\" exists in bucket \"$CLUSTER_BUCKET\" and that you have access to it!"
    exit 2
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

if [[ "$CONFIRMED_YES" != "yes" ]]; then
    ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_TARGET_ENVIRONMENT'?"
fi

set -x

for FOLDER in bin config data jars schemas; do
    aws s3 sync --delete --exclude 'credentials*.sh' \
        "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER" \
        "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/$FOLDER"
done

if ! aws s3 sync --exclude '*' --include 'credentials*.sh' \
        "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/config" \
        "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config"
then
    set +x
    echo "You will have to copy your credentials manually!"
fi
