#! /bin/bash

# Sync one environment over another.

show_help () {
    echo "Usage: `basename $0` [-y] [-c] <bucket_name> <source_env> <target_env>"
    echo
    echo "This will sync the target environmnet with the source environment."
    echo "You normally specify environments by their prefix in S3."
    echo "Unless you pass in '-y', you will have to confirm the sync operation since"
    echo "it is potentially destructive."
    echo "If '-c' is passed in, an attempt will be made to copy credentials, but is"
    echo "not an error if that fails."
}

OPTIND=1         # Reset in case getopts has been used previously in the shell.

CONFIRMED_YES=no
COPY_CREDENTIALS=no

while getopts ":h?yc" opt; do
    case "$opt" in
    h|\?)
        show_help
        exit 0
        ;;
    y)  CONFIRMED_YES=yes
        ;;
    c)  COPY_CREDENTIALS=yes
        ;;
    esac
done

shift $((OPTIND-1))

if [[ $# -ne 3 ]]; then
    show_help
    exit 1
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

if [[ "$CONFIRMED_YES" != "yes" ]]; then
    ask_to_confirm "Are you sure you want to overwrite '$CLUSTER_TARGET_ENVIRONMENT'?"
fi

set -e -x

for FOLDER in bin config data jars schemas; do
    aws s3 sync --delete --exclude 'credentials*.sh' \
        "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/$FOLDER" \
        "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/$FOLDER"
done

set +x

if [[ "$COPY_CREDENTIALS" = "yes" ]]; then
    set -x
    if ! aws s3 sync --exclude '*' --include 'credentials*.sh' \
            "s3://$CLUSTER_BUCKET/$CLUSTER_SOURCE_ENVIRONMENT/config" \
            "s3://$CLUSTER_BUCKET/$CLUSTER_TARGET_ENVIRONMENT/config"
    then
        set +x
        echo "You will have to copy your credentials manually."
    fi
fi
