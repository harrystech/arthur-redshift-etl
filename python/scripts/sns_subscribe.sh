#!/usr/bin/env bash

# Create a topic (if necessary) and then subscribe the user

DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e -u

# === Command line args ===

show_usage_and_exit() {
    echo "Usage: `basename $0` [<environment>] <email_address>"
    echo "The environment defaults to \"$DEFAULT_PREFIX\"."
    exit ${1-0}
}

while getopts ":h" opt; do
  case $opt in
    h)
      show_usage_and_exit
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [[ $# -eq 1 ]]; then
    PROJ_ENVIRONMENT="$DEFAULT_PREFIX"
    NOTIFICATION_ENDPOINT="$1"
elif [[ $# -eq 2 ]]; then
    PROJ_ENVIRONMENT="$1"
    NOTIFICATION_ENDPOINT="$2"
else
    show_usage_and_exit 1
fi

set -x

# === Configuration ===

ENV_PREFIX=$( arthur.py show_value resource_prefix --prefix "$PROJ_ENVIRONMENT" )
STATUS_NAME="$ENV_PREFIX-status"
PAGE_NAME="$ENV_PREFIX-page"
VALIDATION_NAME="$ENV_PREFIX-validation"

# ===  Create topic and subscription ===

TOPIC_ARN_FILE="/tmp/topic_arn_${USER}$$.json"
trap "rm -f \"$TOPIC_ARN_FILE\"" EXIT

for TOPIC in "$STATUS_NAME" "$PAGE_NAME" "$VALIDATION_NAME"; do
    aws sns create-topic --name "$TOPIC" | tee "$TOPIC_ARN_FILE"
    TOPIC_ARN=`jq --raw-output < "$TOPIC_ARN_FILE" '.TopicArn'`
    if [[ -z "$TOPIC_ARN" ]]; then
        set +x
        echo "Failed to find topic arn in output. Check your settings, including VPN etc."
        exit 1
    fi
    aws sns set-topic-attributes --topic-arn "$TOPIC_ARN" --attribute-name DisplayName --attribute-value "ETL News"
    aws sns subscribe --topic-arn "$TOPIC_ARN" --protocol email --notification-endpoint "$NOTIFICATION_ENDPOINT"
done

set +x
echo
echo "List of subscriptions related to $PROJ_ENVIRONMENT:"
aws sns list-topics | jq --raw-output '.Topics[].TopicArn' | grep "$ENV_PREFIX"
