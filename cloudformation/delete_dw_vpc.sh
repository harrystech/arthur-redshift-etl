#! /bin/bash

set -e -u

case "$0" in
    *vpc*)
      BASE_NAME="dw-vpc"
      ;;
    *cluster*)
      BASE_NAME="dw-cluster"
      ;;
esac


if [[ $# -ne 1 || "$1" = "-h" ]]; then
    cat <<EOF
Usage: $0 ENV

Delete a given stack for the data warehouse.  The name of the stack to be deleted is "${BASE_NAME}-{ENV}".
EOF
    exit 0
fi

ENV_NAME="$1"
STACK_NAME="${BASE_NAME}-${ENV_NAME}"

set -x
aws cloudformation delete-stack --stack-name "$STACK_NAME"
