#! /bin/bash

set -e -u

if [[ $# -ne 1 || "$1" = "-h" ]]; then
    cat <<EOF
Usage: $0 ENV

Delete a given stack for the data warehouse.  The name of the stack to be deleted is "dw-vpc-{ENV}".
EOF
    exit 0
fi

ENV_NAME="$1"
BASE_NAME="dw-vpc"
STACK_NAME="${BASE_NAME}-${ENV_NAME}"

set -x
aws cloudformation delete-stack --stack-name "$STACK_NAME"
