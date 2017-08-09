#! /bin/bash

set -e -u

if [[ $# -lt 2 || "$1" = "-h" ]]; then
    cat <<EOF
Usage: $0 ENV DW_VPC_NAME [ParameterKey=name,ParameterValue=value ...]

Create/update a new stack for the data warehouse.  The name of the stack will be "dw-cluster-{ENV}".
Additional parameters are passed on to the AWS CLI.
EOF
    exit 0
fi

ENV_NAME="$1"
DW_VPC_NAME="$2"
shift 2

BASE_NAME="dw-cluster"
STACK_NAME="${BASE_NAME}-${ENV_NAME}"

BINDIR=`dirname $0`
SCRIPT=`basename $0 .sh`

set -x

# Because of the "set -e", a failed validation will stop this script:
aws cloudformation validate-template --template-body "file://$BINDIR/cluster.yaml" >/dev/null

case "$SCRIPT" in

  create*)

    aws cloudformation create-stack \
        --on-failure DO_NOTHING \
        --template-body "file://$BINDIR/cluster.yaml" \
        --stack-name "$STACK_NAME" \
        --parameters \
            "ParameterKey=VpcStackName,ParameterValue=$DW_VPC_NAME" \
            "$@" \
        --tags \
            "Key=user:project,Value=data-warehouse" \
            "Key=user:env,Value=$ENV_NAME"
    ;;

  update*)

    aws cloudformation update-stack \
        --template-body "file://$BINDIR/cluster.yaml" \
        --stack-name "$STACK_NAME" \
        --parameters \
            "ParameterKey=VpcStackName,ParameterValue=$DW_VPC_NAME" \
            "$@" \
        --tags \
            "Key=user:project,Value=data-warehouse" \
            "Key=user:env,Value=$ENV_NAME"
    ;;

   *)
    echo "Unexpected: $SCRIPT"
    exit 1
    ;;

esac

set +x
echo "To see resources for this stack, run:"
echo "aws cloudformation list-stack-resources --stack-name \"$STACK_NAME\""
