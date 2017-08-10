#! /bin/bash

set -e -u

if [[ $# -lt 2 || "$1" = "-h" ]]; then
    cat <<EOF
Usage: $0 ENV OBJECT_STORE [ParameterKey=name,ParameterValue=value ...]

Create/update a new stack for the data warehouse.  The name of the stack will be "dw-vpc-{ENV}".
Additional parameters are passed on to the AWS CLI.
EOF
    exit 0
fi

ENV_NAME="$1"
OBJECT_STORE="$2"
shift 2

BASE_NAME="dw-vpc"
STACK_NAME="${BASE_NAME}-${ENV_NAME}"
KEYPAIR="dw-${ENV_NAME}-keypair"

BINDIR=`dirname $0`
SCRIPT=`basename $0 .sh`

set -x

# Because of the "set -e", a failed validation will stop this script:
aws cloudformation validate-template --template-body "file://$BINDIR/vpc.yaml" >/dev/null

case "$SCRIPT" in

  create*)

    # Make sure the key pair exists:
    aws ec2 describe-key-pairs --key-names "$KEYPAIR" >/dev/null

    aws cloudformation create-stack \
        --on-failure DO_NOTHING \
        --template-body "file://$BINDIR/vpc.yaml" \
        --stack-name "$STACK_NAME" \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters \
            "ParameterKey=ObjectStore,ParameterValue=$OBJECT_STORE" \
            "ParameterKey=KeyName,ParameterValue=$KEYPAIR" \
            "$@" \
        --tags \
            "Key=user:project,Value=data-warehouse" \
            "Key=user:env,Value=$ENV_NAME"
    ;;

  update*)

    aws cloudformation update-stack \
        --template-body "file://$BINDIR/vpc.yaml" \
        --stack-name "$STACK_NAME" \
        --capabilities CAPABILITY_NAMED_IAM \
        --parameters \
            "ParameterKey=ObjectStore,ParameterValue=$OBJECT_STORE" \
            "ParameterKey=KeyName,ParameterValue=$KEYPAIR" \
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
