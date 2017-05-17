#!/usr/bin/env bash

# Start a EMR cluster in AWS for a running ETL steps.
#   Non-interactive jobs will run the steps and then quit.
#   Interactive jobs will run the steps and then wait for additional work.
#
# Checkout the setup_env.sh and sync_env.sh scripts to have files ready in S3 for the EMR cluster.
#
# TODO Find a better way to parameterize cluster, check out cloud formation?

DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e -u

# === Command line args ===

show_usage_and_exit() {
    echo "Usage: $0 [-i] <bucket_name> [<environment>]"
    echo "The environment defaults to \"$DEFAULT_PREFIX\"."
    exit ${1-0}
}

CLUSTER_IS_INTERACTIVE=no
while getopts ":hi" opt; do
  case $opt in
    h)
      show_usage_and_exit
      ;;
    i)
      CLUSTER_IS_INTERACTIVE=yes
      shift
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [[ $# -lt 1 || $# -gt 2 ]]; then
    show_usage_and_exit 1
fi

set -x

# === Basic configuration ===

PROJ_BUCKET="$1"
PROJ_ENVIRONMENT="${2-$DEFAULT_PREFIX}"

CLUSTER_RELEASE_LABEL="emr-5.0.0"
CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"},{"Name":"Zeppelin"},{"Name":"Sqoop"}]'
CLUSTER_REGION="us-east-1"

# === Derived configuration ===

CLUSTER_LOGS="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/logs/"
CLUSTER_NAME="ETL Cluster ($PROJ_ENVIRONMENT) `date +'%Y-%m-%d %H:%M'`"


if [[ "$CLUSTER_IS_INTERACTIVE" = "yes" ]]; then
    CLUSTER_TERMINATE="--no-auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--termination-protected"
else
    CLUSTER_TERMINATE="--auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--no-termination-protected"
fi

if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
    AWS_TAGS="DataWarehouseEnvironment=Production"
else
    AWS_TAGS="DataWarehouseEnvironment=Development"
fi

# === Validate bucket and environment information (sanity check on args) ===

BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    set +x
    echo "Failed to find \"$BOOTSTRAP\""
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 2
fi

# === Fill in config templates ===

BINDIR=`dirname $0`
TOPDIR=`\cd $BINDIR/.. && \pwd`
CONFIG_SOURCE="$TOPDIR/aws_config"
CONFIG_DIR="/tmp/cluster_config_${USER}$$"
if [[ -d "$CONFIG_DIR" ]]; then
    rm -f "$CONFIG_DIR"/*
else
    mkdir "$CONFIG_DIR"
fi

for JSON_IN_FILE in "$CONFIG_SOURCE/application_env.json" \
                 "$CONFIG_SOURCE/bootstrap_actions.json" \
                 "$CONFIG_SOURCE"/steps_*.json
do
    JSON_OUT_FILE="$CONFIG_DIR"/`basename $JSON_IN_FILE`
    sed -e "s,#{bucket_name},$PROJ_BUCKET,g" \
        -e "s,#{etl_environment},$PROJ_ENVIRONMENT,g" \
        "$JSON_IN_FILE" > "$JSON_OUT_FILE"
done

# ===  Start cluster ===

CLUSTER_ID_FILE="$CONFIG_DIR/cluster_id_${USER}$$.json"

aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "$CLUSTER_RELEASE_LABEL" \
        --applications "$CLUSTER_APPLICATIONS" \
        --tags "$AWS_TAGS" \
        --log-uri "$CLUSTER_LOGS" \
        --enable-debugging \
        --region "$CLUSTER_REGION" \
        --instance-groups "file://$CONFIG_SOURCE/instance_groups.json" \
        --use-default-roles \
        --configurations "file://$CONFIG_DIR/application_env.json" \
        --ec2-attributes "file://$CONFIG_SOURCE/ec2_attributes.json" \
        --bootstrap-actions "file://$CONFIG_DIR/bootstrap_actions.json" \
        $CLUSTER_TERMINATE \
        $CLUSTER_TERMINATION_PROTECTION \
        | tee "$CLUSTER_ID_FILE"
CLUSTER_ID=`jq --raw-output < "$CLUSTER_ID_FILE" '.ClusterId'`

if [[ -z "$CLUSTER_ID" ]]; then
    set +x
    echo "Failed to find cluster id in output -- cluster probably didn't start. Check your VPN etc."
    exit 1
fi

aws emr describe-cluster --cluster-id "$CLUSTER_ID" |
    jq '.Cluster.Status | {"State": .State}, .Timeline, .StateChangeReason | if has("CreationDateTime") then map_values(todate) else . end'

if [[ "$CLUSTER_IS_INTERACTIVE" = "yes" ]]; then
    sleep 10
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"

    set +x
    say "Your cluster is now running. All functions appear normal." || echo "Your cluster is now running. All functions appear normal."
else
    if [[ -r "$CONFIG_DIR/steps_$PROJ_ENVIRONMENT.json" ]]; then
        STEPS_FILE="file://$CONFIG_DIR/steps_$PROJ_ENVIRONMENT.json"
    else
        STEPS_FILE="file://$CONFIG_DIR/steps_default.json"
    fi
    aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "$STEPS_FILE"
fi

set +x
echo "If you need to proxy into the cluster, use:"
echo "aws emr socks --cluster-id \"$CLUSTER_ID\" --key-pair-file \"<location of your key file>\""
