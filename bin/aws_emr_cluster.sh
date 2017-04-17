#!/usr/bin/env bash

# Start a EMR cluster in AWS for a Spark job.
#   Non-interactive jobs will run the steps and then quit.
#   Interactive jobs will run the steps and then wait for additional work.
#
# Checkout the setup_env.sh and sync_env.sh scripts to have files ready in S3 for the EMR cluster.
#
# TODO Find a better way to parameterize cluster, check out cloud formation?

set -e

# === Command line args ===

show_usage_and_exit() {
    set +x
    echo "Usage: $0 [-i] <bucket_name> [<environment>]"
    echo "The environment defaults to the user name ($USER)."
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

# === Basic cluster configuration ===

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="${2-$USER}"

# FIXME Remove harrys reference? Use config?
SSH_KEY_PAIR_FILE="$HOME/.ssh/harrys-dw-cluster-key.pem"

# === Derived cluster configuration ===

CLUSTER_LOGS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/logs/"
CLUSTER_NAME="ETL Cluster ($CLUSTER_ENVIRONMENT) `date +'%Y-%m-%d %H:%M'`"
CLUSTER_RELEASE_LABEL="emr-5.0.0"
CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"},{"Name":"Zeppelin"},{"Name":"Sqoop"}]'

# TODO This should come out of the user's configuration
CLUSTER_REGION="us-east-1"

if [[ "$CLUSTER_IS_INTERACTIVE" = "yes" ]]; then
    CLUSTER_TERMINATE="--no-auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--termination-protected"
else
    CLUSTER_TERMINATE="--auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--no-termination-protected"
fi

if [[ "$CLUSTER_ENVIRONMENT" =~ "production" ]]; then
    CLUSTER_TAGS="DataWarehouseEnvironment=Production"
else
    CLUSTER_TAGS="DataWarehouseEnvironment=Development"
fi

# === Validate bucket and environment information (sanity check on args) ===

BOOTSTRAP="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    set +x
    echo "Failed to find \"$BOOTSTRAP\""
    echo "Check whether the bucket \"$CLUSTER_BUCKET\" and folder \"$CLUSTER_ENVIRONMENT\" exist!"
    exit 2
fi

# === Fill in config templates ===

BINDIR=`dirname $0`
TOPDIR=`\cd $BINDIR/.. && \pwd`
CLUSTER_CONFIG_SOURCE="$TOPDIR/aws_config"

# Remove non-alphanumeric characters with a '_' to make sure we'll have a safe filename.
SAFE_ENVIRONMENT=`printf "$CLUSTER_ENVIRONMENT" | tr -s -c '[:alnum:]' '_'`

CLUSTER_CONFIG_DIR="/tmp/cluster_config_${USER}_${SAFE_ENVIRONMENT}_$$"
if [[ -d "$CLUSTER_CONFIG_DIR" ]]; then
    rm -f "$CLUSTER_CONFIG_DIR"/*
else
    mkdir "$CLUSTER_CONFIG_DIR"
fi

for JSON_IN_FILE in "$CLUSTER_CONFIG_SOURCE/application_env.json" \
                 "$CLUSTER_CONFIG_SOURCE/bootstrap_actions.json" \
                 "$CLUSTER_CONFIG_SOURCE"/steps_*.json
do
    JSON_OUT_FILE="$CLUSTER_CONFIG_DIR"/`basename $JSON_IN_FILE`
    sed -e "s,#{bucket_name},$CLUSTER_BUCKET,g" \
        -e "s,#{etl_environment},$CLUSTER_ENVIRONMENT,g" \
        "$JSON_IN_FILE" > "$JSON_OUT_FILE"
done

# ===  Start cluster ===

CLUSTER_ID_FILE="$CLUSTER_CONFIG_DIR/cluster_id.json"

aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "$CLUSTER_RELEASE_LABEL" \
        --applications "$CLUSTER_APPLICATIONS" \
        --tags "$CLUSTER_TAGS" \
        --log-uri "$CLUSTER_LOGS" \
        --enable-debugging \
        --region "$CLUSTER_REGION" \
        --instance-groups "file://$CLUSTER_CONFIG_SOURCE/instance_groups.json" \
        --use-default-roles \
        --configurations "file://$CLUSTER_CONFIG_DIR/application_env.json" \
        --ec2-attributes "file://$CLUSTER_CONFIG_SOURCE/ec2_attributes.json" \
        --bootstrap-actions "file://$CLUSTER_CONFIG_DIR/bootstrap_actions.json" \
        $CLUSTER_TERMINATE \
        $CLUSTER_TERMINATION_PROTECTION \
        | tee "$CLUSTER_ID_FILE"
CLUSTER_ID=`jq --raw-output < "$CLUSTER_ID_FILE" '.ClusterId'`

aws emr describe-cluster --cluster-id "$CLUSTER_ID" |
    jq '.Cluster.Status | {"State": .State}, .Timeline, .StateChangeReason | if has("CreationDateTime") then map_values(todate) else . end'

if [[ "$CLUSTER_IS_INTERACTIVE" = "yes" ]]; then
    sleep 10
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"

    set +x
    say "Your cluster is now running. All functions appear normal." || echo "Your cluster is now running. All functions appear normal."
else
    if [[ -r "$CLUSTER_CONFIG_DIR/steps_$CLUSTER_ENVIRONMENT.json" ]]; then
        STEPS_FILE="file://$CLUSTER_CONFIG_DIR/steps_$CLUSTER_ENVIRONMENT.json"
    else
        STEPS_FILE="file://$CLUSTER_CONFIG_DIR/steps_default.json"
    fi
    aws emr add-steps --cluster-id "$CLUSTER_ID" --steps "$STEPS_FILE"
fi

set +x
echo "If you need to proxy into the cluster, use:"
echo "aws emr socks --cluster-id \"$CLUSTER_ID\" --key-pair-file \"$SSH_KEY_PAIR_FILE\""
