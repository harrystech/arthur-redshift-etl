#!/usr/bin/env bash

# Start a EMR cluster in AWS for a Spark job.
#   Non-interactive jobs will run the steps and then quit.
#   Interactive jobs will run the steps and then wait for additional work.

set -e

# === Command line args ===

CLUSTER_IS_INTERACTIVE=no
while getopts ":hi" opt; do
  case $opt in
    h)
      echo "Usage: $0 [-i] <bucket_name> <environment>"
      exit 0
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

if [[ $# -lt 1 -o $# -gt 2 ]]; then
    echo "Usage: $0 [-i] <bucket_name> <environment>"
    exit 0
fi

set -x

# === Basic cluster configuration ===

CLUSTER_BUCKET="s3://$1"
CLUSTER_ENVIRONMENT="${2-$USER}"

SSH_KEY_PAIR_FILE="$HOME/.ssh/emr-spark-cluster.pem"

# === Derived cluster configuration ===

CLUSTER_LOGS="$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/logs/"
CLUSTER_NAME="ETL Cluster ($CLUSTER_ENVIRONMENT) `date +'%Y-%m-%d %H:%M'`"

if [ "$CLUSTER_IS_INTERACTIVE" = "yes" ]; then
    CLUSTER_TERMINATE="--no-auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--termination-protected"
    CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"},{"Name":"Presto-Sandbox"},{"Name":"Zeppelin-Sandbox"}]'
else
    CLUSTER_TERMINATE="--auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--no-termination-protected"
    CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"}]'
fi

if [ "$CLUSTER_ENVIRONMENT" = "production" ]; then
    CLUSTER_TAGS="EMR_SPARK_ETL_TYPE=production"
else
    CLUSTER_TAGS="EMR_SPARK_ETL_TYPE=development"
fi

# === Fill in config templates ===

CLUSTER_CONFIG_DIR="/var/tmp/cluster_config_${USER}_${CLUSTER_ENVIRONMENT}_$$"
if [[ -d "$CLUSTER_CONFIG_DIR" ]]; then
    rm -f "$CLUSTER_CONFIG_DIR"/*
else
    mkdir "$CLUSTER_CONFIG_DIR"
fi

# TODO Find a better way to parameterize cluster, check out cloud formation?

for JSON_FILE in application_env.json bootstrap_actions.json; do
    sed -e "s,#{bucket_name},$CLUSTER_BUCKET,g" -e "s,#{etl_environment},$CLUSTER_ENVIRONMENT,g" "$JSON_FILE" > "$CLUSTER_CONFIG_DIR/$JSON_FILE"
done

# ===  Start cluster ===

CLUSTER_ID_FILE="$CLUSTER_CONFIG_DIR/cluster_id.json"

aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "emr-4.6.0" \
        --applications "$CLUSTER_APPLICATIONS" \
        --tags "$CLUSTER_TAGS" \
        --log-uri "$CLUSTER_LOGS" \
        --region "us-east-1" \
        --instance-groups "file://./instance_groups.json" \
        --use-default-roles \
        --configurations "file://$CLUSTER_CONFIG_DIR/application_env.json" \
        --ec2-attributes "file://./ec2_attributes.json" \
        --bootstrap-actions "file://$CLUSTER_CONFIG_DIR/bootstrap_actions.json" \
        $CLUSTER_TERMINATE \
        $CLUSTER_TERMINATION_PROTECTION \
        | tee "$CLUSTER_ID_FILE"
CLUSTER_ID=`jq --raw-output < "$CLUSTER_ID_FILE" '.ClusterId'`

if [ "$CLUSTER_IS_INTERACTIVE" = "yes" ]; then
    sleep 10
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"
    say "Your cluster is now running. All functions appear normal." || echo "Your cluster is now running. All functions appear normal."
    aws emr socks --cluster-id "$CLUSTER_ID" --key-pair-file "$SSH_KEY_PAIR_FILE"
else
    echo "If you need to proxy into the cluster, use:"
    echo aws emr socks --cluster-id "$CLUSTER_ID" --key-pair-file "$SSH_KEY_PAIR_FILE"
fi

aws emr describe-cluster --cluster-id "$CLUSTER_ID" | \
    jq '.Cluster.Status | {"State": .State}, .Timeline, .StateChangeReason | if has("CreationDateTime") then map_values(todate) else . end'
