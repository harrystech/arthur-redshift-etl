#!/usr/bin/env bash

# Start a EMR cluster in AWS for a Spark job.
#   Non-interactive jobs will run the steps and then quit.
#   Interactive jobs will run the steps and then wait for additional work.

# TODO
# (1) Copy the jar files into {environment}/jars:
#      RedshiftJDBC41-1.1.10.1010.jar
#      commons-csv-1.4.jar
#      postgresql-9.4.1208.jar
#      spark-csv_2.10-1.4.0.jar
# (2.1) Package up the Python code (python3 setup.py sdist) and copy it into the same dir:
#      redshift-etl-0.6.2.tar.gz
# (2.2) Along with the requirements file for the ETL package:
#      requirements.txt
# (3) Copy the configuration into {environment}/config:
#      aws s3 cp --recursive ~/gits/.../config/ s3://{BUCKET_NAME}/${USER}/config/

set -e

# === Command line args ===

CLUSTER_IS_INTERACTIVE=no
while getopts ":hi" opt; do
  case $opt in
    h)
      echo "Usage: $0 [-i] <bucket_name> [<environment>]"
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

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 [-i] <bucket_name> [<environment>]"
    exit 0
fi

set -x

# === Basic cluster configuration ===

CLUSTER_BUCKET="$1"
CLUSTER_ENVIRONMENT="${2-$USER}"

SSH_KEY_PAIR_FILE="$HOME/.ssh/emr-spark-cluster.pem"

# === Derived cluster configuration ===

CLUSTER_LOGS="s3://$CLUSTER_BUCKET/$CLUSTER_ENVIRONMENT/logs/"
CLUSTER_NAME="ETL Cluster ($CLUSTER_ENVIRONMENT) `date +'%Y-%m-%d %H:%M'`"
CLUSTER_RELEASE_LABEL="emr-5.0.0"
CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"},{"Name":"Zeppelin"},{"Name":"Sqoop"}]'
CLUSTER_REGION="us-east-1"

if [ "$CLUSTER_IS_INTERACTIVE" = "yes" ]; then
    CLUSTER_TERMINATE="--no-auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--termination-protected"
else
    CLUSTER_TERMINATE="--auto-terminate"
    CLUSTER_TERMINATION_PROTECTION="--no-termination-protected"
fi

if [ "$CLUSTER_ENVIRONMENT" = "production" ]; then
    CLUSTER_TAGS="EMR_SPARK_ETL_TYPE=production"
elif [ "$CLUSTER_IS_INTERACTIVE" = "yes" ]; then
    CLUSTER_TAGS="EMR_SPARK_ETL_TYPE=interactive"
else
    CLUSTER_TAGS="EMR_SPARK_ETL_TYPE=development"
fi

# === Fill in config templates ===

# XXX Allow users to set top dir (when virtual env is not adjacent to bin, config, etc.)
BINDIR=`dirname $0`
TOPDIR=`\cd $BINDIR/.. && \pwd`
CLUSTER_CONFIG_SOURCE="$TOPDIR/aws_config"

CLUSTER_CONFIG_DIR="/tmp/cluster_config_${USER}_${CLUSTER_ENVIRONMENT}_$$"
if [[ -d "$CLUSTER_CONFIG_DIR" ]]; then
    rm -f "$CLUSTER_CONFIG_DIR"/*
else
    mkdir "$CLUSTER_CONFIG_DIR"
fi

# TODO Find a better way to parameterize cluster, check out cloud formation?

for JSON_FILE in application_env.json bootstrap_actions.json steps.json; do
    sed -e "s,#{bucket_name},$CLUSTER_BUCKET,g" \
        -e "s,#{etl_environment},$CLUSTER_ENVIRONMENT,g" \
        "$CLUSTER_CONFIG_SOURCE/$JSON_FILE" > "$CLUSTER_CONFIG_DIR/$JSON_FILE"
done

# ===  Start cluster ===

CLUSTER_ID_FILE="$CLUSTER_CONFIG_DIR/cluster_id.json"

aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "$CLUSTER_RELEASE_LABEL" \
        --applications "$CLUSTER_APPLICATIONS" \
        --tags "$CLUSTER_TAGS" \
        --log-uri "$CLUSTER_LOGS" \
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

# Wait for cluster to initialize
sleep 10

if [ "$CLUSTER_IS_INTERACTIVE" = "yes" ]; then
    aws emr wait cluster-running --cluster-id "$CLUSTER_ID"
    say "Your cluster is now running. All functions appear normal." || echo "Your cluster is now running. All functions appear normal."
    aws emr socks --cluster-id "$CLUSTER_ID" --key-pair-file "$SSH_KEY_PAIR_FILE"
else
    aws emr add-steps \
        --cluster-id "$CLUSTER_ID" \
        --steps "file://$CLUSTER_CONFIG_DIR/steps.json"
    echo "If you need to proxy into the cluster, use:"
    echo aws emr socks --cluster-id "$CLUSTER_ID" --key-pair-file "$SSH_KEY_PAIR_FILE"
fi

aws emr describe-cluster --cluster-id "$CLUSTER_ID" | \
    jq '.Cluster.Status | {"State": .State}, .Timeline, .StateChangeReason | if has("CreationDateTime") then map_values(todate) else . end'
