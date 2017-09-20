#!/usr/bin/env bash

# Start an EMR cluster in AWS for running ETL steps.
#
# Checkout the upload_env.sh and sync_env.sh scripts to have files ready in S3 for the EMR cluster.
# Then use something like `arthur.py --submit <cluster id> extract ...`

DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e -u

# === Command line args ===

show_usage_and_exit() {
    echo "Usage: $0 [<environment>]"
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

if [[ $# -gt 1 ]]; then
    show_usage_and_exit 1
fi

set -x

# === Basic configuration ===

PROJ_BUCKET=$( arthur.py show_value object_store.s3.bucket_name )
PROJ_ENVIRONMENT=$( arthur.py show_value --prefix "${1-$DEFAULT_PREFIX}" object_store.s3.prefix )

CLUSTER_RELEASE_LABEL=$( arthur.py show_value resources.EMR.release_label )
CLUSTER_APPLICATIONS='[{"Name":"Spark"},{"Name":"Ganglia"},{"Name":"Zeppelin"},{"Name":"Sqoop"}]'
CLUSTER_REGION=$( arthur.py show_value resources.VPC.region )

# === Derived configuration ===

CLUSTER_LOGS="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/logs/"
CLUSTER_NAME="ETL Cluster ($PROJ_ENVIRONMENT, `date +'%Y-%m-%d %H:%M'`)"
if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
  ENV_NAME="production"
else
  ENV_NAME="development"
fi
AWS_TAGS="user:project=data-warehouse user:env=$ENV_NAME"

# === Validate bucket and environment information (sanity check on args) ===

BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    set +x
    echo "Failed to find \"$BOOTSTRAP\""
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 2
fi

# === Fill in config templates ===

INSTANCE_GROUPS_JSON=$( arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact instance_groups )
APPLICATION_ENV_JSON=$( arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact application_env )
EC2_ATTRIBUTES_JSON=$( arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact ec2_attributes )
BOOTSTRAP_ACTIONS_JSON=$( arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact bootstrap_actions )

# ===  Start cluster ===

CLUSTER_ID_FILE="/tmp/cluster_id_${USER}$$.json"

aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label "$CLUSTER_RELEASE_LABEL" \
        --applications "$CLUSTER_APPLICATIONS" \
        --tags $AWS_TAGS \
        --log-uri "$CLUSTER_LOGS" \
        --enable-debugging \
        --region "$CLUSTER_REGION" \
        --instance-groups "$INSTANCE_GROUPS_JSON" \
        --use-default-roles \
        --configurations "$APPLICATION_ENV_JSON" \
        --ec2-attributes "$EC2_ATTRIBUTES_JSON" \
        --bootstrap-actions "$BOOTSTRAP_ACTIONS_JSON" \
        --no-auto-terminate --termination-protected \
        | tee "$CLUSTER_ID_FILE"

CLUSTER_ID=`jq --raw-output < "$CLUSTER_ID_FILE" '.ClusterId'`

if [[ -z "$CLUSTER_ID" ]]; then
    set +x
    echo "Failed to find cluster id in output -- cluster probably didn't start. Check your VPN etc."
    exit 1
fi

aws emr describe-cluster --cluster-id "$CLUSTER_ID" |
    jq '.Cluster.Status | {"State": .State}, .Timeline, .StateChangeReason | if has("CreationDateTime") then map_values(todate) else . end'

sleep 10
aws emr wait cluster-running --cluster-id "$CLUSTER_ID"

set +x +v
say "Your cluster is now running. All functions appear normal." || echo "Your cluster is now running. All functions appear normal."

cat <<EOF
# If you want to proxy into the cluster to enjoy port forwarding, use:

  aws emr socks --cluster-id "$CLUSTER_ID" --key-pair-file "<location of your key file>"

# If you need to login into the master node, use:

  aws emr ssh --cluster-id "$CLUSTER_ID" --key-pair-file "<location of your key file>"

# If you want to submit steps, use (and remember to always pass in the prefix):

  arthur.py --submit "$CLUSTER_ID" [command] --prolix --prefix "$PROJ_ENVIRONMENT" [options ...]

# To easily reference this cluster, set the environment variable in your shell:

  export CLUSTER_ID="$CLUSTER_ID"

# * Do not forget to shutdown the cluster as soon as you no longer need it. *
EOF
