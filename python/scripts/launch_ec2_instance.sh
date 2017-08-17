#!/usr/bin/env bash

# Start an EC2 instance in AWS for one-off stuff.

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

# === Derived configuration ===

INSTANCE_NAME="Arthur (env=$PROJ_ENVIRONMENT\, user=$USER\, `date '+%s'`)"
if [[ "$PROJ_ENVIRONMENT" =~ "production" ]]; then
  ENV_NAME="production"
else
  ENV_NAME="development"
fi
AWS_TAGS="Key=user:project,Value=data-warehouse Key=user:env,Value=$ENV_NAME"

# === Validate bucket and environment information (sanity check on args) ===

BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" > /dev/null; then
    set +x
    echo "Failed to find \"$BOOTSTRAP\""
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 2
fi

# === Fill in config templates ===

CLI_INPUT_JSON=$( arthur.py render_template --prefix "${2-$DEFAULT_PREFIX}" --compact ec2_instance )
USER_DATA_JSON=$( arthur.py render_template --prefix "${2-$DEFAULT_PREFIX}" --compact cloud_init )

# ===  Start instance ===

INSTANCE_ID_FILE="/tmp/instance_id_${USER}$$.json"

aws ec2 run-instances --cli-input-json "$CLI_INPUT_JSON" --user-data "$USER_DATA_JSON" | tee "$INSTANCE_ID_FILE"

INSTANCE_ID=`jq --raw-output < "$INSTANCE_ID_FILE" '.Instances[0].InstanceId'`

if [[ -z "$INSTANCE_ID" ]]; then
    set +x
    echo "Failed to find instance id in output -- instance probably didn't start. Check your VPN etc."
    exit 1
fi

aws ec2 wait instance-exists --instance-ids "$INSTANCE_ID"
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
aws ec2 create-tags --resources "$INSTANCE_ID" --tags "Key=Name,Value=$INSTANCE_NAME" "$AWS_TAGS"

PUBLIC_DNS_NAME=`aws ec2 describe-instances --instance-ids "$INSTANCE_ID" |
    jq --raw-output '.Reservations[0].Instances[0].PublicDnsName'`

set +x +v
cat <<EOF
# Give the machine a few more seconds to bootstrap before you try logging in with:

    ssh -i '<location of your key file>' -l ec2-user $PUBLIC_DNS_NAME

# Or if you setup 'User' and 'IdentityFile' for 'Host ec2-*.amazonaws.com' in your ~/.ssh/config:

    ssh $PUBLIC_DNS_NAME

EOF
