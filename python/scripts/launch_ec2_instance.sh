#!/usr/bin/env bash

# Start an EC2 instance in AWS for one-off stuff.

USER="${USER-nobody}"
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -e -u

# === Command line args ===

show_usage_and_exit() {
    cat <<USAGE

Usage: $(basename $0) [<environment>]

Start a new EC2 instance tied to the given environment.
The environment defaults to "$DEFAULT_PREFIX".

USAGE
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

PROJ_BUCKET=$(arthur.py show_value object_store.s3.bucket_name)
PROJ_ENVIRONMENT=$(arthur.py show_value --prefix "${1-$DEFAULT_PREFIX}" object_store.s3.prefix)

# === Derived configuration ===

# This is more specific and allows us to see who started the instance.
INSTANCE_NAME="Arthur ETL ($PROJ_BUCKET\, $PROJ_ENVIRONMENT\, user=$DEFAULT_PREFIX\, $(date '+%s'))"

# === Validate bucket and environment information (sanity check on args) ===

BOOTSTRAP="s3://$PROJ_BUCKET/$PROJ_ENVIRONMENT/bin/bootstrap.sh"
if ! aws s3 ls "$BOOTSTRAP" >/dev/null; then
    set +x
    echo "Failed to find \"$BOOTSTRAP\""
    echo "Check whether the bucket \"$PROJ_BUCKET\" and folder \"$PROJ_ENVIRONMENT\" exist!"
    exit 2
fi

# === Fill in config templates ===

CLI_INPUT_JSON=$(arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact ec2_instance)
USER_DATA_JSON=$(arthur.py render_template --prefix "$PROJ_ENVIRONMENT" --compact cloud_init)

# ===  Start instance ===

INSTANCE_ID_FILE="/tmp/instance_id_${USER}$$.json"
trap "rm -f \"$INSTANCE_ID_FILE\"" EXIT

aws ec2 run-instances --cli-input-json "$CLI_INPUT_JSON" --user-data "$USER_DATA_JSON" |
    tee "$INSTANCE_ID_FILE" |
    jq '.Instances[] | {Monitoring: .Monitoring, InstanceId: .InstanceId, ImageId: .ImageId}'

INSTANCE_ID=$(jq --raw-output '.Instances[0].InstanceId' <"$INSTANCE_ID_FILE")

if [[ -z "$INSTANCE_ID" ]]; then
    set +x
    echo "Failed to find instance id in output -- instance probably didn't start. Check your VPN etc."
    exit 1
fi

aws ec2 wait instance-exists --instance-ids "$INSTANCE_ID"
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
aws ec2 create-tags --resources "$INSTANCE_ID" --tags "Key=Name,Value=$INSTANCE_NAME"

PUBLIC_DNS_NAME=$(
    aws ec2 describe-instances --instance-ids "$INSTANCE_ID" |
    jq --raw-output '.Reservations[0].Instances[0].PublicDnsName'
)

set +x +v

KEYPAIR=$(arthur.py show_value resources.key_name)

cat <<EOF
# Give the EC2 instance a few more seconds to bootstrap before you try logging in with:

    ssh -i ~/.ssh/$KEYPAIR.pem -l ec2-user $PUBLIC_DNS_NAME

# The output of the bootstrap script is in /var/log/cloud-init-output.log on the EC2 instance.

EOF
