#!/bin/bash
set -eu

show_usage_and_exit () {
    cat <<EOF
Usage: `basename $0` [-p aws_profile] <config_dir> <target_env>

Drops you into a shell in a Docker container with Arthur installed and
configured to point at <config_dir> and <target_env>. This script should be
run with your data warehouse directory as the current directory.

The optional -p flag lets you use the given profile from your AWS CLI config
within the container.

NOTE: You must build the Docker image with docker_build.sh before using this
script.
EOF
    exit ${1-0}
}

profile=""

while getopts "hp:" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      p)
        profile="-e AWS_PROFILE=$OPTARG"
        shift
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

if [[ $# -lt 3 ]]; then
    show_usage_and_exit 1
fi

config_dir="$1"
target_env="$2"

docker run --rm -it -v "$PWD":/data-warehouse -v ~/.aws:/root/.aws -e DATA_WAREHOUSE_CONFIG=/data-warehouse/$config_dir -e USER=$target_env $profile harrystech/arthur
