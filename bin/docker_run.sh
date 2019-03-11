#!/bin/bash
set -eu

show_usage_and_exit () {
    cat <<EOF
Usage: `basename $0` [-p aws_profile] <config_dir> [<target_env>]

This will drop you into a shell in a Docker container with Arthur installed and
configured to use <config_dir>.
The <target_env> defaults to \$ARTHUR_DEFAULT_PREFIX (or \$USER if not set).
The optional -p flag lets you use the given profile from your AWS CLI config
within the container. If \$AWS_PROFILE is set, it will be used as a default.

This script should be run with your data warehouse directory as the current directory.

(You must build the Docker image with docker_build.sh before using this script.)

EOF
    exit ${1-0}
}

profile="${AWS_PROFILE-}"
target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"

while getopts ":hp:" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      p)
        profile="$OPTARG"
        shift
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Wrong number of arguments!" >&2
    echo >&2
    show_usage_and_exit 1
fi

config_dir="$1"
if [[ ! -d "$config_dir" ]]; then
    echo "Bad configuration directory: $config_dir"
    exit 1
fi

if [[ $# -eq 2 ]]; then
  target_env="$2"
fi

if [[ -n "$profile" ]]; then
    profile_arg="-e AWS_PROFILE=$profile"
else
    profile_arg=""
fi

set -x
docker run --rm -it -v "$PWD":/data-warehouse -v ~/.aws:/root/.aws \
    -e DATA_WAREHOUSE_CONFIG=/data-warehouse/$config_dir \
    -e "ARTHUR_DEFAULT_PREFIX=$target_env" -e "USER=$target_env" \
    $profile_arg harrystech/arthur
