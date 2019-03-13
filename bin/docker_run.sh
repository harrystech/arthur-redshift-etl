#!/bin/bash

set -e

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-p aws_profile] [-t tag] [<config_dir> [<target_env>]]

This will drop you into a shell in a Docker container with Arthur installed and
configured to use <config_dir>.

The <config_dir> default to \$DATA_WAREHOUSE_CONFIG.
The <target_env> defaults to \$ARTHUR_DEFAULT_PREFIX (or \$USER if not set).
The optional -p flag lets you use the given profile from your AWS CLI config
within the container. If \$AWS_PROFILE is set, it will be used as a default.

You must build the Docker image with docker_build.sh before using this script.

EOF
    exit ${1-0}
}

profile="${AWS_PROFILE-}"
tag="latest"

while getopts ":hp:t:" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      p)
        profile="$OPTARG"
        ;;
      t)
        tag="$OPTARG"
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        show_usage_and_exit 1
      ;;
    esac
done
shift $((OPTIND -1))

if [[ $# -gt 2 ]]; then
    echo "Wrong number of arguments!" >&2
    show_usage_and_exit 1
fi

if [[ $# -eq 2 ]]; then
    config_arg="$1"
    target_env="$2"
elif [[ $# -eq 1 ]]; then
    config_arg="$1"
    target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"
else
    if [[ -z "$DATA_WAREHOUSE_CONFIG" ]]; then
        echo "You must set DATA_WAREHOUSE_CONFIG when not specifying the config directory." >&2
        show_usage_and_exit 1
    fi
    config_arg="$DATA_WAREHOUSE_CONFIG"
    target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"
fi

set -u

if [[ ! -d "$config_arg" ]]; then
    echo "Bad configuration directory: $config_arg"
    exit 1
fi
config_abs_path=$(\cd "$config_arg" && \pwd)
data_warehouse_path=`dirname "$config_abs_path"`
config_path=`basename "$config_abs_path"`

if [[ -n "$profile" ]]; then
    profile_arg="-e AWS_PROFILE=$profile"
else
    profile_arg=""
fi

set -x
docker run --rm -it \
    --volume "$data_warehouse_path":/data-warehouse --volume ~/.aws:/root/.aws \
    -e DATA_WAREHOUSE_CONFIG="/data-warehouse/$config_path" \
    -e ARTHUR_DEFAULT_PREFIX="$target_env" \
    $profile_arg "arthur:$tag"
