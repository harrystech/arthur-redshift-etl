#!/bin/bash

# This version of the start script leverages the Docker image from the
# GitHub repo, which gets published when we merge code into our main
# branch. It doesn't expect for the Arthur codebase to be locally
# installed and so is nicer for end-users of Arthur.
#
# This can be added to your data warehouse repo or retrieved ad-hoc using:
#   curl -o bin/arthur.sh https://raw.githubusercontent.com/harrystech/arthur-redshift-etl/master/bin/arthur.sh
#   chmod +x bin/arthur.sh

if ! type docker >/dev/null ; then
  echo "You need to install a Docker environment first." 1>&2
  exit 1
fi

# Make sure Docker version is recent enough ("--pull always" was added in version 20)
if [[ $(docker version --format '{{.Server.Version}}') =~ ^1[0-9].* ]]; then
  echo "You need to upgrade your Docker environment to version >= 20." 1>&2
  exit 1
fi

set -o errexit

aws_profile="${AWS_PROFILE-${AWS_DEFAULT_PROFILE-default}}"
config_dir="$DATA_WAREHOUSE_CONFIG"
docker_image="ghcr.io/harrystech/arthur-redshift-etl/arthur-etl"
tag="latest"
target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"
verbose=0

# We delayed checking for unset vars until after we've tried to grab the default values.
set -o nounset

show_usage_and_exit () {
  cat <<EOF

Usage:
  $(basename "$0") [-p aws_profile] [<config_dir> [<target_env>]]
  $(basename "$0") [-v] [-p aws_profile] [-c <config_dir>] [-e <target_env>]

This will start a Docker container with Arthur installed for you, pulling the latest
Docker image for you if necessary. The following settings will be used:

  ARTHUR_DEFAULT_PREFIX="$target_env"
  AWS_PROFILE="$aws_profile"
  DATA_WAREHOUSE_CONFIG="$config_dir"

If any of these settings are not set (""), then you should add your default values
into your shell start-up script (likely either .bashrc or .zshrc).

Default values are shown above and can be overridden as:
* Use <config_dir> to override \$DATA_WAREHOUSE_CONFIG.
* Use <target_env> to override \$ARTHUR_DEFAULT_PREFIX.
* Use <aws_profile> to override \$AWS_PROFILE.

Advanced option: [-t tag]
  If you would like to run a version other than the latest, you can
  pass in the version after "-t". (The default is "$tag".)

EOF
  exit "${1-0}"
}

while getopts ":hc:e:p:t:v" opt; do
  case "$opt" in
    h)
      show_usage_and_exit
      ;;
    c)
      config_dir="$OPTARG"
      ;;
    e)
      target_env="$OPTARG"
      ;;
    p)
      aws_profile="$OPTARG"
      ;;
    t)
      tag="$OPTARG"
      ;;
    v)
      verbose=1
      ;;
    \?)
      echo "Invalid option: -$OPTARG" 1>&2
      show_usage_and_exit 1
      ;;
  esac
done
shift $((OPTIND -1))

if [[ $# -gt 2 ]]; then
    echo "Wrong number of arguments!" 1>&2
    show_usage_and_exit 1
elif [[ $# -eq 2 ]]; then
    # Override both, config directory and target prefix.
    config_dir="$1"
    target_env="$2"
elif [[ $# -eq 1 ]]; then
    # Just override target prefix.
    config_dir="$1"
elif [[ -z "$config_dir" ]]; then
    echo "You must set DATA_WAREHOUSE_CONFIG when not specifying the config directory." 1>&2
    show_usage_and_exit 1
fi

if [[ ! -d "$config_dir" ]]; then
    echo "Cannot stat configuration directory: $config_dir" 1>&2
    exit 1
fi
config_abs_path=$(\cd "$config_dir" && \pwd)
data_warehouse_path=$(dirname "$config_abs_path")
config_path=$(basename "$config_abs_path")

# The command below binds the following directories
#   - the "data warehouse" directory as /opt/data-warehouse, which is the parent of the chosen
#     configuration directory (always read-write when we need to write an arthur.log file)
#   - the '~/.aws' directory which contains the config and credentials needed (always read-write
#     when we need to write to the cli cache)
#   - the '~/.ssh' directory which contains the keys to login into EMR and EC2 hosts (for interactive shells)
# The command below sets these environment variables
#   - ARTHUR_DEFAULT_PREFIX to pick the default "environment" (same as S3 prefix)
#   - AWS_PROFILE to pick the right user or role with access to ETL admin privileges
#   - DATA_WAREHOUSE_CONFIG so that Arthur finds the configuration files

if [[ "$verbose" != "0" ]]; then
    set -o xtrace
fi
# shellcheck disable=SC2086
docker run --rm --interactive --tty \
    --env ARTHUR_DEFAULT_PREFIX="$target_env" \
    --env AWS_PROFILE="$aws_profile" \
    --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
    --env DBT_ROOT="$data_warehouse_path/dbt" \
    --env DBT_PROFILES_DIR="${HOME}/.dbt/profiles.yml" \
    --pull always \
    --sysctl net.ipv4.tcp_keepalive_time=300 \
    --sysctl net.ipv4.tcp_keepalive_intvl=60 \
    --sysctl net.ipv4.tcp_keepalive_probes=9 \
    --volume ~/.aws:/home/arthur/.aws \
    --volume ~/.ssh:/home/arthur/.ssh:ro \
    --volume "$data_warehouse_path:/opt/data-warehouse" \
    --volume /var/run/docker.sock:/var/run/docker.sock \
    "$docker_image:$tag"
