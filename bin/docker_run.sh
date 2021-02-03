#!/bin/bash

set -o errexit

case "$0" in
    *docker_run.sh|*run_arthur.sh)
        action="run"
        action_description="drop you into a shell"
        ;;
    *docker_deploy.sh|*deploy_with_arthur.sh)
        action="deploy"
        action_description="deploy your data warehouse into S3"
        ;;
    *docker_upload.sh|*deploy_arthur.sh)
        action="upload"
        action_description="build the Docker image and upload your ELT code to S3"
        ;;
    *run_validation.sh)
        action="validate"
        action_description="install a validation pipeline to run immediately"
        ;;
    *)
        echo "Internal Error: unknown script name!" >&2
        exit 1
        ;;
esac

BUILD_SCRIPT="${0%/*}/build_arthur.sh"

show_usage_and_exit () {
    cat <<EOF

Usage: $(basename "$0") [-p aws_profile] [-t image_tag] [-w] [<config_dir> [<target_env>]]

This will $action_description inside a Docker container
with Arthur installed and configured to use <config_dir>.

The <config_dir> defaults to \$DATA_WAREHOUSE_CONFIG.
The <target_env> defaults to \$ARTHUR_DEFAULT_PREFIX (or \$USER if not set).

The optional -p flag lets you use the given profile from your AWS CLI config
within the container. When the flag is not present, \$AWS_PROFILE is used if set
or \$AWS_DEFAULT_PROFILE is used if set and \$AWS_PROFILE isn't.

With the -w flag, port 8086 is published to access the HTTP server in the ETL.

EOF
    exit "${1-0}"
}

profile="${AWS_PROFILE-${AWS_DEFAULT_PROFILE-}}"
tag="latest"
publish_arg=""

config_arg="$DATA_WAREHOUSE_CONFIG"
target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"

# We delayed checking for unset vars until after we've tried to grab the default values.
set -o nounset

while getopts ":hp:t:w" opt; do
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
      w)
        publish_arg="--publish 8086:8086/tcp"
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
elif [[ $# -eq 2 ]]; then
    # Override both, config directory and target prefix.
    config_arg="$1"
    target_env="$2"
elif [[ $# -eq 1 ]]; then
    # Just override target prefix.
    config_arg="$1"
elif [[ -z "$config_arg" ]]; then
    echo "You must set DATA_WAREHOUSE_CONFIG when not specifying the config directory." >&2
    show_usage_and_exit 1
fi

if [[ ! -d "$config_arg" ]]; then
    echo "Bad configuration directory: $config_arg"
    exit 1
fi
config_abs_path=$(\cd "$config_arg" && \pwd)
data_warehouse_path=$(dirname "$config_abs_path")
config_path=$(basename "$config_abs_path")

if [[ -n "$profile" ]]; then
    profile_arg="--env AWS_PROFILE=$profile"
else
    profile_arg=""
fi

# The commands below bind the following directories
#   - the "data warehouse" directory as /opt/data-warehouse, which is the parent of the chosen
#     configuration directory (always read-write when we need to write an arthur.log file)
#   - the '~/.aws' directory which contains the config and credentials needed (always read-write
#     when we need to write to the cli cache)
#   - the '~/.ssh' directory which contains the keys to login into EMR and EC2 hosts (for interactive shells)
#   - the current directory as `/opt/src/arthur-redshift-etl` when running a shell, to allow development
# The commands below set these environment variables
#   - DATA_WAREHOUSE_CONFIG so that Arthur finds the configuration files
#   - ARTHUR_DEFAULT_PREFIX to pick the default "environment" (same as S3 prefix)
#   - AWS_PROFILE to pick the right user or role with access to ETL admin privileges
# In case you are running interactively, this also exposes port 8086 for ETL monitoring.

if ! grep 'name="redshift_etl"' setup.py >/dev/null 2>&1; then
    # This only applies to "run" since the other actions do not mount the source directory.
    if [[ "$action" = "run" ]]; then
        action="run-ro"
        echo "Did not find source path (looked for setup.py) -- switching to standalone mode."
        echo "Changes to code in /opt/src/arthur-redshift-etl will not be preserved between runs."
        echo "However, changes to your schemas or config will be reflected in your local filesystem."
    fi
fi

# We always build a new image for deploying so that we deploy the latest version with the latest version.
# Otherwise, we only build if we cannot find the image locally.
# NOTA BENE We do not try to ascertain whether the image is "out of date."
if [[ "$action" = "deploy" ]]; then
    BUILD_IMAGE=yes
elif ! docker image inspect "arthur-redshift-etl:$tag" >/dev/null 2>&1; then
    echo "Docker image not found ... building on demand."
    BUILD_IMAGE=yes
else
    BUILD_IMAGE=no
fi
if [[ "$BUILD_IMAGE" = "yes" ]]; then
    "$BUILD_SCRIPT" -t "$tag"
fi

case "$action" in
    deploy)
        set -o xtrace
        # shellcheck disable=SC2086
        docker run --rm --tty \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
            --volume ~/.aws:/home/arthur/.aws \
            --volume "$data_warehouse_path":/opt/data-warehouse \
            $profile_arg \
            "arthur-redshift-etl:$tag" \
            arthur.py sync --force --deploy
        ;;
    run)
        set -o xtrace
        # shellcheck disable=SC2086
        docker run --rm --interactive --tty \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
            --sysctl net.ipv4.tcp_keepalive_time=300 \
            --sysctl net.ipv4.tcp_keepalive_intvl=60 \
            --sysctl net.ipv4.tcp_keepalive_probes=9 \
            --volume ~/.aws:/home/arthur/.aws \
            --volume ~/.ssh:/home/arthur/.ssh:ro \
            --volume "$data_warehouse_path:/opt/data-warehouse" \
            --volume "$(pwd):/opt/src/arthur-redshift-etl" \
            $publish_arg \
            $profile_arg \
            "arthur-redshift-etl:$tag"
        ;;
    run-ro)
        set -o xtrace
        # shellcheck disable=SC2086
        docker run --rm --interactive --tty \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
            --sysctl net.ipv4.tcp_keepalive_time=300 \
            --sysctl net.ipv4.tcp_keepalive_intvl=60 \
            --sysctl net.ipv4.tcp_keepalive_probes=9 \
            --volume ~/.aws:/home/arthur/.aws \
            --volume ~/.ssh:/home/arthur/.ssh:ro \
            --volume "$data_warehouse_path:/opt/data-warehouse" \
            $publish_arg \
            $profile_arg \
            "arthur-redshift-etl:$tag"
        ;;
    upload)
        set -o xtrace
        # shellcheck disable=SC2086
        docker run --rm --tty \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
            --volume ~/.aws:/home/arthur/.aws \
            --volume "$data_warehouse_path:/opt/data-warehouse" \
            $profile_arg \
            "arthur-redshift-etl:$tag" \
            upload_env.sh -y
        ;;
    validate)
        set -o xtrace
        # shellcheck disable=SC2086
        docker run --rm --tty \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            --env DATA_WAREHOUSE_CONFIG="/opt/data-warehouse/$config_path" \
            --volume ~/.aws:/home/arthur/.aws \
            --volume "$data_warehouse_path":/opt/data-warehouse \
            $profile_arg \
            "arthur-redshift-etl:$tag" \
            install_validation_pipeline.sh
        ;;
    *)
        echo "Internal Error: unknown action '$action'!" >&2
        exit 1
        ;;
esac
