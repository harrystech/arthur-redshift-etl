#!/bin/bash

set -o errexit

case "$0" in
    *docker_run.sh|*run_arthur.sh)
        action="run"
        action_description="drop you into a shell"
        ;;
    *docker_deploy.sh|*deploy_with_arthur.sh)
        action="deploy"
        action_description="deploy your data warehouse from a shell"
        ;;
    *docker_upload.sh|*deploy_arthur.sh)
        action="upload"
        action_description="upload your ELT code from a shell (after building the image)"
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


show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-p aws_profile] [-t image_tag] [<config_dir> [<target_env>]]

This will $action_description inside a Docker container with Arthur installed and
configured to use <config_dir>.

The <config_dir> defaults to \$DATA_WAREHOUSE_CONFIG.
The <target_env> defaults to \$ARTHUR_DEFAULT_PREFIX (or \$USER if not set).
The optional -p flag lets you use the given profile from your AWS CLI config
within the container. If \$AWS_PROFILE is set, it will be used as a default.

You must have built the Docker image with build_arthur.sh before using this script!

EOF
    exit ${1-0}
}

profile="${AWS_PROFILE-}"
tag="latest"

config_arg="$DATA_WAREHOUSE_CONFIG"
target_env="${ARTHUR_DEFAULT_PREFIX-$USER}"

# We delayed checking for unset vars until after we've tried to grab the default values.
set -o nounset

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
data_warehouse_path=`dirname "$config_abs_path"`
config_path=`basename "$config_abs_path"`

if [[ -n "$profile" ]]; then
    profile_arg="--env AWS_PROFILE=$profile"
else
    profile_arg=""
fi

# The commands below bind the following directories
#   - the "data warehouse" directory which is the parent of the chosen configuration directory
#   - the '~/.aws' directory which contains the config and credentials needed
#   - the '~/.ssh' directory which contains the keys to login into EMR and EC2 hosts (for interactive shells)
#   - the current directory as `/arthur-redshift-etl` when running a shell, to allow development
# The commands below set these environment variables
#   - DATA_WAREHOUSE_CONFIG so that Arthur finds the configuration files
#   - ARTHUR_DEFAULT_PREFIX to pick the default "environment" (same as S3 prefix)
#   - AWS_PROFILE to pick the right user or role with access to ETL admin privileges
# In case you are running interactively, this also exposes port 8086 for ETL monitoring.

case "$action" in
    run)
        set -o xtrace
        docker run --rm --interactive --tty \
            --publish 8086:8086/tcp \
            --volume "$data_warehouse_path":/data-warehouse \
            --volume `pwd`:/arthur-redshift-etl \
            --volume ~/.aws:/root/.aws \
            --volume ~/.ssh:/root/.ssh \
            --env DATA_WAREHOUSE_CONFIG="/data-warehouse/$config_path" \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            $profile_arg \
            "arthur:$tag"
        ;;
    deploy)
        set -o xtrace
        docker run --rm --tty \
            --volume "$data_warehouse_path":/data-warehouse \
            --volume ~/.aws:/root/.aws \
            --env DATA_WAREHOUSE_CONFIG="/data-warehouse/$config_path" \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            $profile_arg \
            "arthur:$tag" \
            /bin/bash -c 'source /tmp/redshift_etl/venv/bin/activate && arthur.py sync --force --deploy'
        ;;
    upload)
        set -o xtrace
        bin/release_version.sh
        docker build --tag "arthur:$tag" .
        # TODO(tom): This needs to be interactive because of the y/n-question from the upload script.
        docker run --rm --interactive --tty \
            --volume "$data_warehouse_path":/data-warehouse \
            --volume ~/.aws:/root/.aws \
            --env DATA_WAREHOUSE_CONFIG="/data-warehouse/$config_path" \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            $profile_arg \
            "arthur:$tag" \
            /bin/bash -c 'source /tmp/redshift_etl/venv/bin/activate && cd /arthur-redshift-etl && ./bin/upload_env.sh'
        ;;
    validate)
        set -o xtrace
        docker run --rm --interactive --tty \
            --volume "$data_warehouse_path":/data-warehouse \
            --volume ~/.aws:/root/.aws \
            --env DATA_WAREHOUSE_CONFIG="/data-warehouse/$config_path" \
            --env ARTHUR_DEFAULT_PREFIX="$target_env" \
            $profile_arg \
            "arthur:$tag" \
            /bin/bash -c 'source /tmp/redshift_etl/venv/bin/activate && install_validation_pipeline.sh'
        ;;
    *)
        echo "Internal Error: unknown action '$action'!" >&2
        exit 1
        ;;
esac
