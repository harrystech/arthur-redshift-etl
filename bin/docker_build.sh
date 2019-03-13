#!/bin/bash

set -eu

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-t tag_name]

Builds the Docker image that you can use to run Arthur locally instead of manually
configuring your development environment. Docker itself must already be installed.
Also runs the script 'bin/release_version.sh' to add version information to the build.

EOF
    exit ${1-0}
}

profile=""
tag="latest"

while getopts ":ht:" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      t)
        tag="$OPTARG"
        shift
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

set -x
bin/release_version.sh
docker build --tag "arthur:$tag" .
