#!/bin/bash

set -o errexit -o nounset

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-t image_tag]

This builds the Docker image to run Arthur locally. Docker itself must already be installed.
The script 'bin/release_version.sh' is run to update version information for the build.

EOF
    exit ${1-0}
}

tag="latest"

while getopts ":ht:" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
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

set -o xtrace

bin/release_version.sh
docker build --tag "arthur:$tag" .
