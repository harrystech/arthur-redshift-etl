#!/bin/bash

set -o errexit -o nounset

tag="latest"

show_usage_and_exit () {
    cat <<EOF

Usage: `basename $0` [-t image_tag]

This builds the Docker image to run Arthur locally. Docker itself must already be installed.
The script 'bin/release_version.sh' is run to update version information for the build.
The image tag defaults to: $tag

EOF
    exit ${1-0}
}

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

BIN_DIR=$(\cd "${0%/*}"/ && \pwd)
TOP_DIR=$(\cd "$BIN_DIR/.." && \pwd)

set -o xtrace

cd "$TOP_DIR"
bin/release_version.sh
docker build --tag "arthur-redshift-etl:$tag" .
