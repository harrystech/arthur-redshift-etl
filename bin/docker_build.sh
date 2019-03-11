#!/bin/bash
set -eu

show_usage_and_exit () {
    cat <<EOF
Usage: `basename $0`

Builds the Docker image that you can use to run Arthur locally instead of manually
configuring your development environment. Docker itself must already be installed.
EOF
    exit ${1-0}
}

profile=""

while getopts ":h" opt; do
    case "$opt" in
      h)
        show_usage_and_exit
        ;;
      \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
      ;;
    esac
done

set -x
docker build -t harrystech/arthur .
