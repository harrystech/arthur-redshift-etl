#!/usr/bin/env bash

# Terminate an EMR cluster in AWS (or list all active ones to find the one to terminate).

USER="${USER-nobody}"
DEFAULT_PREFIX="${ARTHUR_DEFAULT_PREFIX-$USER}"

set -o errexit -o nounset

# === Command line args ===

show_usage_and_exit() {
    cat <<USAGE

Usage: `basename $0` [<cluster_id>]

This will terminate the cluster with the given cluster ID.
If no cluster ID is provided, all active clusters are listed.

USAGE
    exit ${1-0}
}

while getopts ":h" opt; do
  case $opt in
    h)
      show_usage_and_exit
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
  esac
done

if [[ $# -gt 1 ]]; then
    show_usage_and_exit 1
elif [[ $# -eq 0 ]]; then
    ACTIVE=$(
        set -o xtrace
        aws emr list-clusters --active |
            jq --raw-output '.Clusters?[] | [.Id, .Name, .Status.State] | @tsv'
    )
    set +o xtrace
    echo
    if [[ -z "$ACTIVE" ]]; then
        echo "No active clusters."
    else
        echo "Active clusters:"
        echo "$ACTIVE"
    fi
else
    set -o xtrace
    CLUSTERID="$1"
    # This will fail for us if the cluster ID is invalid.
    aws emr modify-cluster-attributes --no-termination-protected --cluster-id "$CLUSTERID"
    aws emr terminate-clusters --cluster-ids "$CLUSTERID"
    aws emr list-clusters --active --query "Clusters[?Id=='$CLUSTERID']" |
        jq --raw-output '.[] | [.Id, .Name, .Status.State] | @tsv'
fi
