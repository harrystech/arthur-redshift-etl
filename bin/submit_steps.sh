#!/usr/bin/env bash

# Submit more steps to an already running (EMR) Spark cluster.

set -e -x

CLUSTER_ID="${1?'Missing CLUSTER_ID'}"
STEPS_FILE="${2?'Missing steps file'}"

aws emr add-steps \
    --cluster-id "$CLUSTER_ID" \
    --steps "file://$STEPS_FILE"
