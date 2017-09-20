#!/usr/bin/env bash

set -e -u

# Submit to a local Spark cluster.  (Submitting to an EMR cluster is done using steps.)
# This will grab all the JAR files in the local jars directory.
# N.B. Arthur will automatically use this script for extracting with Spark.

# CAVEAT If you make changes here, be sure to re-install the package to make sure changes
# propagate to the copy of this script in your path.

BIN_DIR=`dirname $0`
TOP_DIR=`\cd $BIN_DIR/../.. && \pwd`
PYTHON3="$BIN_DIR/python3"

JARS_PATH="./jars $TOP_DIR/jars"
for JARS_LOCATION in $JARS_PATH; do
    if [[ -d "$JARS_LOCATION" ]]; then
        JARS_DIR=`\cd "$JARS_LOCATION" && \pwd`
        break
    fi
done
if [[ -z "${JARS_DIR:-}" ]]; then
    echo "Directory for JAR files not found (searched: $JARS_PATH)"
    exit 1
fi
JAR_FILES=`ls -1 "$JARS_DIR"/*.jar`
JAR_LIST=`echo $JAR_FILES | sed 's: :,:g'`

if [[ ! -x "$PYTHON3" ]]; then
    echo "Cannot find Python executable: $PYTHON3"
    exit 1
fi

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 command [options]"
    exit 2
fi

COMMAND=`which $1`
if [[ "$COMMAND" = "" ]]; then
    echo "Cannot find: $1"
    exit 2
fi
shift

set -x

export PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
PYSPARK_PYTHON="$PYTHON3"
PYSPARK_DRIVER_PYTHON="$PYTHON3"

exec spark-submit \
    --jars "$JAR_LIST" \
    --conf spark.driver.maxResultSize=4G \
    --executor-memory 4G --driver-memory 4G --executor-cores 2 \
    "$COMMAND" "$@"
exit $?
