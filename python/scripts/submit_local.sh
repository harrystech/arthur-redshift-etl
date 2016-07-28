#!/usr/bin/env bash

set -e

# Submit to a local Spark cluster.  (Submitting to an EMR cluster is done using steps.)
# This will grab all the JAR files in the .jars directory.

# CAVEAT If you make changes here, be sure to re-install the package to make sure changes
# propagate to the copy of this script in your path.

# XXX This assumes that the script in venv/bin is called
BIN_DIR=`dirname $0`
TOP_DIR=`\cd $BIN_DIR/../.. && \pwd`
JARS_DIR="$TOP_DIR/jars"
PYTHON3=`which python3`

if [[ ! -d "$JARS_DIR" ]]; then
    echo "Directory does not exist: $JARS_DIR"
    exit 1
fi

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


# XXX Switch to "local:" files
JAR_FILES=`ls -1 "$JARS_DIR" | grep 'jar$' | sed -e "s:^:$JARS_DIR/:"`
JARS_ARG=`echo $JAR_FILES | sed 's: :,:g'`

set -x

export PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
PYSPARK_PYTHON="$PYTHON3"
PYSPARK_DRIVER_PYTHON="$PYTHON3"

exec spark-submit --jars "$JARS_ARG" \
    --executor-memory 4G --driver-memory 4G --executor-cores 2 \
    --conf spark.driver.maxResultSize=4G \
    "$COMMAND" "$@"
