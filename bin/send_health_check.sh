#! /bin/bash
#
# Facilitates monitoring scheduled Arthur jobs by sending an HTTP GET request
# to a specified URI.
#
# This script is called near the beginning and end of Arthur data pipelines.
# The service that receives the GET request is expected to sound an alarm if
# it does not receive the second request within some configured amount of
# time.
#
# The script expects this environment variable to be set:
#   HEALTH_CHECK_URI - URI to the job-specific endpoint for monitoring
#
# The environment variable is usually set in the environment.sh file. It may
# also be simply exported from the current shell, as an alternative.
#
# The HTTP endpoint is expected to handle requests to these three resources:
#   HEALTH_CHECK_URI - The bare URI itself, when the job completes
#   HEALTH_CHECK_URI/start - To indicate the job has started
#   HEALTH_CHECK_URI/fail - To explicitly indicate job failure

if [[ "$1" == "-h" || ("$1" != "" && "$1" != "start" && "$1" != "fail") ]]; then
    echo "Usage: $0 [ start | fail ]"
    exit 1
fi

set -o errexit

FILENAME="/tmp/redshift_etl/config/environment.sh"
echo "Attempting to read configuration environment variables from '$FILENAME'"
if [ -r "$FILENAME" ]; then
    source "$FILENAME"
else
    echo "Failed to find '$FILENAME'"
fi
if [ -z "$HEALTH_CHECK_URI" ]; then
    echo "Failed to find value in environment variable HEALTH_CHECK_URI, health checks disabled"
    exit 0
fi

# Status will be only one of: "", "start", or "fail"
status="$1"
if [ -n "$status" ]; then
    HEALTH_CHECK_URI="$HEALTH_CHECK_URI/$status"
fi

set -o xtrace
set -o nounset

curl --connect-timeout 5 --max-time 60 --retry 5 "$HEALTH_CHECK_URI"
exit $?
