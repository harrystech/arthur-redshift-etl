#! /bin/bash

# This script sends a "ping" by hitting the Cronut app with a POST request.
# See also https://github.com/harrystech/cronut for our dead-man switch service.

# This script expects these environment variables to be set to connect to Cronut:
#   CRONUT_BASE_URL - location of the CRONUT service
#   CRONUT_API_TOKEN - identify this project
#   CRONUT_PUBLIC_KEY - encrypt the public id in transit
#
# To identify the desired schedule (using its public ID), the environment variables
# must contain one that looks like "CRONUT_<command line arg>".
# For example,  "ping_cronut.sh WAKEUP" will look for the public ID in CRONUT_WAKEUP.
#
# Note that the "name" here simply refers to the command line argument and
# need not be related to the name chosen in the Cronut service.

if [[ $# -lt 1 || "$1" = "-h" ]]; then
    echo "Usage: $0 <Name of Cronut Public ID in env file>"
    exit 0
fi

set -e

for CREDENTIALS in credentials.sh cronut_env.sh cronut_public_ids.sh; do
    if [ -r "/tmp/redshift_etl/config/$CREDENTIALS" ]; then
        source "/tmp/redshift_etl/config/$CREDENTIALS"
    fi
done

NAME="CRONUT_$1"
CRONUT_PUBLIC_ID=${!NAME}

if [ -z $CRONUT_PUBLIC_ID ]; then
    echo "Could not find value for \"$1\" (checked \$$NAME)"
    exit 1
fi

CURRENT_TIME=`date '+%s'`
echo "Sending POST request to Cronut for \"$1\" with public_id=$CRONUT_PUBLIC_ID (timestamp=$CURRENT_TIME)"

set -u

CRONUT_PUBLIC_KEY_FILE="/tmp/cronut_pub_$$"
echo -n "$CRONUT_PUBLIC_KEY" > "$CRONUT_PUBLIC_KEY_FILE"
trap "rm \"$CRONUT_PUBLIC_KEY_FILE\"" EXIT

# We need to encrypt and url-encode the public_id parameter for the ping:
echo -n "$CURRENT_TIME-$CRONUT_PUBLIC_ID" |
openssl rsautl -encrypt -pubin -inkey "$CRONUT_PUBLIC_KEY_FILE" |
curl --silent \
    --data-urlencode "public_id@-" \
    --header "X-CRONUT-API-TOKEN: $CRONUT_API_TOKEN" \
    "$CRONUT_BASE_URL/ping/" | grep PONG
exit $?
