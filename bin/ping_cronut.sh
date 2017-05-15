#! /bin/bash

# This script sends a "ping" by hitting the Cronut app with a POST request.
# See also https://github.com/harrystech/cronut for our dead-man switch service.

# This script expects these environment variables to be set to connect to Cronut:
#   CRONUT_BASE_URL - location of the CRONUT service
#   CRONUT_API_TOKEN - identify this project
#   CRONUT_PUBLIC_KEY - encrypt the public id in transit
# We usually keep this information in the credentials.sh file.
#
# To identify the desired schedule (using its public ID), the environment variables
# must contain one that looks like "CRONUT_<command line arg>".
# For example,  "ping_cronut.sh WAKEUP" will look for the public ID in CRONUT_WAKEUP.
# We keep this information in the cronut_public_ids.sh file.
#
# If the public ID cannot be found, this script will not create an error. (Since the
# dead-man switch itself will detect the missing ping.)
#
# Note that the "name" here simply refers to the command line argument and
# need not be related to the name chosen in the Cronut service.

if [[ $# -lt 1 || "$1" = "-h" ]]; then
    echo "Usage: $0 <Name of Cronut Public ID in env file>"
    exit 0
fi

set -e

for CREDENTIALS in credentials.sh cronut_public_ids.sh; do
    FILENAME="/tmp/redshift_etl/config/$CREDENTIALS"
    if [ -r "$FILENAME" ]; then
        echo "Reading '$FILENAME'"
        source "$FILENAME"
    else
        echo "Failed to find '$FILENAME'"
    fi
done

NAME="CRONUT_$1"
CRONUT_PUBLIC_ID=${!NAME}

for ENV_NAME in CRONUT_BASE_URL CRONUT_API_TOKEN CRONUT_PUBLIC_KEY; do
    ENV_VALUE=${!ENV_NAME}
    if [ -z "$ENV_VALUE" ]; then
        echo "Failed to find value for $ENV_NAME"
        exit 0
    fi
done
if [ -z "$CRONUT_PUBLIC_ID" ]; then
    echo "Could not find public id for '$1' (checked variable \$$NAME)"
    exit 0
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
