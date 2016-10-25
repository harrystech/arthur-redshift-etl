#! /bin/bash -e

# This script expects these environment variables:
#   CRONUT_API_TOKEN - identify this project
#   CRONUT_PUBLIC_ID - identify this particular schedule
#   CRONUT_PUBLIC_KEY - encrypt the public id in transit
#   CRONUT_BASE_URL - location of the CRONUT service
# See also https://github.com/harrystech/cronut

source /tmp/redshift_etl/config/cronut_env.sh

CRONUT_PUBLIC_KEY_FILE="/tmp/cronut_pub_$$"
trap "rm \"$CRONUT_PUBLIC_KEY_FILE\"" EXIT
echo -n "$CRONUT_PUBLIC_KEY" > "$CRONUT_PUBLIC_KEY_FILE"

CURRENT_TIME=`date '+%s'`
echo -n "$CURRENT_TIME-$CRONUT_PUBLIC_ID" |
openssl rsautl -encrypt -pubin -inkey "$CRONUT_PUBLIC_KEY_FILE" |
curl --silent \
    --data-urlencode "public_id@-" \
    --header "X-CRONUT-API-TOKEN: $CRONUT_API_TOKEN" \
    "$CRONUT_BASE_URL/ping/" | grep PONG
exit $?
