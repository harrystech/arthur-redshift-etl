#!/usr/bin/env bash

RELEASE_PATH="python/etl/config/release.txt"

set -eu

if [[ $# -gt 0 ]]; then
    cat <<EOF

Usage: `basename $0`

This will write current version information to '$RELEASE_PATH'.

EOF
    exit 0
fi

if ! type -a git >/dev/null 2>&1 ; then
    echo "Executable 'git' not found" >&2
    exit 1
fi

RELEASE_FILE="/tmp/upload_env_release_${USER-nobody}$$.txt"
> "$RELEASE_FILE"
trap "rm \"$RELEASE_FILE\"" EXIT

echo "toplevel=`git rev-parse --show-toplevel`" >> "$RELEASE_FILE"
GIT_COMMIT_HASH=$(git rev-parse HEAD)
if GIT_LATEST_TAG=$(git describe --exact-match --tags HEAD 2>/dev/null); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_LATEST_TAG)" >> "$RELEASE_FILE"
elif GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_BRANCH)" >> "$RELEASE_FILE"
else
    echo "commit=$GIT_COMMIT_HASH" >> "$RELEASE_FILE"
fi
echo "date=`date '+%Y-%m-%d %H:%M:%S%z'`" >> "$RELEASE_FILE"
cat "$RELEASE_FILE" | tee "python/etl/config/release.txt"
