#!/usr/bin/env bash

RELEASE_FILE="python/etl/config/release.txt"
TEMP_RELEASE_FILE="/tmp/release_version_${USER-nobody}$$.txt"

set -o errexit -o nounset

if [[ $# -gt 0 ]]; then
    cat <<EOF

Usage: $(basename "$0")

This will update current version information in '$RELEASE_FILE'.

EOF
    exit 0
fi

if ! type -a git >/dev/null 2>&1 ; then
    echo "Executable 'git' not found" >&2
    exit 1
fi

# Create an empty file to add information to (and make sure that it gets cleaned up).
true > "$TEMP_RELEASE_FILE"
# shellcheck disable=SC2064
trap "rm \"$TEMP_RELEASE_FILE\"" EXIT

echo "toplevel=$(git rev-parse --show-toplevel)" >> "$TEMP_RELEASE_FILE"

GIT_COMMIT_HASH=$(git rev-parse HEAD)
if GIT_LATEST_TAG=$(git describe --exact-match --tags HEAD 2>/dev/null); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_LATEST_TAG)" >> "$TEMP_RELEASE_FILE"
elif GIT_BRANCH=$(git symbolic-ref --short --quiet HEAD); then
    echo "commit=$GIT_COMMIT_HASH ($GIT_BRANCH)" >> "$TEMP_RELEASE_FILE"
else
    echo "commit=$GIT_COMMIT_HASH" >> "$TEMP_RELEASE_FILE"
fi

# Pick up the date from the latest commit.
echo "date=$(git log -1 --format='%ai' HEAD)" >> "$TEMP_RELEASE_FILE"

# We add the latest commit hash to the release file which is misleading if we're pulling in modified files.
if git status --porcelain 2>/dev/null | grep -E '^ M|^M' >/dev/null; then
    echo "WARNING Not all of your changes have been committed!" >&2
    echo >&2
    echo "warning=locally modified files exist" >> "$TEMP_RELEASE_FILE"
fi

if cmp "$TEMP_RELEASE_FILE" "$RELEASE_FILE" >/dev/null 2>&1; then
    echo "Release information is unchanged." >&2
else
    echo "Updating release information in $RELEASE_FILE" >&2
    cp "$TEMP_RELEASE_FILE" "$RELEASE_FILE"
fi
cat "$TEMP_RELEASE_FILE"
