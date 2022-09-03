#! /bin/bash

# Entrypoint script for Docker image to adjust for mounting the source directory.
# 1. If we are using the Python code that was copied into the image, then there's
#    nothing else to do. The package information is correct (and static).
# 2. If we are mounting the local source into the image, then we may either find
#    no package information or that the package information is out of date.
#    In this case, we need to make sure that the virtual environment can find
#    the up-to-date package information.
# 3. Finally it's easy to forget to update package information when changing
#    one of the scripts but then their old version will continue to be used.
#
# Bottom line: We should always run "python setup.py develop" when starting up.
# Side-effect: You will find a python/redshift_etl.egg-info directory locally.
#              (This also means that we cannot mount the source read-only.)

set -o errexit -o nounset

export PATH="/opt/local/redshift_etl/bin:$PATH"

if [[ -r "/opt/local/redshift_etl/venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source /opt/local/redshift_etl/venv/bin/activate

    # Using "--quiet" here to reduce the startup noise for "end users."
    if [[ -d "/opt/src/arthur-redshift-etl/.git" ]]; then
        (
          set -o xtrace
          cd /opt/src/arthur-redshift-etl
          python3 setup.py --quiet develop || echo "Warning: Failed to update image to latest source version" 2>&1
        )
    fi
fi

echo arthur | sudo -S chmod 777 /var/run/docker.sock

exec "$@"
