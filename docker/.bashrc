#!/bin/bash

# This is the default .bashrc file inside a container.

# Source global definitions
if [ -f /etc/bashrc ]; then
    # shellcheck disable=SC1091
    source /etc/bashrc
fi

# Generally useful
alias ll='ls -alF'

# Useful when developing Arthur
alias develop="( \cd /opt/src/arthur-redshift-etl && python setup.py develop )"

# Change prompt to show active profile and default prefix.
PS1='(aws:$AWS_PROFILE, prefix:$ARTHUR_DEFAULT_PREFIX) \$ '

if [[ -z "$VIRTUAL_ENV" ]]; then
    # shellcheck disable=SC1091
    source /opt/local/redshift_etl/venv/bin/activate
fi

# shellcheck disable=SC1091
source /opt/src/arthur-redshift-etl/etc/arthur_completion.sh
