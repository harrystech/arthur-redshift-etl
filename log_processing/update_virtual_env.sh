#! /bin/sh

if [[ $# -ne 1 ]]; then
    cat <<USAGE

Usage:
  $0 ENV_DIR

Example:
  $0 venv

Create or update the virtual environment by installing required packages.

USAGE
    exit 0
fi

deactivate || echo "failed to deactivate previous virtual env (OK)"

set -e -x
VENV_DIR="${1%/}"
test -d $VENV_DIR || mkdir -p $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
pip3 install --requirement ./requirements.txt --disable-pip-version-check
pip3 install -e .

echo "Done."
