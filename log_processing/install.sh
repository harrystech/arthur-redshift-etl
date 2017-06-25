#! /bin/sh

# Create deployment package for lambda function to consume log files

if [[ $# -ne 1 ]]; then
    cat <<USAGE

Usage:
  $0 ENV_DIR

Example:
  $0 venv

Create the necessary virtual environment and install required packages.

USAGE
    exit 0
fi

set -e -x
VENV_DIR="${1%/}"
test -d $VENV_DIR || mkdir -p $VENV_DIR
python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
pip3 install --requirement ./requirements.txt --disable-pip-version-check
pip3 install -e .

echo "Done."
