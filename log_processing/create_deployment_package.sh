#! /bin/sh

set -e

if [[ $# -ne 1 ]]; then
    cat <<USAGE

Usage:
  $0 ENV_DIR

Example:
  $0 venv

Create the ZIP file to upload for Lambda execution.

USAGE
    exit 0
fi

VENV_NAME="${1%%/}"
if [[ ! -d "$VENV_NAME" || ! -f "$VENV_NAME/bin/activate" ]]; then
    echo "Not found (or not valid as virtual environment): $VENV_NAME"
    exit 1
fi

set +e +x
deactivate || echo "failed to deactivate previous virtual env (OK)"
echo source "$VENV_NAME/bin/activate"
source "$VENV_NAME/bin/activate"

set -e -x
ZIP_FILE="$PWD/log_processing_`date +%Y%m%d%H%M`.zip"
test -f "$ZIP_FILE" && \rm "$ZIP_FILE"

(
  cd $VIRTUAL_ENV/lib/python3.*/site-packages || exit 2
  find . -name '__pycache__' -prune \
      -o -name 'boto3' -prune \
      -o -name 'botocore' -prune \
      -o -name 'pip' -prune \
      -o -name 'setuptools' -prune \
      -o -name '*.dist-info' -prune \
      -o -type f -print |
  zip -qr9 "$ZIP_FILE" -@
)
zip -qru "$ZIP_FILE" etl_log_processing/*.py

set +x
echo
echo "Created '$ZIP_FILE'"
