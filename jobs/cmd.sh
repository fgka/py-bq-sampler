#!/bin/bash
# It *needs* to be BASH
# Source: https://cloud.google.com/run/docs/quickstarts/jobs/build-create-shell

# Required env vars:
# - BUCKET: where to scan for 'requirements.txt' files
# - PIP_REPO_ENDPOINT: where to upload packages to, note it *DOES* end with a '/'
# Alternatively: inform them as CLI args

BUCKET=${BUCKET:-$1}
PIP_REPO_ENDPOINT=${PIP_REPO_ENDPOINT:-$2}
WHEELS_DIR="wheels"
REQ_FILE_NAME="requirements.txt"
PIP_TIMEOUT_SEC=300

function help
{
  echo
  echo "Usage:"
  echo "  ${0} <BUCKET> <PIP_REPO_ENDPOINT>"
  echo
  echo "Alternative:"
  echo "  export BUCKET=\"<BUCKET>\""
  echo "  export PIP_REPO_ENDPOINT=\"<PIP_REPO_ENDPOINT>\""
  echo "  ${0}"
}

function gsutil_ls
{
  gsutil ls gs://${BUCKET}/**/${REQ_FILE_NAME}
}

function gsutil_copy
{
  local OBJECT=${1}

  gsutil copy ${OBJECT} ./
}

function pip_download
{
  python3 -m pip download \
    --isolated \
    --no-cache-dir \
    --timeout ${PIP_TIMEOUT_SEC} \
    --dest ${WHEELS_DIR} \
    --requirement ${REQ_FILE_NAME}
}

function twine_upload
{
  # The reason for uploading one by one is that 
  #   it seems like twine keep the same connection the whole time
  #   and it eventually will timeout.
  local PACKAGE=${1}

  python3 -m twine upload \
    --non-interactive \
    --disable-progress-bar \
    --skip-existing \
    --repository-url ${PIP_REPO_ENDPOINT} \
    ${PACKAGE}
}

function process_gs_object
{
  local OBJECT=${1}

  local TMP=`mktemp -d`
  pushd ${TMP}
  gsutil_copy ${OBJECT}
  pip_download
  # https://cloud.google.com/artifact-registry/docs/python/store-python
  ls -1 ${WHEELS_DIR} \
  | while read FILE
    do
      twine_upload ${WHEELS_DIR}/${FILE}
    done
  popd
  rm -rf ${TMP}
}

function main
{
  echo "[INFO] Using bucket: <${BUCKET}>"
  echo "[INFO] Using pip repository endpoint: <${PIP_REPO_ENDPOINT}>"

  gsutil_ls \
   | while read OBJECT
     do
       process_gs_object ${OBJECT}
     done
}

function check_input
{
  if [ -z ${BUCKET} ]
  then
    echo "[ERROR] Bucket missing"
    help
    exit 1
  fi
  if [ -z ${PIP_REPO_ENDPOINT} ]
  then
    echo "[ERROR] Pip repository endpoint missing"
    help
    exit 1
  fi
}

check_input
main
