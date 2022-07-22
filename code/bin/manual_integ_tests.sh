#!/bin/bash
# vim: ai:sw=4:ts=4:sta:et:fo=croql:nu
#
# Copyright 2022 Google.
# This software is provided as-is, without warranty or representation for any use or purpose.
# Your use of it is subject to your agreement with Google.
#
# This script will do the following:
#
# Setup:
# - All is deployed;
# - There is the trigger in place;
# - The two buckets (policy and request) are completely empty.
# - One exception: there is the general policy file.
# - [DEMO] Show the default/general policy file and where it is located;
# - [DEMO] Show that there are no tables copied;
# - [DEMO] Show the empty requests bucket.
#
# In the beginning:
# - [STORY] As a Scientist I need a table sample;
# - [DEMO] Navigate to the project/data set/table (to show the path) that is needed;
# - [DEMO] Create an empty POLICY file on the policy bucket;
# - [DEMO] Show the default policy applied to the specific table on the DS Env.
#
# Well I need something different:
# - [STORY] As a Scientist I'm not happy with the current sample;
# - [DEMO] Navigate into the REQUEST bucket and put a different request (that is beyond what the current general sample says);
# - [DEMO] Show that the request did fail, since the general policy does cap it;
# - [STORY] Wait, I asked for but did not get, what now?
# - [DEMO] Create a specific policy on the POLICY bucket which allows a bigger/different sample;
# - [DEMO] Show that with the specific policy the request can be fulfil (show the new sample in BQ);
#
# I need another table:
# - [STORY] As a Scientist I need a new table (and I already know that I need the policy first);
# - [DEMO] Create a non-empty POLICY file but corrupt (preferably a JSON invalid file);
# - [DEMO] Show that although the file is corrupt, the general policy applies (assumes the invalid file as an empty file) and the general sample is delivered;
# - [STORY] Let us fix the mistake on the POLICY;
# - [DEMO] Fix the file and show that the, specific, POLICY sample applies;
# - [STORY] Let me now, create my specific request;
# - [DEMO] Create a faulty request file (again, corrupt JSON or wrong schema is the best);
# - [DEMO] Show that still what is valid is the POLICY sample and not the request;
# - [STORY] Let me fix the request;
# - [DEMO] Fix the request file;
# - [DEMO] Show that the request applies.
#
# I don't need the request anymore:
# - [STORY] As a Scientist, I don't need as much data anymore on this table;
# - [DEMO] Remove the request file and show that the POLICY or general policy is the new sample;
# - [STORY] My bad, I don't need the table anymore;
# - [DEMO] Remove the policy file entirely;
# - [DEMO] Show that the sample table disappears.
#
# Wait there is a new schema in PROD:
# - [STORY] In PROD there is a new column for table;
# - [DEMO] Add a new column to a sampled table;
# - [DEMO] Show that the sample table now contains the new column;
# - [STORY] The PROD table has been removed (but the policies/requests are still there);
# - [DEMO] Remove a PROD table;
# - [DEMO] Show that the sample table disappears.
#
# I need more time with this table:
# - [STORY] As a Scientist I need table data for multiple days;
# - [DEMO] Put the lock file in place (to interrupt all sampling);
# - [DEMO] Change a table schema (by adding a column);
# - [DEMO] Show that the new schema is not applied, since the lock is in place;
# - [STORY] Ok, I'm done;
# - [DEMO] Remove the lock file;
# - [DEMO] Show that the column is now in the sample table.

set -ef

# Needs to be the first thing
ALL_CLI_ARGS="${*}"

###############################################################
### DEPENDENCIES DEFINITIONS
###############################################################

LINUX_GETOPT_CMD="getopt"
MAC_GETOPT_CMD="/usr/local/opt/gnu-getopt/bin/getopt"
GETOPT_CMD="${LINUX_GETOPT_CMD}"
LINUX_SED_CMD="sed"
MAC_SED_CMD="/usr/local/opt/gnu-sed/libexec/gnubin/sed"
SED_CMD="${LINUX_SED_CMD}"

unset REQUIRED_UTILITIES
set -a REQUIRED_UTILITIES
REQUIRED_UTILITIES=(
  "bash"
  "bq"
  "jq"
  "getopt"
  "gcloud"
  "gsutil"
  "sed"
  "tee"
)
if [ "Darwin" == $(uname -s) ]; then
  REQUIRED_UTILITIES+=("${MAC_GETOPT_CMD}")
  REQUIRED_UTILITIES+=("${MAC_SED_CMD}")
  GETOPT_CMD="${MAC_GETOPT_CMD}"
  SED_CMD="${MAC_SED_CMD}"
fi

###############################################################
### CLI DEFINITIONS
###############################################################

# CLI
# Source: https://gist.github.com/magnetikonline/22c1eb412daa350eeceee76c97519da8
OPT_HELP="help"
OPT_VERBOSE="verbose"
OPT_NO_DEPLOY="no-deploy"
OPT_LOGFILE="log"
OPT_REGION="region"
OPT_NAME="function"
OPT_PRJ_ID="prj-id"
OPT_TARGET_PRJ_ID="tgt-prj-id"
OPT_SA_EMAIL="sa-email"
OPT_PUBSUB_CMD="pubsub-cmd"
OPT_PUBSUB_ERROR="pubsub-error"
OPT_SCHED_JOB="cronjob"
OPT_POLICY_BUCKET="policy-bucket"
OPT_REQUEST_BUCKET="request-bucket"

unset ARGUMENT_FLAG_LIST
set -a ARGUMENT_FLAG_LIST
ARGUMENT_FLAG_LIST=(
  "${OPT_HELP}"
  "${OPT_VERBOSE}"
  "${OPT_NO_DEPLOY}"
)

unset ARGUMENT_LIST
set -a ARGUMENT_LIST
ARGUMENT_LIST=(
  "${OPT_LOGFILE}"
  "${OPT_REGION}"
  "${OPT_NAME}"
  "${OPT_PRJ_ID}"
  "${OPT_TARGET_PRJ_ID}"
  "${OPT_SA_EMAIL}"
  "${OPT_PUBSUB_CMD}"
  "${OPT_PUBSUB_ERROR}"
  "${OPT_SCHED_JOB}"
  "${OPT_POLICY_BUCKET}"
  "${OPT_REQUEST_BUCKET}"
)

# read arguments
CLI_ARGS=$(
  ${GETOPT_CMD} \
    --options "h" \
    --longoptions "$(printf "%s," "${ARGUMENT_FLAG_LIST[@]}")$(printf "%s:," "${ARGUMENT_LIST[@]:1}")${ARGUMENT_LIST[0]}:" \
    --name "$(basename "${0}")" \
    -- ${ALL_CLI_ARGS}
)

###############################################################
### DEFAULT DEFINITIONS
###############################################################

DEFAULT_REGION="europe-west3"
DEFAULT_FUNCTION_NAME="bq-sampler"
DEFAULT_FUNCTION_SA="${FUNCTION_NAME}-sa"
DEFAULT_PUBSUB_CMD_TOPIC="${FUNCTION_NAME}-cmd"
DEFAULT_PUBSUB_ERROR_TOPIC="${FUNCTION_NAME}-error"
DEFAULT_SCHEDULER_JOB_NAME="cronjob-${FUNCTION_NAME}"
DEFAULT_POLICY_BUCKET_NAME="sample-policy-1234"
DEFAULT_REQUEST_BUCKET_NAME="sample-request-9876"

###############################################################
### GLOBAL DEFINITIONS
###############################################################

PROJECT_ID=""
PROJECT_NUMBER=""
TARGET_PROJECT_ID=""
TARGET_PROJECT_NUMBER=""
REGION="${DEFAULT_REGION}"
LOCATION="${REGION}"
FUNCTION_NAME="${DEFAULT_FUNCTION_NAME}"
FUNCTION_SA="${DEFAULT_FUNCTION_SA}"
PUBSUB_CMD_TOPIC="${DEFAULT_PUBSUB_CMD_TOPIC}"
PUBSUB_ERROR_TOPIC="${DEFAULT_SCHEDULER_JOB_NAME}"
SCHEDULER_JOB_NAME="${DEFAULT_SCHEDULER_JOB_NAME}"
POLICY_BUCKET_NAME="${DEFAULT_POLICY_BUCKET_NAME}"
REQUEST_BUCKET_NAME="${DEFAULT_REQUEST_BUCKET_NAME}"
POLICY_OBJECT_PATH="default-policy.json"
SAMPLING_LOCK_OBJECT_PATH="block-sampling"

LOG_FILE=$(mktemp)
# YES == true
IS_DEBUG="NO"
DEBUG_LEVEL="DBG"

DEPLOY="YES"

###############################################################
### TEST_FILES
###############################################################

# Source: https://stackoverflow.com/questions/4175264/how-to-retrieve-absolute-path-given-relative
CURR_DIR=$(
  cd "$(dirname "${0}")"
  pwd
)
TEST_DATA_DIR="${CURR_DIR}/../integ_test_data"
POLICIES_DIR="${TEST_DATA_DIR}/policies"
REQUESTS_DIR="${TEST_DATA_DIR}/requests"

DATASET_ID_1="census_bureau_acs"
TABLE_DS_1_ID_1="blockgroup_2014_5yr"
TABLE_DS_1_ID_2="blockgroup_2017_5yr"
DATASET_ID_2="new_york_taxi_trips"
TABLE_DS_2_ID_1="tlc_fhv_trips_2015"
TABLE_DS_2_ID_2="tlc_green_trips_2016"

POLICY_DEFAULT="${POLICIES_DIR}/default_policy.json"
POLICY_EMPTY="${POLICIES_DIR}/empty_policy.json"
POLICY_EMPTY_PATH="${DATASET_ID_1}/${TABLE_DS_1_ID_1}.json"
POLICY_NON_JSON="${POLICIES_DIR}/non_json_policy.json"
POLICY_NON_JSON_PATH="${DATASET_ID_1}/${TABLE_DS_1_ID_2}.json"
POLICY_FULL_RANDOM="${POLICIES_DIR}/policy_full_random.json"
POLICY_FULL_RANDOM_PATH="${DATASET_ID_2}/${TABLE_DS_2_ID_2}.json"
POLICY_FULL_SORTED="${POLICIES_DIR}/policy_full_sorted_table_1.json"
POLICY_FULL_SORTED_PATH="${DATASET_ID_2}/${TABLE_DS_2_ID_1}.json"

REQUEST_EMPTY="${REQUESTS_DIR}/empty_request.json"
REQUEST_EMPTY_PATH="${DATASET_ID_1}/${TABLE_DS_1_ID_2}.json"
REQUEST_NON_JSON="${REQUESTS_DIR}/non_json_request.json"
REQUEST_NON_JSON_PATH="${DATASET_ID_1}/${TABLE_DS_1_ID_1}.json"
REQUEST_FULL_RANDOM="${REQUESTS_DIR}/request_full_random.json"
REQUEST_FULL_RANDOM_PATH="${DATASET_ID_2}/${TABLE_DS_2_ID_1}.json"
REQUEST_FULL_SORTED="${REQUESTS_DIR}/request_full_sorted_table_2.json"
REQUEST_FULL_SORTED_PATH="${DATASET_ID_2}/${TABLE_DS_2_ID_2}.json"

###############################################################
### LOG
###############################################################

function emit_log_msg {
  local ALL_ARGS=${*}

  local LEVEL="${1}"
  shift
  local MSG="${*}"

  if [ ${IS_DEBUG} == "YES" -o ${LEVEL} != ${DEBUG_LEVEL} ]; then
    echo "$(date -u +%F-%T) [${LEVEL}]: ${MSG}" | tee -a ${LOG_FILE}
  fi
}

function log_dbg {
  local MSG="${*}"

  emit_log_msg ${DEBUG_LEVEL} ${MSG}
}

function log_info {
  local MSG="${*}"

  emit_log_msg INFO ${MSG}
}

function log_warn {
  local MSG="${*}"

  emit_log_msg WARN ${MSG}
}

function log_error {
  local MSG=${*}

  emit_log_msg ERR ${MSG}
}

###############################################################
### EXEC CMD
###############################################################

function _exec_cmd {
  local ALL_ARGS="${*}"

  # 0 == true
  local IS_OUTPUT_LOG=${1}
  shift
  local CMD=${*}

  local ERR_OUT_FILE=$(mktemp)
  if [ "${IS_OUTPUT_LOG}" == "0" ]; then
    log_dbg "Executing ${CMD}"
    echo ${CMD} | bash >>${LOG_FILE} 2>&1
  else
    echo ${CMD} | bash 2>${ERR_OUT_FILE}
  fi
  if [ ${?} -ne 0 ]; then
    cat ${ERR_OUT_FILE} >>${LOG_FILE}
    log_error "Cloud not execute command, see logs for details in ${LOG_FILE}. Command: ${CMD}"
    rm -f ${ERR_OUT_FILE}
    exit 1
  fi
  rm -f ${ERR_OUT_FILE}
}

function exec_cmd {
  local CMD="${*}"

  _exec_cmd "0" "${CMD}"
}

function exec_cmd_out {
  local CMD="${*}"

  _exec_cmd "1" "${CMD}"
}

###############################################################
### GCP PROJECT ID/NUMBER
###############################################################

function current_project_id {
  exec_cmd_out "gcloud config get-value core/project"
}

function project_number {
  local PRJ_ID=${1:-$PROJECT_ID}

  exec_cmd_out "gcloud projects describe ${PRJ_ID} --format=\"get(projectNumber)\""
}

###############################################################
### TEMPLATING
###############################################################

function project_values {
  log_dbg "Setting project related variables: target project ID = ${TARGET_PROJECT_ID} and project ID = ${PROJECT_ID}"
  if [ -z "${TARGET_PROJECT_ID}" ]; then
    log_error "Target project ID cannot be empty"
    exit 1
  fi
  if [ -z "${PROJECT_ID}" ]; then
    PROJECT_ID=$(current_project_id)
  fi
  PROJECT_NUMBER=$(project_number ${PROJECT_ID})
  TARGET_PROJECT_ID=${TARGET_PROJECT_ID}
  TARGET_PROJECT_NUMBER=$(project_number ${TARGET_PROJECT_ID})
  log_dbg "Set project related variables: target project ID = ${TARGET_PROJECT_ID}(${TARGET_PROJECT_NUMBER}) and project ID = ${PROJECT_ID}(${PROJECT_NUMBER})"
}

###############################################################
### DEPENDENCIES
###############################################################

function check_utilities {
  # 0 == true
  local HAS_ALL=0
  # shellcheck disable=SC2068
  for U in ${REQUIRED_UTILITIES[@]}; do
    if [ -z "$(which ${U})" ]; then
      log_error "Missing utility ${U}"
      HAS_ALL=1
    fi
  done
}

###############################################################
### HELP
###############################################################

function help {
  echo
  echo -e "Usage:"
  echo -e "\t${0} [-h | --${OPT_HELP}] [--${OPT_VERBOSE}] [--${OPT_NO_DEPLOY}]"
  echo -e "\t\t--${OPT_TARGET_PRJ_ID} <TARGET_PROJECT_ID>"
  echo -e "\t\t[--${OPT_LOGFILE} <LOG_FILE>]"
  echo -e "\t\t[--${OPT_REGION} <REGION>]"
  echo -e "\t\t[--${OPT_PRJ_ID} <PROJECT_ID>]"
  echo -e "\t\t[--${OPT_NAME} <FUNCTION_NAME>]"
  echo -e "\t\t[--${OPT_SA_EMAIL} <FUNCTION_SA_EMAIL>]"
  echo -e "\t\t[--${OPT_PUBSUB_CMD} <PUBSUB_CMD_TOPIC>]"
  echo -e "\t\t[--${OPT_PUBSUB_ERROR} <PUBSUB_ERROR_TOPIC>]"
  echo -e "\t\t[--${OPT_SCHED_JOB} <SCHEDULER_JOB_NAME>]"
  echo -e "\t\t[--${OPT_POLICY_BUCKET} <POLICY_BUCKET_NAME>]"
  echo -e "\t\t[--${OPT_REQUEST_BUCKET} <REQUEST_BUCKET_NAME>]"
  echo -e "Where:"
  echo -e "\t-h | --${OPT_HELP} this help"
  echo -e "\t--${OPT_VERBOSE} set log level to DEBUG"
  echo -e "\t--${OPT_NO_DEPLOY} skip CloudFunction deploy step"
  echo -e "\t--${OPT_LOGFILE} with <LOG_FILE> being where the logs will be saved"
  echo -e "\t--${OPT_REGION} with <REGION> overwriting the default region, which is '${DEFAULT_REGION}'"
  echo -e "\t--${OPT_PRJ_ID} with <PROJECT_ID> overwriting the default, which is the current authenticated project ID"
  echo -e "\t--${OPT_TARGET_PRJ_ID} with <TARGET_PROJECT_ID> being where the sample should land, always required"
  echo
  echo -e "Example:"
  echo -e "\t${0} --${OPT_LOGFILE} integ_test.log \\"
  echo -e "\t\t--${OPT_TARGET_PRJ_ID} MY_TARGET_PROJECT_ID \\"
  echo -e "\t\t--${OPT_REGION} ${DEFAULT_REGION} \\"
  echo -e "\t\t--${OPT_NAME} ${DEFAULT_FUNCTION_NAME} \\"
  echo -e "\t\t--${OPT_SA_EMAIL} ${DEFAULT_FUNCTION_SA} \\"
  echo -e "\t\t--${OPT_PUBSUB_CMD} ${DEFAULT_PUBSUB_CMD_TOPIC} \\"
  echo -e "\t\t--${OPT_PUBSUB_ERROR} ${DEFAULT_PUBSUB_ERROR_TOPIC} \\"
  echo -e "\t\t--${OPT_SCHED_JOB} ${DEFAULT_SCHEDULER_JOB_NAME} \\"
  echo -e "\t\t--${OPT_POLICY_BUCKET} ${DEFAULT_POLICY_BUCKET_NAME} \\"
  echo -e "\t\t--${OPT_REQUEST_BUCKET} ${DEFAULT_REQUEST_BUCKET_NAME}"
  echo
}

###############################################################
### HIGH LEVEL HELPER FUNCTIONS
###############################################################

### High level

function show_content {
  log_info "Showing content"
  list_bucket "${POLICY_BUCKET_NAME}"
  list_bucket "${REQUEST_BUCKET_NAME}"
  list_big_query "${TARGET_PROJECT_ID}"
}

function table_id_from_path {
  local PRJ_ID=${1}
  local OBJ_PATH=${2}

  if [ -z "${PRJ_ID}" ]; then
    log_error "No project ID for creating table ID"
    exit 1
  fi
  if [ -z "${OBJ_PATH}" ]; then
    log_error "No object path for creating table ID"
    exit 1
  fi
  local DATASET_ID="${OBJ_PATH%%/*}"
  local TABLE_ID="${OBJ_PATH##*/}"
  TABLE_ID="${TABLE_ID//.json/}"
  echo "${PRJ_ID}:${DATASET_ID}.${TABLE_ID}"
}

function wait_for_function {
  echo "Monitor function execution at:"
  echo "  https://console.cloud.google.com/functions/details/${REGION}/${FUNCTION_NAME}?project=${PROJECT_ID}"
  read -n 1 -p "Once the function finished executing, press <ENTER>:" TO_IGNORE
}

function wait_for_big_query {
  local TBL_ID=${1}

  if [ -z "${TBL_ID}" ]; then
    log_error "No table given for BigQuery table details"
    exit 1
  fi
  local TBL_PRJ_ID="${TBL_ID%%:*}"
  local TBL_DS_ID="${TBL_ID##*:}"
  local TBL_TBL_ID="${TBL_DS_ID##*.}"
  TBL_DS_ID="${TBL_DS_ID%%.*}"
  echo "Check BigQuery state at:"
  echo "  https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${TBL_PRJ_ID}!2s${TBL_DS_ID}!3s${TBL_TBL_ID}"
  read -n 1 -p "Once you've checked, press <ENTER>:" TO_IGNORE
}

### GCS

function clear_bucket {
  local BUCKET=${1}

  if [ -z "${BUCKET}" ]; then
    log_error "No bucket given for clean up"
    exit 1
  fi
  log_info "Cleaning up bucket ${BUCKET}"
  exec_cmd "gsutil rm -r gs://${BUCKET}/** || echo \"Error. Ignoring.\""
}

function list_bucket {
  local BUCKET=${1}

  if [ -z "${BUCKET}" ]; then
    log_error "No bucket given for listing"
    exit 1
  fi
  log_info "Listing all objects in bucket ${BUCKET}"
  exec_cmd_out "gsutil ls -r gs://${BUCKET}"
}

### GCS: object

function upload_file {
  local BUCKET=${1}
  local OBJ_PATH=${2}
  local FILE=${3}

  if [ -z "${BUCKET}" ]; then
    log_error "No bucket given for upload"
    exit 1
  fi
  if [ -z "${OBJ_PATH}" ]; then
    log_error "No bucket path given for upload"
    exit 1
  fi
  if [ -z "${FILE}" ]; then
    log_error "No file given for upload"
    exit 1
  fi
  local GCS_URI="gs://${BUCKET}/${OBJ_PATH}"
  log_info "Uploading file ${FILE} into ${GCS_URI}"
  exec_cmd "gsutil cp ${FILE} ${GCS_URI}"
}

function delete_object {
  local BUCKET=${1}
  local OBJ_PATH=${2}

  if [ -z "${BUCKET}" ]; then
    log_error "No bucket given for upload"
    exit 1
  fi
  if [ -z "${OBJ_PATH}" ]; then
    log_error "No bucket path given for upload"
    exit 1
  fi
  local GCS_URI="gs://${BUCKET}/${OBJ_PATH}"
  log_info "Deleting object in ${GCS_URI}"
  exec_cmd "gsutil rm -f ${GCS_URI}"
}

### BigQuery

function clear_big_query {
  local PRJ_ID=${1}

  if [ -z "${PRJ_ID}" ]; then
    log_error "No project given for BigQuery clean up"
    exit 1
  fi
  log_info "Cleaning up BigQuery in project ${PRJ_ID}"
  local -a DATASETS
  # format: <PRJ>:<DS>
  DATASETS=($(_list_all_datasets ${PRJ_ID}))
  # shellcheck disable=SC2068
  for DS_ID in ${DATASETS[@]}; do
    log_info "Deleting BigQuery dataset ${DS_ID}"
    _delete_big_query_dataset ${DS_ID}
  done
}

function list_big_query {
  local PRJ_ID=${1}

  if [ -z "${PRJ_ID}" ]; then
    log_error "No project given for BigQuery up"
    exit 1
  fi
  log_info "Listing all BigQuery content in project ${PRJ_ID}"
  local -a DATASETS
  # format: <PRJ>:<DS>
  DATASETS=($(_list_all_datasets ${PRJ_ID}))
  # shellcheck disable=SC2068
  for DS_ID in ${DATASETS[@]}; do
    local -a TABLES
    # format: <PRJ>:<DS>.<TBL>
    TABLES=($(_list_all_tables_in_dataset ${DS_ID}))
    for TBL_ID in ${TABLES[@]}; do
      big_query_table_details ${TBL_ID}
    done
  done
}

### BigQuery: dataset

function _list_all_datasets {
  local PRJ_ID=${1}

  # format: <PRJ>:<DS>
  exec_cmd_out "bq --location ${LOCATION} --project_id ${PRJ_ID} --format json ls -d" |
    jq -c -r '.[].id'
}

function _delete_big_query_dataset {
  local DATASET_ID=${1}

  exec_cmd "bq --location ${LOCATION} rm -r -f ${DATASET_ID}"
}

### BigQuery: table

function _drop_big_query_table {
  local TBL_ID=${1}

  exec_cmd "bq --location ${LOCATION} rm -f ${TBL_ID}"
}

function _list_all_tables_in_dataset {
  local DATASET_ID=${1}

  # format: <PRJ>:<DS>.<TBL>
  exec_cmd_out "bq --location ${LOCATION} --format json ls ${DATASET_ID}" |
    jq -c -r '.[].id'
}

function big_query_rename_table {
  local TBL_ID=${1}
  local TBL_NEW_ID=${2}

  if [ -z "${TBL_ID}" ]; then
    log_error "No table given for BigQuery table renaming"
    exit 1
  fi
  if [ -z "${TBL_NEW_ID}" ]; then
    log_error "No table given for BigQuery table renaming"
    exit 1
  fi
  log_info "Renaming table ${TBL_ID} into ${TBL_NEW_ID}"
  _big_query_copy_table "${TBL_ID}" "${TBL_NEW_ID}"
  _drop_big_query_table "${TBL_ID}"
}

function _big_query_copy_table {
  local TBL_ID=${1}
  local TBL_NEW_ID=${2}

  exec_cmd "bq --location ${LOCATION} cp ${TBL_ID} ${TBL_NEW_ID}"
}

function big_query_table_details {
  local TBL_ID=${1}

  if [ -z "${TBL_ID}" ]; then
    log_error "No table given for BigQuery table details"
    exit 1
  fi
  log_info "Getting details for BigQuery table ${TBL_ID}"
  exec_cmd_out "bq --location ${LOCATION} --project_id ${PRJ_ID} --format json show ${TBL_ID}" |
    jq -c '{"table":.id, "columns": [.schema.fields[].name], "rows": .numRows}'
}

function big_query_table_add_column {
  local TBL_ID=${1}
  local COL_NAME=${2}

  if [ -z "${TBL_ID}" ]; then
    log_error "No table given for BigQuery table schema change"
    exit 1
  fi
  if [ -z "${COL_NAME}" ]; then
    log_error "No column name given for BigQuery table schema change"
    exit 1
  fi
  log_info "Adding column ${COL_NAME} to BigQuery table ${TBL_ID}"
  local CURR_SCHEMA=$(mktemp)
  local NEW_SCHEMA=$(mktemp)
  _big_query_table_schema "${TBL_ID}" >${CURR_SCHEMA}
  if [ -z "$(grep ${COL_NAME} ${CURR_SCHEMA})" ]; then
    cat ${CURR_SCHEMA} |
      jq -c ". + [{
                \"name\": \"${COL_NAME}\",
                \"type\": \"STRING\",
                \"mode\": \"NULLABLE\",
                \"description\": \"Added column\"
            }]" \
        >${NEW_SCHEMA}
    _big_query_update_schema "${TBL_ID}" "${NEW_SCHEMA}"
  else
    log_info "Current schema already contains column ${COL_NAME}. Ignoring."
  fi
}

function _big_query_table_schema {
  local TBL_ID=${1}

  exec_cmd_out "bq --location ${LOCATION} --format json show --schema ${TBL_ID}"
}

function _big_query_update_schema {
  local TBL_ID=${1}
  local SCHEMA_FILE=${2}

  exec_cmd "bq --location ${LOCATION} update --schema ${SCHEMA_FILE} ${TBL_ID}"
}

function big_query_table_drop_column {
  local TBL_ID=${1}
  local COL_NAME=${2}

  if [ -z "${TBL_ID}" ]; then
    log_error "No table given for BigQuery table schema change"
    exit 1
  fi
  if [ -z "${COL_NAME}" ]; then
    log_error "No column name given for BigQuery table schema change"
    exit 1
  fi
  log_info "Dropping column ${COL_NAME} in BigQuery table ${TBL_ID}"
  local TBL_DS_ID="${TBL_ID##*:}"
  _big_query_query_replace ${TBL_ID} "SELECT * EXCEPT(${COL_NAME}) FROM \"${TBL_DS_ID}\""
}

function _big_query_query_replace {
  local TBL_ID="${1}"
  shift 1
  local QUERY="${@}"

  log_info "Executing query ${QUERY} into ${TBL_ID}"
  local TBL_PRJ_ID="${TBL_ID%%:*}"
  exec_cmd "bq \
      --location ${LOCATION} \
      --project_id ${TBL_PRJ_ID} \
      query \
      --destination_table ${TBL_ID} \
      --replace \
      --use_legacy_sql=false \
      \"${QUERY}\""
  log_dbg "Executed query ${QUERY} into ${TBL_ID}"
}

### Function

function deploy_function {
  log_info "Deploying function ${FUNCTION_NAME}"
  local -a ENV_VARS
  ENV_VARS+=("BQ_LOCATION=${LOCATION}")
  ENV_VARS+=("TARGET_PROJECT_ID=${TARGET_PROJECT_ID}")
  ENV_VARS+=("POLICY_BUCKET_NAME=${POLICY_BUCKET_NAME}")
  ENV_VARS+=("DEFAULT_POLICY_OBJECT_PATH=${DEFAULT_POLICY_OBJECT_PATH}")
  ENV_VARS+=("REQUEST_BUCKET_NAME=${REQUEST_BUCKET_NAME}")
  ENV_VARS+=("SAMPLING_LOCK_OBJECT_PATH=${SAMPLING_LOCK_OBJECT_PATH}")
  ENV_VARS+=("CMD_TOPIC_NAME=projects/${PROJECT_ID}/topics/${PUBSUB_CMD_TOPIC}")
  ENV_VARS+=("ERROR_TOPIC_NAME=projects/${PROJECT_ID}/topics/${PUBSUB_ERROR_TOPIC}")
  ENV_VARS+=("LOG_LEVEL=INFO")
  local ENV_VARS_COMMA=$(
    local IFS=","
    echo "${ENV_VARS[*]}"
  )
  exec_cmd "gcloud functions deploy \"${FUNCTION_NAME}\" \
        --project=\"${PROJECT_ID}\" \
        --region=\"${REGION}\" \
        --runtime=\"python39\" \
        --entry-point=\"handler\" \
        --service-account=\"${FUNCTION_SA}\" \
        --trigger-topic=\"${PUBSUB_CMD_TOPIC}\" \
        --set-env-vars=\"${ENV_VARS_COMMA}\""
}

function trigger_function {
  log_info "Triggering function ${FUNCTION_NAME} using ${SCHEDULER_JOB_NAME}"
  exec_cmd "gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
        --project=\"${PROJECT_ID}\" \
        --location=\"${LOCATION}\""
  echo "Monitor function execution at:"
  echo "  https://console.cloud.google.com/functions/details/${REGION}/${FUNCTION_NAME}?project=${PROJECT_ID}"
  read -n 1 -p "Once the function finished executing, press <ENTER>:" TO_IGNORE
}

function function_logs {
  log_info "Getting logs for function ${FUNCTION_NAME}"
  exec_cmd_out "gcloud logging read \
        \"resource.labels.function_name=${FUNCTION_NAME} severity>=WARNING\" \
        --project=\"${PROJECT_ID}\""
}

###############################################################
### INTEGRATION TESTS
###############################################################

###############################################################
function clean_up {
  log_info "###############################################################"
  log_info "# Setup:"
  log_info "# - All is deployed;"
  log_info "# - There is the trigger in place;"
  log_info "# - The two buckets (policy and request) are completely empty."
  log_info "# - One exception: there is the general policy file."
  log_info "# - [DEMO] Show the default/general policy file and where it is located;"
  log_info "# - [DEMO] Show that there are no tables copied;"
  log_info "# - [DEMO] Show the empty requests bucket."
  log_info "###############################################################"
  log_info "Cleaning up demo: emptying buckets and target tables"
  clear_bucket "${POLICY_BUCKET_NAME}"
  clear_bucket "${REQUEST_BUCKET_NAME}"
  clear_big_query "${TARGET_PROJECT_ID}"
  log_info "Adding general policy"
  upload_file "${POLICY_BUCKET_NAME}" "${POLICY_OBJECT_PATH}" "${POLICY_DEFAULT}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
function first_empty_policy {
  log_info "###############################################################"
  log_info "# In the beginning:"
  log_info "# - [STORY] As a Scientist I need a table sample;"
  log_info "# - [DEMO] Navigate to the project/data set/table (to show the path) that is needed;"
  log_info "# - [DEMO] Create an empty POLICY file on the policy bucket;"
  log_info "# - [DEMO] Show the default policy applied to the specific table on the DS Env."
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_EMPTY_PATH}"
  log_info "Adding empty policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_EMPTY}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
function request_not_compliant {
  log_info "###############################################################"
  log_info "# Well I need something different:"
  log_info "# - [STORY] As a Scientist I'm not happy with the current sample;"
  log_info "# - [DEMO] Navigate into the REQUEST bucket and put a different request (that is beyond what the current general sample says);"
  log_info "# - [DEMO] Show that the request did fail, since the general policy does cap it;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_EMPTY_PATH}"
  log_info "Adding request violating policy limits: ${TARGET}"
  upload_file "${REQUEST_BUCKET_NAME}" "${TARGET}" "${REQUEST_FULL_RANDOM}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function policy_for_request_compliance {
  log_info "###############################################################"
  log_info "# - [STORY] Wait, I asked for but did not get, what now?"
  log_info "# - [DEMO] Create a specific policy on the POLICY bucket which allows a bigger/different sample;"
  log_info "# - [DEMO] Show that with the specific policy the request can be fulfil (show the new sample in BQ);"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_EMPTY_PATH}"
  log_info "Adding policy to allow requested sample: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
function invalid_policy_json {
  log_info "###############################################################"
  log_info "# I need another table:"
  log_info "# - [STORY] As a Scientist I need a new table (and I already know that I need the policy first);"
  log_info "# - [DEMO] Create a non-empty POLICY file but corrupt (preferably a JSON invalid file);"
  log_info "# - [DEMO] Show that although the file is corrupt, the general policy applies (assumes the invalid file as an empty file) and the general sample is delivered;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_NON_JSON_PATH}"
  log_info "Adding non-json policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${PROJECT_ID}/${POLICY_NON_JSON_PATH}" "${POLICY_NON_JSON}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function fix_invalid_policy_json {
  log_info "###############################################################"
  log_info "# - [STORY] Let us fix the mistake on the POLICY;"
  log_info "# - [DEMO] Fix the file and show that the, specific, POLICY sample applies;"
  log_info "# - [STORY] Let us fix the mistake on the POLICY;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_NON_JSON_PATH}"
  log_info "Fixing non-json policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function invalid_request_json {
  log_info "###############################################################"
  log_info "# - [STORY] Let me now, create my specific request;"
  log_info "# - [DEMO] Create a faulty request file (again, corrupt JSON or wrong schema is the best);"
  log_info "# - [DEMO] Show that still what is valid is the POLICY sample and not the request;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_EMPTY_PATH}"
  log_info "Adding non-json request: ${TARGET}"
  upload_file "${REQUEST_BUCKET_NAME}" "${TARGET}" "${REQUEST_NON_JSON}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function fix_invalid_request_json {
  log_info "###############################################################"
  log_info "# - [STORY] Let me fix the request;"
  log_info "# - [DEMO] Fix the request file;"
  log_info "# - [DEMO] Show that the request applies."
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_EMPTY_PATH}"
  log_info "Fixing non-json request: ${TARGET}"
  upload_file "${REQUEST_BUCKET_NAME}" "${TARGET}" "${REQUEST_FULL_RANDOM}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
function request_removal_default_sample {
  log_info "###############################################################"
  log_info "# I don't need the request anymore:"
  log_info "# - [STORY] As a Scientist, I don't need as much data anymore on this table;"
  log_info "# - [DEMO] Remove the request file and show that the POLICY or general policy is the new sample;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_RANDOM_PATH}"
  log_info "Adding a new table but with invalid policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_NON_JSON}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function policy_removal {
  log_info "###############################################################"
  log_info "# - [STORY] My bad, I don't need the table anymore;"
  log_info "# - [DEMO] Remove the policy file entirely;"
  log_info "# - [DEMO] Show that the sample table disappears."
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_RANDOM_PATH}"
  log_info "Removing invalid policy: ${TARGET}"
  delete_object "${POLICY_BUCKET_NAME}" "${TARGET}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
function source_schema_change_overwrites_local {
  log_info "###############################################################"
  log_info "# Wait there is a schema overwrite in the Sample:"
  log_info "# - [STORY] In the sample there is a new column in the table;"
  log_info "# - [DEMO] Add a new column to a sampled table;"
  log_info "# - [DEMO] Show that the sample table does not contain the new column anymore;"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_RANDOM_PATH}"
  log_info "Adding table policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  trigger_function
  show_content
  local BQ_TABLE_ID=$(table_id_from_path "${TARGET_PROJECT_ID}" "${POLICY_FULL_RANDOM_PATH}")
  log_info "Changing sample table schema: ${BQ_TABLE_ID}"
  big_query_table_add_column "${BQ_TABLE_ID}" "column_in_sample_table"
  show_content
  wait_for_big_query "${BQ_TABLE_ID}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function source_table_removed {
  log_info "###############################################################"
  log_info "# - [STORY] The PROD table has been removed (but the policies/requests are still there);"
  log_info "# - [DEMO] Remove a PROD table;"
  log_info "# - [DEMO] Show that the sample table disappears."
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_RANDOM_PATH}"
  log_info "Creating policy and sample for: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  trigger_function
  show_content
  local BQ_TABLE_ID=$(table_id_from_path "${PROJECT_ID}" "${POLICY_FULL_RANDOM_PATH}")
  local BQ_TABLE_NEW_ID="${BQ_TABLE_ID}_cloned"
  big_query_rename_table "${BQ_TABLE_ID}" "${BQ_TABLE_NEW_ID}"
  wait_for_big_query "${BQ_TABLE_ID}"
  trigger_function
  show_content
  big_query_rename_table "${BQ_TABLE_NEW_ID}" "${BQ_TABLE_ID}"
  log_info "###############################################################"
}

###############################################################
function lock_sampling {
  log_info "###############################################################"
  log_info "# I need more time with this table:"
  log_info "# - [STORY] As a Scientist I need table data for multiple days;"
  log_info "# - [DEMO] Put the lock file in place (to interrupt all sampling);"
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_RANDOM_PATH}"
  log_info "Adding table policy: ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  trigger_function
  show_content
  log_info "Locking sample"
  upload_file "${REQUEST_BUCKET_NAME}" "${SAMPLING_LOCK_OBJECT_PATH}" "${POLICY_EMPTY}"
  log_info "Removing policy for table: ${TARGET}"
  delete_object "${POLICY_BUCKET_NAME}" "${TARGET}"
  trigger_function
  show_content
  delete_object "${REQUEST_BUCKET_NAME}" "${SAMPLING_LOCK_OBJECT_PATH}"
  trigger_function
  show_content
  log_info "###############################################################"
}

function source_schema_change_during_lock {
  log_info "###############################################################"
  log_info "# - [DEMO] Change a table schema (by adding a column);"
  log_info "# - [DEMO] Show that the new schema is not applied, since the lock is in place;"
  log_info "# - [STORY] Ok, I'm done;"
  log_info "# - [DEMO] Remove the lock file;"
  log_info "# - [DEMO] Show that the column is now in the sample table."
  log_info "###############################################################"
  local TARGET="${PROJECT_ID}/${POLICY_FULL_SORTED_PATH}"
  log_info "Locking sample and changing schema in ${TARGET}"
  upload_file "${POLICY_BUCKET_NAME}" "${TARGET}" "${POLICY_FULL_RANDOM}"
  upload_file "${REQUEST_BUCKET_NAME}" "${SAMPLING_LOCK_OBJECT_PATH}" "${POLICY_EMPTY}"
  local BQ_TABLE_ID=$(table_id_from_path "${PROJECT_ID}" "${POLICY_FULL_SORTED_PATH}")
  log_info "Adding column to source table schema: ${BQ_TABLE_ID}"
  big_query_table_add_column "${BQ_TABLE_ID}" "column_in_source_table"
  show_content
  wait_for_big_query "${BQ_TABLE_ID}"
  trigger_function
  show_content
  delete_object "${REQUEST_BUCKET_NAME}" "${SAMPLING_LOCK_OBJECT_PATH}"
  trigger_function
  show_content
  log_info "Removing column from source table schema: ${BQ_TABLE_ID}"
  big_query_table_drop_column "${BQ_TABLE_ID}" "column_in_source_table"
  show_content
  wait_for_big_query "${BQ_TABLE_ID}"
  trigger_function
  show_content
  log_info "###############################################################"
}

###############################################################
### MAIN
###############################################################

function main {
  # Check required options
  if [ -z "${TARGET_PROJECT_ID}" ]; then
    log_error "Target project ID option [--${OPT_TARGET_PRJ_ID}] is always required, unless help."
    help
    exit 1
  fi
  log_info "Logs are in ${LOG_FILE}"
  # prepare
  project_values
  if [ "${DEPLOY}" == "YES" ]; then
    deploy_function
  fi
  clean_up
  # run tests
  first_empty_policy
  request_not_compliant
  policy_for_request_compliance
  invalid_policy_json
  fix_invalid_policy_json
  invalid_request_json
  fix_invalid_request_json
  request_removal_default_sample
  policy_removal
  source_schema_change_overwrites_local
  source_table_removed
  lock_sampling
  source_schema_change_during_lock
}

###############################################################
### PARSING CLI
###############################################################

# Source: https://gist.github.com/magnetikonline/22c1eb412daa350eeceee76c97519da8

eval set -- "${CLI_ARGS}"
# shellcheck disable=SC2145
log_info "CLI args = [${@}]"

while [[ ${#} -gt 0 ]]; do
  case ${1} in
  ## Options without arguments
  -h | --${OPT_HELP})
    help
    exit 0
    ;;
  --${OPT_VERBOSE})
    IS_DEBUG="YES"
    shift 1
    ;;
  --${OPT_NO_DEPLOY})
    DEPLOY="NO"
    shift 1
    ;;
    ## Options with arguments
  --${OPT_LOGFILE})
    # Source: https://stackoverflow.com/questions/4175264/how-to-retrieve-absolute-path-given-relative
    LOG_FILE=$(
      cd "$(dirname "${2}")"
      pwd
    )/$(basename "${2}")
    shift 2
    ;;
  --${OPT_REGION})
    REGION=${2}
    LOCATION="${REGION}"
    shift 2
    ;;
  --${OPT_PRJ_ID})
    PROJECT_ID=${2}
    shift 2
    ;;
  --${OPT_TARGET_PRJ_ID})
    TARGET_PROJECT_ID=${2}
    shift 2
    ;;
  --${OPT_NAME})
    FUNCTION_NAME=${2}
    shift 2
    ;;
  --${OPT_SA_EMAIL})
    FUNCTION_SA=${2}
    shift 2
    ;;
  --${OPT_PUBSUB_CMD})
    PUBSUB_CMD_TOPIC=${2}
    shift 2
    ;;
  --${OPT_PUBSUB_ERROR})
    PUBSUB_ERROR_TOPIC=${2}
    shift 2
    ;;
  --${OPT_SCHED_JOB})
    SCHEDULER_JOB_NAME=${2}
    shift 2
    ;;
  --${OPT_POLICY_BUCKET})
    POLICY_BUCKET_NAME=${2}
    shift 2
    ;;
  --${OPT_REQUEST_BUCKET})
    REQUEST_BUCKET_NAME=${2}
    shift 2
    ;;
  *)
    break
    ;;
  esac
done

check_utilities
main
