# Using Terraform to deploy all

Start at the parent [../README.md](../README.md)

## Enable APIs

```bash
gcloud services enable \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  cloudbuild.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  pubsub.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"
```

## Init

```bash
terraform init
```

## Definitions

From own Infrastructure:
```bash
pushd ../1_source
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export SAMPLER_SERVICE_ACCOUNT_EMAIL=$(jq -c -r '.sampler_function_service_account.value.email' ${OUT_JSON})
export NOTIFICATION_SERVICE_ACCOUNT_EMAIL=$(jq -c -r '.notification_function_service_account.value.email' ${OUT_JSON})
export PUBSUB_CMD_SERVICE_ACCOUNT_EMAIL=$(jq -c -r '.cmd_pubsub_service_account.value.email' ${OUT_JSON})

export POLICY_BUCKET=$(jq -c -r '.policy_bucket.value.name' ${OUT_JSON})

export PUBSUB_CMD_TOPIC_ID=$(jq -c -r '.pubsub_cmd.value.id' ${OUT_JSON})
export PUBSUB_ERROR_TOPIC_ID=$(jq -c -r '.pubsub_err.value.id' ${OUT_JSON})

export ERROR_MONITORING_CHANNEL=$(jq -c -r '.error_monitoring_channel.value.display_name' ${OUT_JSON})
export NOTIFICAITON_ERROR_MONITORING_CHANNEL=$(jq -c -r '.notification_error_monitoring_channel.value.display_name' ${OUT_JSON})
rm -f ${OUT_JSON}
popd 
```

From target project:
```bash
pushd ../2_target
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export REQUEST_BUCKET=$(jq -c -r '.request_bucket.value.name' ${OUT_JSON})
export PUBSUB_BQ_NOTIFICATION_TOPIC_ID=$(jq -c -r '.pubsub_bq_notification.value.id' ${OUT_JSON})
rm -f ${OUT_JSON}
popd 
```

### Terraform Arguments

```bash
TERRAFORM_VAR_ARGS="-var \"project_id=${PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"target_project_id=${TARGET_PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"region=${REGION}\""
TERRAFORM_VAR_ARGS+=" -var \"bq_target_location=${BQ_TARGET_REGION}\""

TERRAFORM_VAR_ARGS+=" -var \"sampler_service_account_email=${SAMPLER_SERVICE_ACCOUNT_EMAIL}\""
TERRAFORM_VAR_ARGS+=" -var \"notification_function_service_account_email=${NOTIFICATION_SERVICE_ACCOUNT_EMAIL}\""
TERRAFORM_VAR_ARGS+=" -var \"pubsub_cmd_service_account_email=${PUBSUB_CMD_SERVICE_ACCOUNT_EMAIL}\""

TERRAFORM_VAR_ARGS+=" -var \"policy_bucket_name=${POLICY_BUCKET}\""
TERRAFORM_VAR_ARGS+=" -var \"request_bucket_name=${REQUEST_BUCKET}\""

TERRAFORM_VAR_ARGS+=" -var \"pubsub_cmd_topic_id=${PUBSUB_CMD_TOPIC_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"pubsub_err_topic_id=${PUBSUB_ERROR_TOPIC_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"pubsub_bq_notification_topic_id=${PUBSUB_BQ_NOTIFICATION_TOPIC_ID}\""

TERRAFORM_VAR_ARGS+=" -var \"monitoring_channel_name=${ERROR_MONITORING_CHANNEL}\""
TERRAFORM_VAR_ARGS+=" -var \"notification_monitoring_channel_name=${NOTIFICAITON_ERROR_MONITORING_CHANNEL}\""

echo "Terraform plan arguments: ${TERRAFORM_VAR_ARGS}"
```

## Plan and Apply

```bash
TMP=$(mktemp)
echo ${TERRAFORM_VAR_ARGS} \
  | xargs terraform plan -out ${TMP} \
  && terraform apply ${TMP} && rm -f ${TMP}
```

## Add missing bits

### Variables

```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export FUNCTION_NAME=$(jq -c -r '.sampler_function.value.name' ${OUT_JSON})
export POLICY_BUCKET=$(jq -c -r '.sampler_function.value.environment_variables.POLICY_BUCKET_NAME' ${OUT_JSON})
DEFAULT_POLICY_OBJECT=$(jq -c -r '.sampler_function.value.environment_variables.DEFAULT_POLICY_OBJECT_PATH' ${OUT_JSON})
export DEFAULT_POLICY_URI="gs://${POLICY_BUCKET}/${DEFAULT_POLICY_OBJECT}"
SMTP_CONFIG_URI=$(jq -c -r '.notification_function.value.environment_variables | if has("SMTP_CONFIG_URI") then .SMTP_CONFIG_URI else "" end' ${OUT_JSON})
SEMDGRID_CONFIG_URI=$(jq -c -r '.notification_function.value.environment_variables | if has("SEMDGRID_CONFIG_URI") then .SEMDGRID_CONFIG_URI else "" end' ${OUT_JSON})
if [ -n "${SMTP_CONFIG_URI}" ]
then
  export NOTIFICATION_TYPE=SMTP
  export NOTIFICATION_CONFIG_URI=${SMTP_CONFIG_URI}
elif [ -n "${SEMDGRID_CONFIG_URI}"]
then
  export NOTIFICATION_TYPE=SENDGRID
  export NOTIFICATION_CONFIG_URI=${SEMDGRID_CONFIG_URI}
fi
export NOTIFICATION_FUNCTION_NAME=$(jq -c -r '.notification_function.value.name' ${OUT_JSON})
export NOTIFICATION_SECRET=$(jq -c -r '.notification_secret.value | keys[0]' ${OUT_JSON})
rm -f ${OUT_JSON}
```

### Default policy

```bash
DEFAULT_POLICY_FILE="<local JSON containing the default policy>"
```

Example:
```bash
DEFAULT_POLICY_FILE="../../code/integ_test_data/policies/default_policy.json"
```

Upload:
```bash
gsutil cp ${DEFAULT_POLICY_FILE} ${DEFAULT_POLICY_URI}
```

### Secret

```bash
echo -e "Set secret content in: \n\thttps://console.cloud.google.com/security/secret-manager/secret/${NOTIFICATION_SECRET}/versions?project=${PROJECT_ID}"
```

### Notification config

Select the config file according to the type of notification:

```bash
echo "Your notification type is ${NOTIFICATION_TYPE}"
```

Define:

```bash
export EMAIL_SUBJECT_LINE="[ALERT] There was an issue executing ${FUNCTION_NAME}"
```

#### SMTP

Create config file (remember to change to your SMTP and authentication values):

```bash
CFG_FILE=$(mktemp)
cat > ${CFG_FILE} << __END__
{
  "username": "me@example.com",
  "password_secret_name": "${NOTIFICATION_SECRET}",
  "smtp_server": "smtp.gmail.com",
  "smtp_port": 587,
  "use_tls": true,
  "subject": "${EMAIL_SUBJECT_LINE}",
  "sender": "me@example.com",
  "recipients": [
    "me@example.com",
    "you@example.com"
  ]
}
__END__
echo "Config file in ${CFG_FILE}"
cat ${CFG_FILE}
```

#### SendGrid

Create config file:

```bash
CFG_FILE=$(mktemp)
cat > ${CFG_FILE} << __END__
{
  "api_key_secret_name": "${NOTIFICATION_SECRET}",
  "subject": "${EMAIL_SUBJECT_LINE}",
  "sender": "me@example.com",
  "recipients": [
    "me@example.com",
    "you@example.com"
  ]
}
__END__
echo "Config file in ${CFG_FILE}"
cat ${CFG_FILE}
```

#### Upload file

```bash
gsutil cp ${CFG_FILE} ${NOTIFICATION_CONFIG_URI}
rm -f ${CFG_FILE}
```

## [Testing](../README.md#testing)