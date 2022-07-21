# Using Terraform to deploy all

## Authenticate

```bash
gcloud auth application-default login
```

### Set default project

```bash
gcloud init
```

## Definitions

Manually set:

```bash
export TARGET_PROJECT_ID="<WHERE THE SAMPLE WILL GO, NOT THE CURRENT PROJECT ID>"
export ERROR_NOTIFICATION_EMAIL_ADDRESS="<EMAIL TO USE IN CASE THERE IS AN UNCAUGHT EXCEPTION>"
```

Automatically set:

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export REGION="europe-west3"
```

Check:

```bash
echo "Main project: ${PROJECT_ID}@${REGION}"
echo "Target project: ${TARGET_PROJECT_ID}@${REGION}"
echo "Error notification email: ${ERROR_NOTIFICATION_EMAIL_ADDRESS}"
```

## Enable APIs

Main project:

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

Target project:

```bash
gcloud services enable \
  bigquery.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com \
  --project="${TARGET_PROJECT_ID}"
```

## Init

```bash
terraform init
```

## Plan

```bash
TMP=$(mktemp)
terraform plan \
  -out ${TMP} \
  -var "project_id=${PROJECT_ID}" \
  -var "target_project_id=${TARGET_PROJECT_ID}" \
  -var "region=${REGION}" \
  -var "notification_monitoring_email_address=${ERROR_NOTIFICATION_EMAIL_ADDRESS}"
```

## Apply

```bash
terraform apply ${TMP} && rm -f ${TMP}
```

## Add missing bits

### Variables

```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export PROJECT_ID=$(jq -c -r '.sampler_function.value.project' ${OUT_JSON})
export TARGET_PROJECT_ID=$(jq -c -r '.request_bucket.value.project' ${OUT_JSON})
export LOCATION=$(jq -c -r '.sampler_function.value.region' ${OUT_JSON})
export FUNCTION_NAME=$(jq -c -r '.sampler_function.value.name' ${OUT_JSON})
POLICY_BUCKET=$(jq -c -r '.sampler_function.value.environment_variables.POLICY_BUCKET_NAME' ${OUT_JSON})
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
export PUBSUB_ERROR_TOPIC=$(jq -c -r '.pubsub_err.value.name' ${OUT_JSON})
export SCHEDULER_JOB_NAME=$(jq -c -r '.trigger_job.value.name' ${OUT_JSON})
```

### Default policy

```bash
DEFAULT_POLICY_FILE="<local JSON containing the default policy>"
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

Similar to [DEPLOY_EMAIL](../code/DEPLOY_EMAIL.md)

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
```

#### Upload file

```bash
gsutil cp ${CFG_FILE} ${NOTIFICATION_CONFIG_URI}
```

## Testing

### Error in PubSub

```bash
gcloud pubsub topics publish ${PUBSUB_ERROR_TOPIC} \
  --project="${PROJECT_ID}" \
  --message="{\"key\":\"value\"}"
```

### Full Sampling

```bash
gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}"
```