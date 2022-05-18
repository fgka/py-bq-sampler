# Using Terraform to deploy all

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

## Init

```bash
pushd terraform
terraform init
popd
```

## Plan

```bash
pushd terraform
terraform plan \
  -var "project_id=${PROJECT_ID}" \
  -var "target_project_id=${TARGET_PROJECT_ID}" \
  -var "region=${REGION}" \
  -var "notification_monitoring_email_address=${ERROR_NOTIFICATION_EMAIL_ADDRESS}"
popd
```

## Apply

```bash
pushd terraform
terraform apply \
  -var "project_id=${PROJECT_ID}" \
  -var "target_project_id=${TARGET_PROJECT_ID}" \
  -var "region=${REGION}" \
  -var "notification_monitoring_email_address=${ERROR_NOTIFICATION_EMAIL_ADDRESS}"
popd
```

## Add missing bits

### Variables

```bash
OUT_JSON=`mktemp`
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export PROJECT_ID=$(jq -c -r '.project_id.value' ${OUT_JSON})
export TARGET_PROJECT_ID=$(jq -c -r '.target_project_id.value' ${OUT_JSON})
export LOCATION=$(jq -c -r '.region.value' ${OUT_JSON})
export FUNCTION_NAME=$(jq -c -r '.function_name.value' ${OUT_JSON})
export DEFAULT_POLICY_URI=$(jq -c -r '.default_policy_uri.value' ${OUT_JSON})
export NOTIFICATION_TYPE=$(jq -c -r '.notification_type.value' ${OUT_JSON})
export NOTIFICATION_FUNCTION_NAME=$(jq -c -r '.notification_function_name.value' ${OUT_JSON})
export NOTIFICATION_SECRET=$(jq -c -r '.notification_secret.value' ${OUT_JSON})
export NOTIFICATION_CONFIG_URI=$(jq -c -r '.notification_config_uri.value' ${OUT_JSON})
export PUBSUB_ERROR_TOPIC=$(jq -c -r '.pubsub_error_topic.value' ${OUT_JSON})
export SCHEDULER_JOB_NAME=$(jq -c -r '.scheduler_job.value' ${OUT_JSON})
```

### Default policy

```bash
DEFAULT_POLICY_FILE="<local JSON containing the default policy>"
```

Upload:

```bash
gs util cp ${DEFAULT_POLICY_FILE} ${DEFAULT_POLICY_URI}
```

### Secret

```bash
echo -e "Set secret content in: \n\thttps://console.cloud.google.com/security/secret-manager/secret/${NOTIFICATION_SECRET}/versions?project=${PROJECT_ID}"
```

### Notification config

Similar to [DEPLOY_EMAIL](../DEPLOY_EMAIL.md)

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
CFG_FILE=`mktemp`
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
CFG_FILE=`mktemp`
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
  --message="{\"key\":\"value\"}"
```

### Full Sampling

```bash
gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}"
```