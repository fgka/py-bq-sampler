# Deploy Email Notification CloudFunction

Requires environment variables defined in [infrastructure](INFRASTRUCTURE.md).

## Definitions

```bash
export EMAIL_FUNCTION_NAME="${FUNCTION_NAME}-notification"
export EMAIL_FUNCTION_SA="${EMAIL_FUNCTION_NAME}-sa"
export EMAIL_FUNCTION_SMTP_SECRET="${EMAIL_FUNCTION_NAME}-smtp-secret"
export EMAIL_FUNCTION_SMTP_CONFIG_GCS_URI="gs://${POLICY_BUCKET_NAME}/smtp_config.json"
export EMAIL_FUNCTION_SENDGRID_SECRET="${EMAIL_FUNCTION_NAME}-sendgrid-secret"
export EMAIL_FUNCTION_SENDGRID_CONFIG_GCS_URI="gs://${POLICY_BUCKET_NAME}/sendgrid_config.json"
export EMAIL_SUBJECT_LINE="[ALERT] There was an issue executing ${FUNCTION_NAME}"
export EMAIL_MONITORING_CHANNEL_NAME="${EMAIL_FUNCTION_NAME}-error-monitoring"
```

## Service Account

### Create

```bash
gcloud iam service-accounts create "${EMAIL_FUNCTION_SA}" \
  --project="${PROJECT_ID}" \
  --description="Service Account used by CloudFunction ${EMAIL_FUNCTION_NAME} to send notifications"
```

### Add Permissions

General IAM:

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role "roles/secretmanager.secretAccessor"
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

Policy bucket IAM:

```bash
gsutil iam ch \
  "serviceAccount:${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com:roles/storage.legacyBucketReader" \
  gs://${POLICY_BUCKET_NAME}
```

## SMTP

Below we assume Gmail as the SMTP provider, please check your SMTP server client setup for the appropriate values.

**NOTE**: If you are using Gmail and 2FA, you will need to set up an application password in the URL:
https://security.google.com/settings/security/apppasswords

### Save Password Secret

Setting secret:

```bash
echo -n "YOUR_SMTP_PASSWORD" \
  | gcloud secrets create "${EMAIL_FUNCTION_SMTP_SECRET}" \
    --data-file=-
```

Alternatively, through the web console:

```bash
gcloud secrets create "${EMAIL_FUNCTION_SMTP_SECRET}"

echo -e "Set secret content in: \n\thttps://console.cloud.google.com/security/secret-manager/secret/${EMAIL_FUNCTION_SMTP_SECRET}/versions?project=${PROJECT_ID}"
```

### Setup config file

For the file definition, see [config.py](./bq_sampler/entity/config.py):

Create config file (remember to change to your SMTP and authentication values):

```bash
SMTP_CFG_FILE=`mktemp`
cat > ${SMTP_CFG_FILE} << __END__
{
  "username": "me@example.com",
  "password_secret_name": "${EMAIL_FUNCTION_SMTP_SECRET}",
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
echo "Config file in ${SMTP_CFG_FILE}"
```

Upload file:

```bash
gsutil cp ${SMTP_CFG_FILE} ${EMAIL_FUNCTION_SMTP_CONFIG_GCS_URI}
```

### Deploy function

Environment variables, see [smtp.py](./bq_sampler/notification/smtp.py):

```bash
unset SMTP_ENV_VARS
set -a SMTP_ENV_VARS
SMTP_ENV_VARS+=("SMTP_CONFIG_URI=${EMAIL_FUNCTION_SMTP_CONFIG_GCS_URI}")
SMTP_ENV_VARS+=("LOG_LEVEL=INFO")
SMTP_ENV_VARS_COMMA=$(local IFS=","; echo "${SMTP_ENV_VARS[*]}")
```

Deploy:

```bash
gcloud functions deploy "${EMAIL_FUNCTION_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime="python39" \
  --entry-point="handler_smtp" \
  --service-account="${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --trigger-topic="${PUBSUB_ERROR_TOPIC}" \
  --set-env-vars="${SMTP_ENV_VARS_COMMA}"
```

### SendGrid

Using SendGrid to send emails. Remember to set the proper API key.

Don't forget to authenticate your sender first in https://docs.sendgrid.com/ui/sending-email/sender-verification.

### Save Password Secret

Setting secret:

```bash
echo -n "YOUR_SENDGRID_API_KEY" \
  | gcloud secrets create "${EMAIL_FUNCTION_SENDGRID_SECRET}" \
    --data-file=-
```

Alternatively, through the web console:

```bash
gcloud secrets create "${EMAIL_FUNCTION_SENDGRID_SECRET}"

echo -e "Set secret content in: \n\thttps://console.cloud.google.com/security/secret-manager/secret/${EMAIL_FUNCTION_SENDGRID_SECRET}/versions?project=${PROJECT_ID}"
```

### Setup config file

For the file definition, see [config.py](./bq_sampler/entity/config.py):

Create config file:

```bash
SENDGRID_CFG_FILE=`mktemp`
cat > ${SENDGRID_CFG_FILE} << __END__
{
  "api_key_secret_name": "${EMAIL_FUNCTION_SENDGRID_SECRET}",
  "subject": "${EMAIL_SUBJECT_LINE}",
  "sender": "me@example.com",
  "recipients": [
    "me@example.com",
    "you@example.com"
  ]
}
__END__
echo "Config file in ${SENDGRID_CFG_FILE}"
```

Upload file:

```bash
gsutil cp ${SENDGRID_CFG_FILE} ${EMAIL_FUNCTION_SENDGRID_CONFIG_GCS_URI}
```

### Deploy function

Environment variables, see [sendgrid.py](./bq_sampler/notification/sendgrid.py):

```bash
unset SENDGRID_ENV_VARS
set -a SENDGRID_ENV_VARS
SENDGRID_ENV_VARS+=("SENDGRID_CONFIG_URI=${EMAIL_FUNCTION_SENDGRID_CONFIG_GCS_URI}")
SENDGRID_ENV_VARS+=("LOG_LEVEL=INFO")
SENDGRID_ENV_VARS_COMMA=$(local IFS=","; echo "${SENDGRID_ENV_VARS[*]}")
```

Deploy:

```bash
gcloud functions deploy "${EMAIL_FUNCTION_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime="python39" \
  --entry-point="handler_sendgrid" \
  --service-account="${EMAIL_FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --trigger-topic="${PUBSUB_ERROR_TOPIC}" \
  --set-env-vars="${SENDGRID_ENV_VARS_COMMA}"
```

## Create code alerts

Remember to change the email address in `EMAIL_ADDRESS_ERROR_FN`:

```bash
EMAIL_ADDRESS_ERROR_FN="me@example.com"
gcloud beta monitoring channels create \
  --project="${PROJECT_ID}" \
  --description="CloudFunction ${EMAIL_FUNCTION_NAME} Errors" \
  --display-name="${EMAIL_MONITORING_CHANNEL_NAME}" \
  --type="email" \
  --channel-labels="email_address=${EMAIL_ADDRESS_ERROR_FN}"
```

Get channel full name:

```bash
MONITORING_CHANNEL_ID=$( \
  gcloud beta monitoring channels list --format=json \
  | jq  -r ".[] | select(.displayName == \"${EMAIL_MONITORING_CHANNEL_NAME}\") | .name" \
)
echo "Monitoring channel ID: ${MONITORING_CHANNEL_ID}"
```

### Generate policies

```bash
ERROR_POLICY_JSON=`mktemp`
sed -e "s/@@FUNCTION_NAME@@/${EMAIL_FUNCTION_NAME}/g" \
  ./gcp_resources/alert-function-error-policy.json.tmpl \
  > ${ERROR_POLICY_JSON}
echo "Error policy file: ${ERROR_POLICY_JSON}"
```

### Apply monitoring policies

Error policy:

```bash
gcloud alpha monitoring policies create \
  --notification-channels="${MONITORING_CHANNEL_ID}" \
  --policy-from-file="${ERROR_POLICY_JSON}"
```

## Testing

End-to-End:

```bash
gcloud pubsub topics publish ${PUBSUB_ERROR_TOPIC} \
  --message="{\"key\":\"value\"}"
```

## Check logs

```bash
gcloud logging read \
  "resource.labels.function_name=${EMAIL_FUNCTION_NAME} severity>=WARNING" \
  --project="${PROJECT_ID}"
```
