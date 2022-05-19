# Deploy CloudFunction

Requires environment variables defined in [infrastructure](INFRASTRUCTURE.md).

## Create Cloud Function

Environment variables, see [process_request.py](./bq_sampler/process_request.py):

```bash
unset ENV_VARS
set -a ENV_VARS
ENV_VARS+=("BQ_LOCATION=${LOCATION}")
ENV_VARS+=("TARGET_PROJECT_ID=${TARGET_PROJECT_ID}")
ENV_VARS+=("POLICY_BUCKET_NAME=${POLICY_BUCKET_NAME}")
ENV_VARS+=("DEFAULT_POLICY_OBJECT_PATH=${DEFAULT_POLICY_OBJECT_PATH}")
ENV_VARS+=("REQUEST_BUCKET_NAME=${REQUEST_BUCKET_NAME}")
ENV_VARS+=("SAMPLING_LOCK_OBJECT_PATH=${DEFAULT_SAMPLING_LOCK_OBJECT_PATH}")
ENV_VARS+=("CMD_TOPIC_NAME=projects/${PROJECT_ID}/topics/${PUBSUB_CMD_TOPIC}")
ENV_VARS+=("ERROR_TOPIC_NAME=projects/${PROJECT_ID}/topics/${PUBSUB_ERROR_TOPIC}")
ENV_VARS+=("LOG_LEVEL=INFO")
ENV_VARS_COMMA=$(local IFS=","; echo "${ENV_VARS[*]}")
```

Deploy:

```bash
gcloud functions deploy "${FUNCTION_NAME}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --runtime="python39" \
  --entry-point="handler" \
  --service-account="${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --trigger-topic="${PUBSUB_CMD_TOPIC}" \
  --set-env-vars="${ENV_VARS_COMMA}"
```

## Check logs

```bash
gcloud logging read \
  "resource.labels.function_name=${FUNCTION_NAME} severity>=WARNING" \
  --project="${PROJECT_ID}"
```

## Create code alerts

```bash
gcloud beta monitoring channels create \
  --project="${PROJECT_ID}" \
  --description="CloudFunction ${FUNCTION_NAME} Errors" \
  --display-name="${MONITORING_CHANNEL_NAME}" \
  --type="pubsub" \
  --channel-labels="topic=projects/${PROJECT_ID}/topics/${PUBSUB_ERROR_TOPIC}"
```

Get channel full name:

```bash
MONITORING_CHANNEL_ID=$( \
  gcloud beta monitoring channels list --format=json \
  | jq  -r ".[] | select(.displayName == \"${MONITORING_CHANNEL_NAME}\") | .name" \
)
echo "Monitoring channel ID: ${MONITORING_CHANNEL_ID}"
```

### Generate policies

```bash
ERROR_POLICY_JSON=`mktemp`
sed -e "s/@@FUNCTION_NAME@@/${FUNCTION_NAME}/g" \
  ./gcp_resources/alert-function-error-policy.json.tmpl \
  > ${ERROR_POLICY_JSON}
echo "Error policy file: ${ERROR_POLICY_JSON}"

NOT_EXEC_POLICY_JSON=`mktemp`
sed -e "s/@@FUNCTION_NAME@@/${FUNCTION_NAME}/g" \
  ./gcp_resources/alter-function-not-executed-policy.json.tmpl \
  > ${NOT_EXEC_POLICY_JSON}
echo "Not executed policy file: ${NOT_EXEC_POLICY_JSON}"
```

### Apply monitoring policies

Error policy:

```bash
gcloud alpha monitoring policies create \
  --notification-channels="${MONITORING_CHANNEL_ID}" \
  --policy-from-file="${ERROR_POLICY_JSON}"
```

Not executed policy:

```bash
gcloud alpha monitoring policies create \
  --notification-channels="${MONITORING_CHANNEL_ID}" \
  --policy-from-file="${NOT_EXEC_POLICY_JSON}"
```

## Testing

End-to-End:

```bash
gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}"
```

## [Deploy Email Notification](DEPLOY_EMAIL.md)

## [Integration Tests](INTEG_TESTING.md)