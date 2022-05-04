# Creating and deploying infrastructure

## Definitions

Special variable, the target project ID:

```bash
export TARGET_PROJECT_ID="<WHERE THE SAMPLE WILL GO, NOT THE CURRENT PROJECT ID>"
```

```bash
export PROJECT_ID=$(gcloud config get-value core/project)
export PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format='get(projectNumber)')
export TARGET_PROJECT_NUMBER=$(gcloud projects describe ${TARGET_PROJECT_ID} --format='get(projectNumber)')
export REGION="europe-west3"
export LOCATION="${REGION}"
export FUNCTION_NAME="bq-sampler"
export FUNCTION_SA="${FUNCTION_NAME}-sa"
export PUBSUB_CMD_TOPIC="${FUNCTION_NAME}-cmd"
export PUBSUB_ERROR_TOPIC="${FUNCTION_NAME}-error"
export SCHEDULER_JOB_NAME="cronjob-${FUNCTION_NAME}"
export POLICY_BUCKET_NAME="sample-policy-${PROJECT_NUMBER}"
export REQUEST_BUCKET_NAME="sample-request-${TARGET_PROJECT_NUMBER}"
export DEFAULT_POLICY_OBJECT_PATH="default-policy.json"
export DEFAULT_SAMPLING_LOCK_OBJECT_PATH="block-sampling"
export MONITORING_CHANNEL_NAME="${FUNCTION_NAME}-error-monitoring"
```

## Service Account

Create:

```bash
gcloud iam service-accounts create "${FUNCTION_SA}" \
  --project="${PROJECT_ID}" \
  --description="Service Account used by CloudFunction ${FUNCTION_NAME} to sample BigQuery"
```

### Add permissions

PubSub publisher:

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/pubsub.publisher"
```

BigQuery data read in hosting project:

```bash
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
```

BigQuery data editor in the target project:

```bash
gcloud projects add-iam-policy-binding ${TARGET_PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding ${TARGET_PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

Storage object view permissions:

```bash
gcloud projects add-iam-policy-binding ${TARGET_PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"
```

## Create storage buckets

### Policy

Create bucket:

```bash
gsutil mb -l ${LOCATION} -p ${PROJECT_ID} \
  gs://${POLICY_BUCKET_NAME}
```

Set IAM:

```bash
gsutil iam ch \
  "serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com:roles/storage.legacyBucketReader" \
  gs://${POLICY_BUCKET_NAME}
```

### Request

Create:

```bash
gsutil mb -l ${LOCATION} -p ${TARGET_PROJECT_ID} \
  gs://${REQUEST_BUCKET_NAME}
```

Set IAM:

```bash
gsutil iam ch \
  "serviceAccount:${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com:roles/storage.legacyBucketReader" \
  gs://${REQUEST_BUCKET_NAME}
```

## Create Pub/Sub

```bash
gcloud pubsub topics create "${PUBSUB_CMD_TOPIC}" \
  --project="${PROJECT_ID}"
gcloud pubsub topics create "${PUBSUB_ERROR_TOPIC}" \
  --project="${PROJECT_ID}"
```

## Create Cloud Scheduler

Set to run every day at 23:00 UTC:

```bash
gcloud beta scheduler jobs create pubsub ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --topic="${PUBSUB_CMD_TOPIC}" \
  --schedule="0 23 * * *" \
  --time-zone="Etc/UTC" \
  --message-body='{"type": "START"}' \
  --description="Cronjob to trigger BigQuery sampling to DataScience Environment"
```

### Manually triggering

```bash
gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}"
```

## [Deploy CloudFunction](DEPLOY.md)

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
  | jq  -r ".[] | select(.displayName == \"${FUNCTION_NAME}-error-monitoring\") | .name" \
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