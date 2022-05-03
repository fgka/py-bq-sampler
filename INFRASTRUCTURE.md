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
export LOCATION="europe-west3"
export FUNCTION_NAME="bq-sampler"
export FUNCTION_SA="${FUNCTION_NAME}-sa"
export PUBSUB_CMD_TOPIC="${FUNCTION_NAME}-cmd"
export PUBSUB_ERROR_TOPIC="${FUNCTION_NAME}-error"
export SCHEDULER_JOB_NAME="cronjob-${FUNCTION_NAME}"
export POLICY_BUCKET_NAME="sample-policy-${PROJECT_NUMBER}"
export REQUEST_BUCKET_NAME="sample-request-${TARGET_PROJECT_NUMBER}"
export DEFAULT_POLICY_OBJECT_PATH="default-policy.json"
```

## Service Account

Create:

```bash
gcloud iam service-accounts create "${FUNCTION_SA}" \
  --project="${PROJECT_ID}" \
  --description="Service Account used by CloudFunction ${FUNCTION_NAME} to sample BigQuery"
```

### Add permissions

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

```bash
gcloud beta scheduler jobs create pubsub ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --topic="${PUBSUB_CMD_TOPIC}" \
  --schedule="0 8 * * *" \
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

## Create Cloud Function

Environment variables, see [process_request.py](./bq_sampler/process_request.py):

```bash
unset ENV_VARS
set -a ENV_VARS
ENV_VARS+=("TARGET_PROJECT_ID=${TARGET_PROJECT_ID}")
ENV_VARS+=("POLICY_BUCKET_NAME=${POLICY_BUCKET_NAME}")
ENV_VARS+=("DEFAULT_POLICY_OBJECT_PATH=${DEFAULT_POLICY_OBJECT_PATH}")
ENV_VARS+=("REQUEST_BUCKET_NAME=${REQUEST_BUCKET_NAME}")
ENV_VARS+=("CMD_TOPIC_NAME=${PUBSUB_CMD_TOPIC}")
ENV_VARS+=("ERROR_TOPIC_NAME=${PUBSUB_ERROR_TOPIC}")
ENV_VARS_COMMA=$(local IFS=","; echo "${ENV_VARS[*]}")
```

Deploy:

```bash
gcloud functions deploy "${FUNCTION_NAME}" \
  --project="${PROJECT_ID}" \
  --runtime="python39" \
  --entry-point="handler" \
  --service-account "${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --trigger-topic="${PUBSUB_CMD_TOPIC}" \
  --set-env-vars="${ENV_VARS_COMMA}"
```

### Check logs

```bash
gcloud logging read \
  --project="${PROJECT_ID}" \
  "resource.labels.function_name=${FUNCTION_NAME} severity>=CRITICAL"
```

## Create code alerts

```bash
gcloud beta monitoring channels create \
    --description="CloudFunction ${FUNCTION_NAME} Errors" \
    --display-name="${FUNCTION_NAME}-error-monitoring" \
    --type=pubsub \
    --channel-labels="topic=${PUBSUB_ERROR_TOPIC}"
```

### Generate policies

```bash
ERROR_POLICY_JSON=`mktemp`
sed -e "s/@@FUNCTION_NAME@@/${FUNCTION_NAME}/g" \
  ./gcp_resources/alert-function-error-policy.json.tmpl \
  > ${ERROR_POLICY_JSON}

NOT_EXEC_POLICY_JSON=`mktemp`
sed -e "s/@@FUNCTION_NAME@@/${FUNCTION_NAME}/g" \
  ./gcp_resources/alter-function-not-executed-policy.json.tmpl \
  > ${NOT_EXEC_POLICY_JSON}
```

### Apply monitoring policies

```bash

```