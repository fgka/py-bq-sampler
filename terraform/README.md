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
export BQ_TARGET_REGION="${REGION}"
```

Check:

```bash
echo "Main project: ${PROJECT_ID}@${REGION}"
echo "Target project: ${TARGET_PROJECT_ID}@${REGION}"
echo "BigQuery target region: ${BQ_TARGET_REGION}"
echo "Error notification email: ${ERROR_NOTIFICATION_EMAIL_ADDRESS}"
```

## [Deploy Source Project Supporting Infrastructure](1_source/README.md)

## [Deploy Target Project](2_target/README.md)

## [Deploy Source Project Sampler](3_source/README.md)

## Testing

### Set variables

PubSub and Scheduler:
```bash
pushd ./1_source
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export PUBSUB_ERROR_TOPIC=$(jq -c -r '.pubsub_err.value.name' ${OUT_JSON})
export SCHEDULER_JOB_NAME=$(jq -c -r '.trigger_job.value.name' ${OUT_JSON})
rm -f ${OUT_JSON}
popd 
```

Functions:
```bash
pushd ./3_source
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export FUNCTION_NAME=$(jq -c -r '.sampler_function.value.name' ${OUT_JSON})
export NOTIFICATION_FUNCTION_NAME=$(jq -c -r '.notification_function.value.name' ${OUT_JSON})
rm -f ${OUT_JSON}
popd 
```

### Error in PubSub

Send error message:
```bash
PUBSUB_SENT_DATE=$(date -u -v-1M +"%Y-%m-%dT%H:%M:%SZ")
LOG_QUERY_SUFFIX="severity>=INFO timestamp>=\"${PUBSUB_SENT_DATE}\""
JQ_QUERY='.[] | (.timestamp + " - " + .textPayload)'
gcloud pubsub topics publish ${PUBSUB_ERROR_TOPIC} \
  --project="${PROJECT_ID}" \
  --message="{\"key\":\"value\"}"
```

Check logs:
```bash
gcloud logging read \
  "resource.labels.function_name=${NOTIFICATION_FUNCTION_NAME} ${LOG_QUERY_SUFFIX}" \
  --format=json \
  --project="${PROJECT_ID}" \
  | jq -cr ${JQ_QUERY} \
  | sort
```

### Full Sampling

Trigger sampling:
```bash
SCHED_TRIGGER_DATE=$(date -u -v-1M +"%Y-%m-%dT%H:%M:%SZ")
LOG_QUERY_SUFFIX="severity>=INFO timestamp>=\"${SCHED_TRIGGER_DATE}\""
JQ_QUERY='.[] | (.timestamp + " - " + .textPayload)'
gcloud beta scheduler jobs run ${SCHEDULER_JOB_NAME} \
  --project="${PROJECT_ID}" \
  --location="${REGION}"
```

Check logs:
```bash
gcloud logging read \
  "(resource.labels.function_name=${FUNCTION_NAME} OR resource.labels.service_name=${FUNCTION_NAME}) ${LOG_QUERY_SUFFIX}" \
  --format=json \
  --project="${PROJECT_ID}" \
  | jq -cr ${JQ_QUERY} \
  | sort
```

## Integration tests

### [Trigger BigQuery cloning jobs](./1_source/README.md#optionally-trigger-integration-test-data-transfer)

### Execute the integration tests

Go to [INTEG_TESTING.md](../code/INTEG_TESTING.md) and execute the shell script as indicated.
