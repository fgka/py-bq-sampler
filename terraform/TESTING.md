# Testing End-To-End

It is assumed you followed the instructions in [README.md](./README.md) and deployed everything, 
either through the [root terraform](./DEPLOY.md) or [step-by-step](./DEPLOY_MANUAL.md).

## Set Variables

PubSub and Scheduler:
```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export PUBSUB_ERROR_TOPIC=$(jq -c -r '.source_1.value.pubsub_err.name' ${OUT_JSON})
export SCHEDULER_JOB_NAME=$(jq -c -r '.source_1.value.trigger_job.name' ${OUT_JSON})
rm -f ${OUT_JSON}
```

Functions:
```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export FUNCTION_NAME=$(jq -c -r '.source_3.value.sampler_function.name' ${OUT_JSON})
export NOTIFICATION_FUNCTION_NAME=$(jq -c -r '.source_3.value.notification_function.name' ${OUT_JSON})
rm -f ${OUT_JSON}
```

## Error In PubSub

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

## Full Sampling

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

## Integration Tests

### [Trigger BigQuery cloning jobs](./1_source/README.md#optionally-trigger-integration-test-data-transfer)

### Execute The Integration Tests

Go to [INTEG_TESTING.md](../code/INTEG_TESTING.md) and execute the shell script as indicated.
