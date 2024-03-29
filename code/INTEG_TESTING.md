# Deploy CloudFunction

Requires deployment as indicated in [terraform](../terraform/README.md) 
*with* integration data set 
and deployed using the [root terraform deployment](../terraform/DEPLOY.md).

## Run manual integration tests

### Definitions

```bash
pushd ../terraform
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
popd
echo "Terraform output in ${OUT_JSON}"

export SAMPLER_SERVICE_ACCOUNT_EMAIL=$(jq -c -r '.source_1.value.sampler_function_service_account.email' ${OUT_JSON})
export PUBSUB_CMD_TOPIC=$(jq -c -r '.source_1.value.pubsub_cmd.name' ${OUT_JSON})
export PUBSUB_ERROR_TOPIC=$(jq -c -r '.source_1.value.pubsub_err.name' ${OUT_JSON})
export SCHEDULER_JOB_NAME=$(jq -c -r '.source_1.value.trigger_job.name' ${OUT_JSON})

export PUBSUB_BQ_NOTIFICATION_TOPIC=$(jq -c -r '.target_2.value.pubsub_bq_notification.id' ${OUT_JSON})

export FUNCTION_NAME=$(jq -c -r '.source_3.value.sampler_function.name' ${OUT_JSON})
export POLICY_BUCKET_NAME=$(jq -c -r '.source_3.value.sampler_function.service_config[0].environment_variables.POLICY_BUCKET_NAME' ${OUT_JSON})
export REQUEST_BUCKET_NAME=$(jq -c -r '.source_3.value.sampler_function.service_config[0].environment_variables.REQUEST_BUCKET_NAME' ${OUT_JSON})
export BQ_TARGET_REGION=$(jq -c -r '.source_3.value.sampler_function.service_config[0].environment_variables.BQ_TARGET_LOCATION' ${OUT_JSON})

rm -f ${OUT_JSON}
```



### Arguments

General:
```bash
MANUAL_INTEG_TESTS_ARGS="--log integ_test.log"
MANUAL_INTEG_TESTS_ARGS+=" --prj-id ${PROJECT_ID}"
MANUAL_INTEG_TESTS_ARGS+=" --tgt-prj-id ${TARGET_PROJECT_ID}"
MANUAL_INTEG_TESTS_ARGS+=" --region ${REGION}"
MANUAL_INTEG_TESTS_ARGS+=" --function ${FUNCTION_NAME}"
MANUAL_INTEG_TESTS_ARGS+=" --cronjob ${SCHEDULER_JOB_NAME}"
MANUAL_INTEG_TESTS_ARGS+=" --policy-bucket ${POLICY_BUCKET_NAME}"
MANUAL_INTEG_TESTS_ARGS+=" --request-bucket ${REQUEST_BUCKET_NAME}"
MANUAL_INTEG_TESTS_ARGS+=" --bq-region ${BQ_TARGET_REGION}"
```

Without deployment:
```bash
MANUAL_INTEG_TESTS_ARGS+=" --no-deploy"
```

With deployment:
```bash
MANUAL_INTEG_TESTS_ARGS+=" --sa-email ${SAMPLER_SERVICE_ACCOUNT_EMAIL}"
MANUAL_INTEG_TESTS_ARGS+=" --pubsub-cmd ${PUBSUB_CMD_TOPIC}"
MANUAL_INTEG_TESTS_ARGS+=" --pubsub-error ${PUBSUB_ERROR_TOPIC}"
MANUAL_INTEG_TESTS_ARGS+=" --pubsub-bq ${PUBSUB_BQ_NOTIFICATION_TOPIC}"
```

Check:
```bash
echo ${MANUAL_INTEG_TESTS_ARGS}
```

### Run Tests

Create custom script:
```bash
SCRIPT_TMP=$(mktemp)
echo "rm -f integ_test.log" > ${SCRIPT_TMP}
CURR_DIR=$(cd "$(dirname .)"; pwd)
echo "${CURR_DIR}/bin/manual_integ_tests.sh ${MANUAL_INTEG_TESTS_ARGS}" >> ${SCRIPT_TMP}
chmod 755 ${SCRIPT_TMP}
echo "Script in: ${SCRIPT_TMP}"
cat ${SCRIPT_TMP}
```

```bash
${SCRIPT_TMP}
```
