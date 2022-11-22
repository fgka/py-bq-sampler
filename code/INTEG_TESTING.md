# Deploy CloudFunction

Requires deployment as indicated in [terraform](../terraform/README.md) *with* integration data set,
as indicated in [source](../terraform/source/README.md)'s [With integration test data](../terraform/source/README.md#with-integration-test-data).

## Run manual integration tests

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
MANUAL_INTEG_TESTS_ARGS+=" --pubsub-bq ${PUBSUB_BQ_NOTIFICATION_TOPIC_NAME}"
```

### Run Tests

Create custom script:
```bash
SCRIPT_TMP=$(mktemp)
echo "rm -f integ_test.log" > ${SCRIPT_TMP}
echo "./bin/manual_integ_tests.sh ${MANUAL_INTEG_TESTS_ARGS}" >> ${SCRIPT_TMP}
chmod 755 ${SCRIPT_TMP}
echo "Script in: ${SCRIPT_TMP}"
cat ${SCRIPT_TMP}
```

```bash
${SCRIPT_TMP}
```
