# Deploy CloudFunction

Requires environment variables defined in [infrastructure](INFRASTRUCTURE.md).

## Run manual integration tests

```bash
rm -f integ_test.log
./bin/manual_integ_tests.sh \
  --log integ_test.log \
  --tgt-prj-id ${TARGET_PROJECT_ID} \
  --region ${REGION} \
  --function ${FUNCTION_NAME} \
  --sa-email ${FUNCTION_SA}@${PROJECT_ID}.iam.gserviceaccount.com \
  --pubsub-cmd ${PUBSUB_CMD_TOPIC} \
  --pubsub-error ${PUBSUB_ERROR_TOPIC} \
  --cronjob ${SCHEDULER_JOB_NAME} \
  --policy-bucket ${POLICY_BUCKET_NAME} \
  --request-bucket ${REQUEST_BUCKET_NAME}
```