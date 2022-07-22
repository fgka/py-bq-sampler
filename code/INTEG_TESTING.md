# Deploy CloudFunction

Requires environment variables defined in [infrastructure](INFRASTRUCTURE.md).

## Importing test data

We use 2 publicly available datasets, namely:

* `bigquery-public-data.census_bureau_acs`
* `bigquery-public-data.new_york_taxi_trips`

These cannot be used directly because the integration tests need some extra permissions that are not broadly given.
Therefore, we need a local copy.

### Create tables to be cloned

The clone datasets must be created ahead of the transfer and in the target location too. 

```bash
unset DATASET_ID_LST
set -a DATASET_ID_LST
DATASET_ID_LST+=("census_bureau_acs")
DATASET_ID_LST+=("new_york_taxi_trips")
for DS_ID in ${DATASET_ID_LST[@]}
do
    bq mk \
      --dataset \
      --project_id=${PROJECT_ID} \
      --data_location=${REGION} \
      ${DS_ID}
done
```

### Clone data sets locally

For more details refer to the
[Enabling the BigQuery Data Transfer Service](https://cloud.google.com/bigquery-transfer/docs/enable-transfer-service)
and [Copy datasets](https://cloud.google.com/bigquery/docs/copying-datasets)
documentation pages.

```bash
SRC_PROJECT_ID="bigquery-public-data"
unset DATASET_ID_LST
set -a DATASET_ID_LST
DATASET_ID_LST+=("census_bureau_acs")
DATASET_ID_LST+=("new_york_taxi_trips")

for DS_ID in ${DATASET_ID_LST[@]}
do
    bq mk --transfer_config \
      --data_source=cross_region_copy \
      --project_id=${PROJECT_ID} \
      --target_dataset=${DS_ID} \
      --location=${REGION} \
      --display_name="Clone from ${SRC_PROJECT_ID}:${DS_ID}" \
      --params="{
        \"source_project_id\":\"${SRC_PROJECT_ID}\",
        \"source_dataset_id\":\"${DS_ID}\",
        \"overwrite_destination_table\":\"true\"
      }"
    TRANSFER_NAME=$(bq ls \
      --transfer_config \
      --transfer_location=${REGION} \
      --project_id=${PROJECT_ID} \
      --format=json \
      | jq --arg ds_id ${DS_ID} -r '.[] | select(.destinationDatasetId==$ds_id) | .name')
    bq mk --transfer_run \
      --location=${REGION} \
      --project_id=${PROJECT_ID} \
      --run_time=$(date -u +%FT%TZ) \
      ${TRANSFER_NAME}
      
done
```

**NOTE**: This can take some time to finish (the shell is released ahead of finishing the actual transfer).
Typical times vary between 15 and 30 minutes to finish. 

## Run manual integration tests

Without deployment:

```bash
rm -f integ_test.log
./bin/manual_integ_tests.sh \
  --no-deploy \
  --log integ_test.log \
  --prj-id ${PROJECT_ID} \
  --tgt-prj-id ${TARGET_PROJECT_ID} \
  --region ${REGION} \
  --function ${FUNCTION_NAME} \
  --cronjob ${SCHEDULER_JOB_NAME} \
  --policy-bucket ${POLICY_BUCKET_NAME} \
  --request-bucket ${REQUEST_BUCKET_NAME}
```

With deployment:

```bash
rm -f integ_test.log
./bin/manual_integ_tests.sh \
  --log integ_test.log \
  --prj-id ${PROJECT_ID} \
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