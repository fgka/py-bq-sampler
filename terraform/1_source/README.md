# Using Terraform to deploy Infrastructure for Functions

Start at the parent [../README.md](../README.md)

## Enable APIs

```bash
gcloud services enable \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  cloudbuild.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  pubsub.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"
```

## Init

```bash
terraform init
```

## Definitions

### Without integration test data

```bash
export CREATE_INTEG_TESTS="false" 
```

### With integration test data

```bash
export CREATE_INTEG_TESTS="true" 
```

### Terraform Arguments

```bash
TERRAFORM_VAR_ARGS="-var \"project_id=${PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"region=${REGION}\""
TERRAFORM_VAR_ARGS+=" -var \"notification_monitoring_email_address=${ERROR_NOTIFICATION_EMAIL_ADDRESS}\""
TERRAFORM_VAR_ARGS+=" -var \"create_integ_test_data=${CREATE_INTEG_TESTS}\""

echo "Terraform arguments: ${TERRAFORM_VAR_ARGS}"
```
## Plan and Apply

```bash
TMP=$(mktemp)
echo ${TERRAFORM_VAR_ARGS} \
  | xargs terraform plan -out ${TMP} \
  && terraform apply ${TMP} && rm -f ${TMP}
```

## (Optionally) Trigger Integration Test Data Transfer

This may take multiple minutes, be patient:
```bash
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

jq -r '.integ_test_data_transfer.value[].name' ${OUT_JSON} \
  | while read TRANSFER_NAME
    do
      echo "Triggering transfer config: ${TRANSFER_NAME}"
      bq mk --transfer_run \
        --location=${REGION} \
        --project_id=${PROJECT_ID} \
        --run_time=$(date -u +%FT%TZ) \
        ${TRANSFER_NAME}
    done
rm -f ${OUT_JSON}
```

## [Deploy Target Project](../2_target/README.md)
