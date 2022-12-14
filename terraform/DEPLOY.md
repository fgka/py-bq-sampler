# Deploying All Using The Root Module

It is assumed you followed the instructions in [README.md](./README.md).

## Definitions

### Without Integration Test Data

```bash
export CREATE_INTEG_TESTS="false" 
```

### With Integration Test Data

```bash
export CREATE_INTEG_TESTS="true" 
```

### Terraform Arguments

```bash
TERRAFORM_VAR_ARGS="-var \"project_id=${PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"target_project_id=${TARGET_PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"region=${REGION}\""
TERRAFORM_VAR_ARGS+=" -var \"bq_target_location=${BQ_TARGET_REGION}\""
TERRAFORM_VAR_ARGS+=" -var \"notification_monitoring_email_address=${ERROR_NOTIFICATION_EMAIL_ADDRESS}\""
TERRAFORM_VAR_ARGS+=" -var \"create_integ_test_data=${CREATE_INTEG_TESTS}\""

echo "Terraform arguments: ${TERRAFORM_VAR_ARGS}"
```

## Plan And Apply

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

jq -r '.source_1.value.integ_test_data_transfer[].name' ${OUT_JSON} \
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

## [Testing](./TESTING.md)
