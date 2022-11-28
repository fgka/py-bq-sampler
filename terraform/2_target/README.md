# Using Terraform to deploy all

Start at the parent [../README.md](../README.md)

## Definitions


```bash
pushd ../1_source
OUT_JSON=$(mktemp)
terraform output -json > ${OUT_JSON}
echo "Terraform output in ${OUT_JSON}"

export SAMPLER_SERVICE_ACCOUNT_EMAIL=$(jq -c -r '.sampler_function_service_account.value.email' ${OUT_JSON})
rm -f ${OUT_JSON}
popd 
```

### Terraform Arguments

```bash
TERRAFORM_VAR_ARGS="-var \"project_id=${TARGET_PROJECT_ID}\""
TERRAFORM_VAR_ARGS+=" -var \"region=${REGION}\""
TERRAFORM_VAR_ARGS+=" -var \"sampler_service_account_email=${SAMPLER_SERVICE_ACCOUNT_EMAIL}\""

echo "Terraform arguments: ${TERRAFORM_VAR_ARGS}"
```

## Enable APIs

```bash
gcloud services enable \
  bigquery.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  pubsub.googleapis.com \
  storage.googleapis.com \
  --project="${TARGET_PROJECT_ID}"
```

## Init

```bash
terraform init
```

## Plan and Apply

```bash
TMP=$(mktemp)
echo ${TERRAFORM_VAR_ARGS} \
  | xargs terraform plan -out ${TMP} \
  && terraform apply ${TMP} && rm -f ${TMP}
```

## Return to [Source](../3_source/README.md)'s [Re-Plan and Apply Source](../3_source/README.md#re-plan-and-apply)
