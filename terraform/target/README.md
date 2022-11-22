# Using Terraform to deploy all

Start at the parent [../README.md](../README.md)

## Definitions

Manually set:

```bash
export SAMPLER_SERVICE_ACCOUNT_EMAIL="<WHERE THE SAMPLE WILL GO, NOT THE CURRENT PROJECT ID>"
```

Check:

```bash
echo "Sampler service account: ${SAMPLER_SERVICE_ACCOUNT_EMAIL}"
```

Or go to [source](../source/README.md)'s [Add missing bits](../source/README.md#add-missing-bits).

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
terraform plan \
  -out ${TMP} \
  -var "project_id=${TARGET_PROJECT_ID}" \
  -var "region=${REGION}" \
  -var "sampler_service_account_email=${SAMPLER_SERVICE_ACCOUNT_EMAIL}" \
  && terraform apply ${TMP} && rm -f ${TMP}
```

## Return to [Source](../source/README.md)'s [Re-Plan and Apply Source](../source/README.md#re-plan-and-apply)
