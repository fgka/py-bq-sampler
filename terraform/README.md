# Using Terraform To Deploy

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

## Enable APIs

### Source

```bash
gcloud services enable \
  artifactregistry.googleapis.com \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
  cloudbuild.googleapis.com \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  pubsub.googleapis.com \
  run.googleapis.com \
  secretmanager.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"
```

### Target

```bash
gcloud services enable \
  bigquery.googleapis.com \
  bigquerydatatransfer.googleapis.com \
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

## (Preferred) [Deploy All From Root](./DEPLOY.md)

## (Optionally) [Deploy All Step-By-Step](./DEPLOY_MANUAL.md)
