////////////////////
// Global/General //
////////////////////

locals {
  request_bucket_name            = "${var.request_bucket_name_prefix}-${data.google_project.project.number}"
  bigquery_datatransfer_sa_email = "service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_service_account" "sampler_service_account" {
  account_id = var.sampler_service_account_email
}

data "google_service_account" "bigquery_datatransfer_service_account" {
  account_id = local.bigquery_datatransfer_sa_email
}

/////////////////////////////////
// Service Account Permissions //
/////////////////////////////////

// Create token

resource "google_service_account_iam_member" "sampler_service_account_cross_project_token" {
  service_account_id = data.google_service_account.sampler_service_account.id
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = data.google_service_account.bigquery_datatransfer_service_account.member
}

// BigQuery

resource "google_project_iam_member" "project_iam_bq" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/bigquery.jobUser",
  ])
  project = var.project_id
  role    = each.key
  member  = data.google_service_account.sampler_service_account.member
}

// BQ Transfer agent

resource "google_project_iam_member" "project_iam_bq_transfer_agent" {
  for_each = toset([
    data.google_service_account.bigquery_datatransfer_service_account.member,
    data.google_service_account.sampler_service_account.member,
  ])
  project = var.project_id
  role    = "roles/bigquerydatatransfer.serviceAgent"
  member  = each.key
}

// Service usage

resource "google_project_iam_member" "project_iam_service_usage" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = data.google_service_account.sampler_service_account.member
}

/////////
// GCS //
/////////

module "request_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  name       = local.request_bucket_name
  iam = {
    "roles/storage.legacyBucketReader" = [data.google_service_account.sampler_service_account.member]
    "roles/storage.objectViewer"       = [data.google_service_account.sampler_service_account.member]
  }
}

////////////
// PubSub //
////////////

module "pubsub_bq_notification" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = var.project_id
  name       = var.pubsub_bq_notification_topic_name
  iam = {
    "roles/pubsub.admin" = [data.google_service_account.sampler_service_account.member]
  }
}

