////////////////////
// Global/General //
////////////////////

locals {
  request_bucket_name             = "${var.request_bucket_name_prefix}-${data.google_project.project.number}"
  bigquery_datatransfer_sa_email  = "service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
  bigquery_datatransfer_sa_member = "serviceAccount:${local.bigquery_datatransfer_sa_email}"
  sampler_service_account_member  = "serviceAccount:${var.sampler_service_account_email}"
  service_account_member_map = tomap({
    sampler     = local.sampler_service_account_member
    bq_transfer = local.bigquery_datatransfer_sa_member
  })
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_service_account" "sampler_service_account" {
  account_id = var.sampler_service_account_email
}

/////////////////////////////////
// Service Account Permissions //
/////////////////////////////////

// Create token

resource "google_service_account_iam_member" "sampler_service_account_cross_project_token" {
  service_account_id = data.google_service_account.sampler_service_account.id
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = local.bigquery_datatransfer_sa_member
}

// BigQuery

resource "google_project_iam_member" "project_iam_bq" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/bigquery.jobUser",
  ])
  project = var.project_id
  role    = each.key
  member  = local.sampler_service_account_member
}

// BQ Transfer agent

resource "google_project_iam_member" "project_iam_bq_transfer_agent" {
  for_each = local.service_account_member_map
  project  = var.project_id
  role     = "roles/bigquerydatatransfer.serviceAgent"
  member   = each.value
}

// Service usage

resource "google_project_iam_member" "project_iam_service_usage" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = local.sampler_service_account_member
}

/////////
// GCS //
/////////

module "request_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  name       = local.request_bucket_name
  iam = {
    "roles/storage.legacyBucketReader" = [local.sampler_service_account_member]
    "roles/storage.objectViewer"       = [local.sampler_service_account_member]
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
    "roles/pubsub.admin" = [local.sampler_service_account_member]
  }
}

