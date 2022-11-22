////////////////////
// Global/General //
////////////////////

locals {
  request_bucket_name = "${var.request_bucket_name_prefix}-${data.google_project.project.number}"
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
  member             = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}

// BigQuery

resource "google_project_iam_member" "project_iam_bq_data" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${var.sampler_service_account_email}"
}

resource "google_project_iam_member" "project_iam_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.sampler_service_account_email}"
}

// BQ Transfer agent

resource "google_project_iam_member" "project_iam_bq_transfer_agent" {
  for_each = toset([
    "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com",
    "serviceAccount:${var.sampler_service_account_email}",
  ])
  project = var.project_id
  role    = "roles/bigquerydatatransfer.serviceAgent"
  member  = each.key
}

// Service usage

resource "google_project_iam_member" "project_iam_service_usage" {
  project = var.project_id
  role    = "roles/serviceusage.serviceUsageConsumer"
  member  = "serviceAccount:${var.sampler_service_account_email}"
}

/////////
// GCS //
/////////

module "request_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  name       = local.request_bucket_name
  iam = {
    "roles/storage.legacyBucketReader" = ["serviceAccount:${var.sampler_service_account_email}"]
    "roles/storage.objectViewer"       = ["serviceAccount:${var.sampler_service_account_email}"]
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
    "roles/pubsub.admin" = ["serviceAccount:${var.sampler_service_account_email}"]
  }
}

