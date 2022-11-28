////////////////////
// Global/General //
////////////////////

data "google_project" "project" {
  project_id = var.project_id
}

//////////////////////
// Service Accounts //
//////////////////////

module "sampler_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = var.project_id
  name         = var.sampler_service_account_name
  generate_key = false
  iam_project_roles = {
    "${var.project_id}" = [
      "roles/bigquery.dataViewer",
      "roles/bigquerydatatransfer.serviceAgent",
    ]
  }
}

module "notification_function_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = var.project_id
  name         = var.notification_function_service_account_name
  generate_key = false
}

module "cmd_pubsub_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = var.project_id
  name         = var.pubsub_cmd_service_account_name
  generate_key = false
}

/////////
// GCS //
/////////

module "policy_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  prefix     = var.policy_bucket_name_prefix
  name       = data.google_project.project.number
  iam = {
    "roles/storage.objectViewer" = [
      "serviceAccount:${module.sampler_service_account.email}",
      "serviceAccount:${module.notification_function_service_account.email}",
    ]
    "roles/storage.legacyBucketReader" = [
      "serviceAccount:${module.sampler_service_account.email}",
      "serviceAccount:${module.notification_function_service_account.email}",
    ]
  }
}

////////////
// PubSub //
////////////

module "pubsub_cmd" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = var.project_id
  name       = var.pubsub_cmd_topic_name
  iam = {
    "roles/pubsub.publisher" = [module.sampler_service_account.iam_email]
  }
}

module "pubsub_err" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = var.project_id
  name       = var.pubsub_error_topic_name
  iam = {
    "roles/pubsub.publisher" = [module.sampler_service_account.iam_email]
  }
}

///////////////
// Scheduler //
///////////////

resource "google_cloud_scheduler_job" "trigger_job" {
  name        = var.scheduler_name
  description = "Cronjob to trigger BigQuery sampling to Target Environment"
  schedule    = var.scheduler_cron_entry

  pubsub_target {
    topic_name = module.pubsub_cmd.id
    data       = base64encode(var.scheduler_data)
  }
}

/////////////////////////////
// Monitoring and Alerting //
/////////////////////////////

resource "google_monitoring_notification_channel" "error_monitoring_channel" {
  display_name = var.monitoring_channel_name
  type         = "pubsub"
  labels = {
    topic = module.pubsub_err.id
  }
}

resource "google_monitoring_notification_channel" "notification_error_monitoring_channel" {
  display_name = var.notification_monitoring_channel_name
  type         = "email"
  labels = {
    email_address = var.notification_monitoring_email_address
  }
}

//////////////////////
// Integ Tests Data //
//////////////////////

resource "google_bigquery_dataset" "integ_test_datasets" {
  count         = var.create_integ_test_data ? length(var.integ_test_datasets) : 0
  project       = var.project_id
  dataset_id    = var.integ_test_datasets[count.index]
  friendly_name = "${var.integ_test_datasets[count.index]}_clone"
  description   = "Clone from ${var.integ_tests_project_id}:${var.integ_test_datasets[count.index]}}"
  location      = var.region
  access {
    role          = "OWNER"
    user_by_email = module.sampler_service_account.email
  }
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
}

resource "google_bigquery_data_transfer_config" "integ_test_data_transfer" {
  count                  = length(google_bigquery_dataset.integ_test_datasets)
  project                = var.project_id
  location               = google_bigquery_dataset.integ_test_datasets[count.index].location
  data_source_id         = "cross_region_copy"
  display_name           = "Cloning job for ${google_bigquery_dataset.integ_test_datasets[count.index].dataset_id}"
  destination_dataset_id = google_bigquery_dataset.integ_test_datasets[count.index].dataset_id
  params = {
    source_dataset_id           = google_bigquery_dataset.integ_test_datasets[count.index].dataset_id
    source_project_id           = var.integ_tests_project_id
    overwrite_destination_table = true
  }
  schedule = "every monday 00:00"
  schedule_options {
    disable_auto_scheduling = true // turns it into a manual transfer
  }
  depends_on = [google_project_iam_member.schedule_permissions]
}

resource "google_project_iam_member" "schedule_permissions" {
  count   = var.create_integ_test_data ? 1 : 0
  project = var.project_id
  role    = "roles/iam.serviceAccountShortTermTokenMinter"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}
