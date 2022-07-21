locals {
  // Cloud Functions
  function_source_exclude = flatten([
    for in_file in var.function_bundle_exclude_list_files : split("\n", file(in_file))
  ])
  notification_function_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}"
  notification_handler       = "${var.notification_function_handler_prefix}${lower(var.notification_function_type)}"
  // something like SMTP_CONFIG_URI="gs://my_policy_bucket/smtp_config.json"
  notification_config_uri = "gs://${module.policy_bucket.name}/${lower(var.notification_function_type)}${var.notification_config_json_suffix}"
  notification_env_vars = {
    LOG_LEVEL                                             = var.notification_function_log_level
    "${upper(var.notification_function_type)}_CONFIG_URI" = local.notification_config_uri
  }
  notification_secret_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}-secret"
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_project" "target_project" {
  project_id = var.target_project_id
}

////////////////////
// Source Project //
////////////////////

// Service Accounts //

module "sampler_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = var.project_id
  name         = var.sampler_service_account_name
  generate_key = false
  iam_project_roles = {
    "${var.project_id}" = [
      "roles/pubsub.publisher",
      "roles/bigquery.dataViewer",
      "roles/storage.objectViewer",
    ]
  }
}

module "notification_function_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = var.project_id
  name         = var.notification_function_service_account_name
  generate_key = false
  iam_project_roles = {
    "${var.project_id}" = [
      "roles/secretmanager.secretAccessor",
      "roles/storage.objectViewer",
    ]
  }
}

// GCS //

module "policy_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.project_id
  prefix     = var.policy_bucket_name_prefix
  name       = data.google_project.project.number
  iam = {
    "roles/storage.legacyBucketReader" = [
      "serviceAccount:${module.sampler_service_account.email}",
      "serviceAccount:${module.notification_function_service_account.email}",
    ]
  }
}

// PubSub //

module "pubsub_cmd" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = var.project_id
  name       = var.pubsub_cmd_topic_name
}

module "pubsub_err" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = var.project_id
  name       = var.pubsub_error_topic_name
}

// Cloud Functions //

module "sampler" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = var.project_id
  region          = var.region
  name            = var.sampler_function_name
  service_account = module.sampler_service_account.email
  function_config = {
    entry_point = var.sampler_function_handler
    runtime     = var.function_runtime
    instances   = 1
    memory      = 256
    timeout     = 180
  }
  environment_variables = {
    BQ_LOCATION                = var.region
    TARGET_PROJECT_ID          = var.target_project_id
    POLICY_BUCKET_NAME         = module.policy_bucket.name
    DEFAULT_POLICY_OBJECT_PATH = var.default_policy_object_path
    SAMPLING_LOCK_OBJECT_PATH  = var.sampling_lock_object_path
    CMD_TOPIC_NAME             = module.pubsub_cmd.id
    ERROR_TOPIC_NAME           = module.pubsub_err.id
    LOG_LEVEL                  = var.sampler_function_log_level
  }
  bucket_name = module.policy_bucket.name
  bundle_config = {
    source_dir  = "../code"
    output_path = "dist/${var.sampler_function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  trigger_config = {
    event    = "google.pubsub.topic.publish"
    resource = module.pubsub_cmd.id
    retry    = null
  }
}

module "notification" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = var.project_id
  region          = var.region
  name            = local.notification_function_name
  service_account = module.notification_function_service_account.email
  function_config = {
    entry_point = local.notification_handler
    runtime     = var.function_runtime
    instances   = 1
    memory      = 256
    timeout     = 180
  }
  environment_variables = local.notification_env_vars
  bucket_name           = module.policy_bucket.name
  bundle_config = {
    source_dir  = "../code"
    output_path = "dist/${local.notification_function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  trigger_config = {
    event    = "google.pubsub.topic.publish"
    resource = module.pubsub_err.id
    retry    = null
  }
}

// Scheduler //

resource "google_cloud_scheduler_job" "trigger_job" {
  name        = var.scheduler_name
  description = "Cronjob to trigger BigQuery sampling to Target Environment"
  schedule    = var.scheduler_cron_entry

  pubsub_target {
    topic_name = module.pubsub_cmd.id
    data       = base64encode(var.scheduler_data)
  }
}

// Monitoring and Alerting //

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

// based on gcp_resources/alert-function-error-policy.json.tmpl
resource "google_monitoring_alert_policy" "alert_error_log_policy" {
  display_name = "${module.sampler.function_name}-error-monitoring"
  documentation {
    content   = "Alerts for ${module.sampler.function_name} execution errors"
    mime_type = "text/markdown"
  }
  alert_strategy {
    notification_rate_limit {
      period = "3600s"
    }
    auto_close = "604800s"
  }
  combiner = "OR"
  conditions {
    display_name = "Log severity >= ${var.monitoring_alert_severity} for function ${module.sampler.function_name}"
    condition_matched_log {
      filter = "resource.labels.function_name=\"${module.sampler.function_name}\"\nseverity>=\"${var.monitoring_alert_severity}\""
    }
  }
  notification_channels = [google_monitoring_notification_channel.error_monitoring_channel.id]
}

// based on gcp_resources/alter-function-not-executed-policy.json.tmpl
resource "google_monitoring_alert_policy" "alert_not_executed_policy" {
  display_name = "${module.sampler.function_name}-not-executed-monitoring"
  documentation {
    content   = "Alert if function ${module.sampler.function_name} is not executed within a day"
    mime_type = "text/markdown"
  }
  combiner = "OR"
  conditions {
    display_name = "Executions for ${module.sampler.function_name} [COUNT]"
    condition_threshold {
      filter          = "metric.type=\"cloudfunctions.googleapis.com/function/execution_count\" resource.type=\"cloud_function\" resource.label.\"function_name\"=\"${module.sampler.function_name}\""
      threshold_value = 1
      trigger {
        count = 1
      }
      duration   = "3600s"
      comparison = "COMPARISON_LT"
      aggregations {
        alignment_period     = "86400s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_COUNT"
      }
    }
  }
  notification_channels = [google_monitoring_notification_channel.error_monitoring_channel.id]
}

// based on gcp_resources/alert-function-error-policy.json.tmpl
resource "google_monitoring_alert_policy" "notification_alert_error_log_policy" {
  display_name = "${module.notification.function_name}-error-monitoring"
  documentation {
    content   = "Alerts for ${module.notification.function_name} execution errors"
    mime_type = "text/markdown"
  }
  alert_strategy {
    notification_rate_limit {
      period = "3600s"
    }
    auto_close = "604800s"
  }
  combiner = "OR"
  conditions {
    display_name = "Log severity >= ${var.monitoring_alert_severity} for function ${module.notification.function_name}"
    condition_matched_log {
      filter = "resource.labels.function_name=\"${module.notification.function_name}\"\nseverity>=\"${var.monitoring_alert_severity}\""
    }
  }
  notification_channels = [google_monitoring_notification_channel.notification_error_monitoring_channel.id]
}

// Secret Manager //

module "notification_secret" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/secret-manager"
  project_id = var.project_id
  secrets = {
    "${local.notification_secret_name}" = [var.region]
  }
  versions = {
    "${local.notification_secret_name}" = {
      v1 = { enabled = true, data = "ADD YOUR SECRET CONTENT MANUALLY AND NOT HERE" }
    }
  }
}

////////////////////
// Target Project //
////////////////////

// X-Project Permissions //

resource "google_project_iam_binding" "target_project_iam_bq_data" {
  project    = var.target_project_id
  role       = "roles/bigquery.dataEditor"
  members    = ["serviceAccount:${module.sampler_service_account.email}"]
}

resource "google_project_iam_binding" "target_project_iam_bq_job" {
  project    = var.target_project_id
  role       = "roles/bigquery.jobUser"
  members    = ["serviceAccount:${module.sampler_service_account.email}"]
}

resource "google_project_iam_binding" "target_project_iam_gcs_obj_viewer" {
  project    = var.target_project_id
  role       = "roles/storage.objectViewer"
  members    = ["serviceAccount:${module.sampler_service_account.email}"]
}

// GCS //

module "request_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = var.target_project_id
  prefix     = var.request_bucket_name_prefix
  name       = data.google_project.target_project.number
  iam = {
    "roles/storage.legacyBucketReader" = ["serviceAccount:${module.sampler_service_account.email}"]
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
  params                 = {
    source_dataset_id           = google_bigquery_dataset.integ_test_datasets[count.index].dataset_id
    source_project_id           = var.integ_tests_project_id
    overwrite_destination_table = true
  }
  schedule               = "every monday 00:00"
  schedule_options {
    disable_auto_scheduling = true // turns it into a manual transfer
  }
  depends_on = [ google_project_iam_member.schedule_permissions ]
}

resource "google_project_iam_member" "schedule_permissions" {
  count   = var.create_integ_test_data ? 1 : 0
  project = var.project_id
  role    = "roles/iam.serviceAccountShortTermTokenMinter"
  member  = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
}