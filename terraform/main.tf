locals {
  project_id        = try(var.project_id)
  target_project_id = try(var.target_project_id)
  region            = try(var.region)
}

data "google_project" "project" {
  project_id = local.project_id
}
data "google_project" "target_project" {
  project_id = local.target_project_id
}

////////////////////
// Source Project //
////////////////////

resource "google_project_service" "services" {
  for_each                   = toset(var.services)
  project                    = local.project_id
  service                    = each.key
  disable_dependent_services = true
  disable_on_destroy         = true
  timeouts {
    create = "30m"
    update = "40m"
  }
}

resource "google_project_service" "target_services" {
  for_each                   = toset(var.target_services)
  project                    = local.target_project_id
  service                    = each.key
  disable_dependent_services = true
  disable_on_destroy         = true
  timeouts {
    create = "30m"
    update = "40m"
  }
}

module "function_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = local.project_id
  name         = var.function_service_account_name
  generate_key = false
  iam_project_roles = {
    "${local.project_id}" = [
      "roles/pubsub.publisher",
      "roles/bigquery.dataViewer",
      "roles/storage.objectViewer",
    ]
  }
  depends_on = [google_project_service.services]
}

module "notification_function_service_account" {
  source       = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/iam-service-account"
  project_id   = local.project_id
  name         = var.notification_function_service_account_name
  generate_key = false
  iam_project_roles = {
    "${local.project_id}" = [
      "roles/secretmanager.secretAccessor",
      "roles/storage.objectViewer",
    ]
  }
  depends_on = [google_project_service.services]
}

module "policy_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = local.project_id
  prefix     = var.policy_bucket_name_prefix
  name       = data.google_project.project.number
  iam = {
    "roles/storage.legacyBucketReader" = [
      "serviceAccount:${module.function_service_account.email}",
      "serviceAccount:${module.notification_function_service_account.email}",
    ]
  }
  depends_on = [google_project_service.services]
}

module "pubsub_cmd" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = local.project_id
  name       = var.pubsub_cmd_topic_name
  depends_on = [google_project_service.services]
}

module "pubsub_err" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/pubsub"
  project_id = local.project_id
  name       = var.pubsub_error_topic_name
  depends_on = [google_project_service.services]
}

locals {
  function_source_exclude = flatten([
    for in_file in var.function_bundle_exclude_list_files : split("\n", file(in_file))
  ])
}

module "sampler" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = local.project_id
  region          = local.region
  name            = var.function_name
  service_account = module.function_service_account.email
  function_config = {
    entry_point = var.function_handler
    runtime     = var.function_runtime
    instances   = 1
    memory      = 256
    timeout     = 180
  }
  environment_variables = {
    BQ_LOCATION                = local.region
    TARGET_PROJECT_ID          = local.target_project_id
    POLICY_BUCKET_NAME         = module.policy_bucket.name
    DEFAULT_POLICY_OBJECT_PATH = var.default_policy_object_path
    SAMPLING_LOCK_OBJECT_PATH  = var.sampling_lock_object_path
    CMD_TOPIC_NAME             = module.pubsub_cmd.id
    ERROR_TOPIC_NAME           = module.pubsub_err.id
    LOG_LEVEL                  = var.function_log_level
  }
  bucket_name = module.policy_bucket.name
  bundle_config = {
    source_dir  = "../"
    output_path = "dist/${var.function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  trigger_config = {
    event    = "google.pubsub.topic.publish"
    resource = module.pubsub_cmd.id
    retry    = null
  }
  depends_on = [google_project_service.services]
}

locals {
  notification_function_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}"
  notification_handler       = "${var.notification_function_handler_prefix}${lower(var.notification_function_type)}"
  // something like SMTP_CONFIG_URI="gs://my_policy_bucket/smtp_config.json"
  notification_config_uri    = "gs://${module.policy_bucket.name}/${lower(var.notification_function_type)}${var.notification_config_json_suffix}"
  notification_env_vars = {
    LOG_LEVEL = var.notification_function_log_level
    "${upper(var.notification_function_type)}_CONFIG_URI" = local.notification_config_uri
  }
  notification_secret_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}-secret"
}

module "notification" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = local.project_id
  region          = local.region
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
    source_dir  = "../"
    output_path = "dist/${local.notification_function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  trigger_config = {
    event    = "google.pubsub.topic.publish"
    resource = module.pubsub_cmd.id
    retry    = null
  }
  depends_on = [google_project_service.services]
}

resource "google_cloud_scheduler_job" "trigger_job" {
  name        = var.scheduler_name
  description = "Cronjob to trigger BigQuery sampling to Target Environment"
  schedule    = var.scheduler_cron_entry

  pubsub_target {
    topic_name = module.pubsub_cmd.id
    data       = base64encode(var.scheduler_data)
  }
  depends_on = [google_project_service.services]
}

resource "google_monitoring_notification_channel" "error_monitoring_channel" {
  display_name = var.monitoring_channel_name
  type         = "pubsub"
  labels = {
    topic = module.pubsub_err.id
  }
  depends_on = [google_project_service.services]
}

resource "google_monitoring_notification_channel" "notification_error_monitoring_channel" {
  display_name = var.notification_monitoring_channel_name
  type         = "email"
  labels = {
    email_address = var.notification_monitoring_email_address
  }
  depends_on = [google_project_service.services]
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
  depends_on            = [google_project_service.services]
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
  depends_on            = [google_project_service.services]
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
  depends_on            = [google_project_service.services]
}

module "notification-secret" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/secret-manager"
  project_id = local.project_id
  secrets = {
    "${local.notification_secret_name}" = [local.region]
  }
  versions = {
    "${local.notification_secret_name}" = {
      v1 = { enabled = true, data = "ADD YOUR SECRET CONTENT MANUALLY AND NOT HERE" }
    }
  }
  depends_on = [google_project_service.services]
}

////////////////////
// Target Project //
////////////////////

resource "google_project_iam_binding" "target_project_iam_bq_data" {
  project    = local.target_project_id
  role       = "roles/bigquery.dataEditor"
  members    = ["serviceAccount:${module.function_service_account.email}"]
  depends_on = [google_project_service.target_services]
}

resource "google_project_iam_binding" "target_project_iam_bq_job" {
  project    = local.target_project_id
  role       = "roles/bigquery.jobUser"
  members    = ["serviceAccount:${module.function_service_account.email}"]
  depends_on = [google_project_service.target_services]
}

resource "google_project_iam_binding" "target_project_iam_gcs_obj_viewer" {
  project    = local.target_project_id
  role       = "roles/storage.objectViewer"
  members    = ["serviceAccount:${module.function_service_account.email}"]
  depends_on = [google_project_service.target_services]
}

module "request_bucket" {
  source     = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/gcs"
  project_id = local.target_project_id
  prefix     = var.request_bucket_name_prefix
  name       = data.google_project.target_project.number
  iam = {
    "roles/storage.legacyBucketReader" = ["serviceAccount:${module.function_service_account.email}"]
  }
  depends_on = [google_project_service.target_services]
}

/////////////
// Outputs //
/////////////

output "region" {
  value = var.region
}

output "project_id" {
  value = local.project_id
}

output "target_project_id" {
  value = local.target_project_id
}

output "default_policy_uri" {
  value = "gs://${module.policy_bucket.name}/${var.default_policy_object_path}"
}

output "function_name" {
  value = module.sampler.function_name
}

output "notification_type" {
  value = var.notification_function_type
}

output "notification_function_name" {
  value = module.notification.function_name
}

output "notification_secret" {
  value = local.notification_secret_name
}

output "notification_config_uri" {
  value = local.notification_config_uri
}

output "pubsub_error_topic" {
  value = module.pubsub_err.topic.name
}

output "scheduler_job" {
  value = google_cloud_scheduler_job.trigger_job.name
}
