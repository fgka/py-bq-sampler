////////////////////
// Global/General //
////////////////////

locals {
  // from 2_target project
  bq_target_location = var.bq_target_location != null ? var.bq_target_location : var.region
  // notification function
  notification_function_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}"
  notification_handler       = "${var.notification_function_handler_prefix}${lower(var.notification_function_type)}"
  // something like SMTP_CONFIG_URI="gs://my_policy_bucket/smtp_config.json"
  notification_config_uri  = "gs://${data.google_storage_bucket.policy_bucket.name}/${lower(var.notification_function_type)}${var.notification_config_json_suffix}"
  notification_secret_name = "${var.notification_function_name_prefix}${lower(var.notification_function_type)}-secret"
  notification_env_vars = {
    LOG_LEVEL                                             = var.notification_function_log_level
    "${upper(var.notification_function_type)}_CONFIG_URI" = local.notification_config_uri
  }
  // function code bundle exclusion
  function_source_exclude = flatten([
    for in_file in var.function_bundle_exclude_list_files : split("\n", file(in_file))
  ])
}

data "google_project" "project" {
  project_id = var.project_id
}

data "google_project" "target_project" {
  project_id = var.target_project_id
}

//////////////////////
// Service Account  //
//////////////////////

data "google_service_account" "sampler_service_account" {
  account_id = var.sampler_service_account_email
}

data "google_service_account" "notification_function_service_account" {
  account_id = var.notification_function_service_account_email
}

data "google_service_account" "cmd_pubsub_service_account" {
  account_id = var.pubsub_cmd_service_account_email
}

/////////
// GCS //
/////////

data "google_storage_bucket" "policy_bucket" {
  name = var.policy_bucket_name
}

data "google_storage_bucket" "request_bucket" {
  name = var.request_bucket_name
}

////////////
// PubSub //
////////////

// Commands
resource "google_pubsub_subscription" "pubsub_cmd_sampler" {
  name  = "${var.sampler_function_name}_http_cmd_push_subscription"
  topic = var.pubsub_cmd_topic_id
  push_config {
    push_endpoint = module.sampler.uri
    oidc_token {
      service_account_email = data.google_service_account.cmd_pubsub_service_account.email
      audience              = module.sampler.uri
    }
  }
  ack_deadline_seconds       = var.sampler_function_timeout
  message_retention_duration = "1200s" # 20 minutes
  retry_policy {
    minimum_backoff = "10s"
  }
}

// BigQuery data transfer
resource "google_pubsub_subscription" "pubsub_bq_notification_sampler" {
  name    = "${var.sampler_function_name}_http_bq_notification_push_subscription"
  topic   = var.pubsub_bq_notification_topic_id
  project = var.project_id
  push_config {
    push_endpoint = module.sampler.uri
    oidc_token {
      service_account_email = data.google_service_account.cmd_pubsub_service_account.email
      audience              = module.sampler.uri
    }
  }
  ack_deadline_seconds       = var.sampler_function_timeout
  message_retention_duration = "1200s" # 20 minutes
  retry_policy {
    minimum_backoff = "10s"
  }
}

/////////////////////
// Cloud Functions //
/////////////////////

module "sampler" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = var.project_id
  region          = var.region
  name            = var.sampler_function_name
  v2              = true
  service_account = data.google_service_account.sampler_service_account.email
  function_config = {
    entry_point     = var.sampler_function_handler
    runtime         = var.function_runtime
    instance_count  = var.sampler_function_max_instances
    memory_mb       = var.sampler_function_memory
    timeout_seconds = var.sampler_function_timeout
  }
  environment_variables = {
    BQ_TARGET_LOCATION             = local.bq_target_location
    BQ_TRANSFER_NOTIFICATION_TOPIC = var.pubsub_bq_notification_topic_id
    TARGET_PROJECT_ID              = var.target_project_id
    POLICY_BUCKET_NAME             = data.google_storage_bucket.policy_bucket.name
    REQUEST_BUCKET_NAME            = data.google_storage_bucket.request_bucket.name
    DEFAULT_POLICY_OBJECT_PATH     = var.default_policy_object_path
    SAMPLING_LOCK_OBJECT_PATH      = var.sampling_lock_object_path
    CMD_TOPIC_NAME                 = var.pubsub_cmd_topic_id
    ERROR_TOPIC_NAME               = var.pubsub_err_topic_id
    LOG_LEVEL                      = var.sampler_function_log_level
  }
  trigger_config = {
    v1 = null // forces HTTP
  }
  ingress_settings = "ALLOW_INTERNAL_AND_GCLB"
  bucket_name      = data.google_storage_bucket.policy_bucket.name
  bundle_config = {
    source_dir  = "../../code"
    output_path = "dist/${var.sampler_function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  iam = {
    "roles/cloudfunctions.invoker" = [
      // pubsub SA
      data.google_service_account.cmd_pubsub_service_account.member,
      // function SA
      data.google_service_account.sampler_service_account.member,
    ]
    "roles/cloudfunctions.serviceAgent" = [
      // function SA
      data.google_service_account.sampler_service_account.member,
    ]
  }
}

// Cloud Run backend permissions

// function SA
resource "google_cloud_run_service_iam_member" "todo_module_name_todo_fn_v2_run_agent" {
  service  = module.sampler.function.service_config[0].service
  location = var.region
  role     = "roles/run.serviceAgent"
  member   = data.google_service_account.sampler_service_account.member
}

// pubsub SA
resource "google_cloud_run_service_iam_member" "todo_module_name_todo_fn_v2_run_pubsub_invoker" {
  service  = module.sampler.function.service_config[0].service
  location = var.region
  role     = "roles/run.invoker"
  member   = data.google_service_account.cmd_pubsub_service_account.member
}

// Notification Function
module "notification" {
  source          = "github.com/GoogleCloudPlatform/cloud-foundation-fabric/modules/cloud-function"
  project_id      = var.project_id
  region          = var.region
  name            = local.notification_function_name
  service_account = data.google_service_account.notification_function_service_account.email
  function_config = {
    entry_point     = local.notification_handler
    runtime         = var.function_runtime
    instance_count  = var.notification_function_max_instances
    memory_mb       = 256
    timeout_seconds = 180
  }
  environment_variables = local.notification_env_vars
  bucket_name           = data.google_storage_bucket.policy_bucket.name
  bundle_config = {
    source_dir  = "../../code"
    output_path = "dist/${local.notification_function_name}-bundle.zip"
    excludes    = local.function_source_exclude
  }
  trigger_config = {
    v1 = {
      event    = "google.pubsub.topic.publish"
      resource = var.pubsub_err_topic_id
      retry    = null
    }
  }
}

/////////////////////////////
// Monitoring and Alerting //
/////////////////////////////

data "google_monitoring_notification_channel" "error_monitoring_channel" {
  display_name = var.notification_monitoring_channel_name
}

data "google_monitoring_notification_channel" "notification_monitoring_channel" {
  display_name = var.notification_monitoring_channel_name
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
  notification_channels = [data.google_monitoring_notification_channel.error_monitoring_channel.id]
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
  notification_channels = [data.google_monitoring_notification_channel.error_monitoring_channel.id]
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
  notification_channels = [data.google_monitoring_notification_channel.notification_monitoring_channel.id]
}

////////////////////
// Secret Manager //
////////////////////

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
  iam = {
    "${local.notification_secret_name}" = {
      "roles/secretmanager.secretAccessor" = [data.google_service_account.notification_function_service_account.member]
    }
  }
}
