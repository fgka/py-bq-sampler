////////////////////
// Global/General //
////////////////////

variable "project_id" {
  description = "Project ID where to deploy and source of data."
  type        = string
}

variable "target_project_id" {
  description = "Project ID where the data will land."
  type        = string
}

variable "bq_target_location" {
  description = "In which region to send the samples to in the target project."
  type        = string
  default     = null
}

variable "region" {
  description = "Default region where to create resources."
  type        = string
  default     = "us-central1"
}

variable "function_bundle_exclude_list_files" {
  description = "All files to provide exclude patterns in the function deployment."
  type        = list(string)
  default = [
    "../.gcloudignore",
    "../.gitignore",
    "function_bundle_extra_exclude.txt"
  ]
}

variable "function_runtime" {
  description = "Default Cloud Function runtime."
  type        = string
  default     = "python39"
}

/////////////
// Buckets //
/////////////

variable "policy_bucket_name_prefix" {
  description = "Prefix to name the policy bucket created in the source project, the suffix is the project numerical ID."
  type        = string
  default     = "sample-policy"
}

variable "request_bucket_name_prefix" {
  description = "Prefix to name the request bucket created in the target project, the suffix is the project numerical ID."
  type        = string
  default     = "sample-request"
}

/////////////////////////
// Special GCS objects //
/////////////////////////

variable "default_policy_object_path" {
  description = "Path, in the policy bucket, for the default policy JSON file."
  type        = string
  default     = "default-policy.json"
}

variable "sampling_lock_object_path" {
  description = "Path, in the request bucket, to indicate that a sampling should be skipped."
  type        = string
  default     = "block-sampling"
}

//////////////////////
// Sampler Function //
//////////////////////

variable "sampler_function_max_instances" {
  description = "The maximum amount of concurrent instances for processing sampling commands."
  type        = number
  default     = 1000
}

variable "sampler_function_memory" {
  description = "The memory allocation for function in MB."
  type        = number
  default     = 512
}

variable "sampler_function_timeout" {
  description = "The sampler function timeout in seconds."
  type        = number
  default     = 540
}

variable "sampler_function_name" {
  description = "Name of the sampler Cloud Function."
  type        = string
  default     = "bq-sampler"
}

variable "sampler_function_handler" {
  description = "Cloud Function handler for sampler."
  type        = string
  default     = "handler"
}

variable "sampler_function_log_level" {
  description = "Sampler function log level."
  type        = string
  default     = "INFO"
}

variable "sampler_service_account_name" {
  description = "Service account to be assigned to the sampler Cloud Function."
  type        = string
  default     = "bq-sampler-sa"
}

///////////////////////////
// Notification Function //
///////////////////////////

variable "notification_function_max_instances" {
  description = "The maximum amount of concurrent instances for notification."
  type        = number
  default     = 10
}

variable "notification_function_type" {
  description = "Which kind of notification to use, either SMTP or SENDGRID."
  type        = string
  default     = "SMTP"
  validation {
    condition     = contains(["SMTP", "SENDGRID"], var.notification_function_type)
    error_message = "The supported types are \"SMTP\" and \"SENDGRID\"."
  }
}

variable "notification_function_name_prefix" {
  description = "Name prefix for notification Cloud Function."
  type        = string
  default     = "bq-sampler-notification-"
}

variable "notification_function_handler_prefix" {
  description = "Cloud Function handler prefix for notification."
  type        = string
  default     = "handler_"
}

variable "notification_function_log_level" {
  description = "Notification function log level."
  type        = string
  default     = "INFO"
}

variable "notification_config_json_suffix" {
  description = "GCS object name suffix for notification configuration."
  type        = string
  default     = "_config.json"
}

variable "notification_function_service_account_name" {
  description = "Service account to be assigned to the notification Cloud Function."
  type        = string
  default     = "bq-sampler-notification-sa"
}

////////////
// PubSub //
////////////

variable "pubsub_cmd_topic_name" {
  description = "Name of the PubSub topic to send commands to the Cloud Function."
  type        = string
  default     = "bq-sampler-cmd"
}

variable "pubsub_error_topic_name" {
  description = "Name of the PubSub topic to send error notifications from the Cloud Function."
  type        = string
  default     = "bq-sampler-err"
}

variable "pubsub_bq_notification_topic_name" {
  description = "Name of the PubSub topic to send BigQuery transfer runs' notifications to."
  type        = string
  default     = "bq-sampler-bq-transfer-notification"
}

variable "pubsub_cmd_service_account_name" {
  description = "Service account to be used by PuSub to trigger sampler function."
  type        = string
  default     = "bq-sampler-pubsub-sa"
}

///////////////////////
// Scheduler/Cronjob //
///////////////////////

variable "scheduler_name" {
  description = "Name of the Cloud Scheduler that triggers the Cloud Function."
  type        = string
  default     = "cronjob-bq-sampler"
}

variable "scheduler_data" {
  description = "Sampling trigger payload."
  type        = string
  default     = "{\"type\": \"START\"}"
}

// Set to run, by default, every day at 23:00 (11pm)
variable "scheduler_cron_entry" {
  description = "Crontab entry to define when the sample should start."
  type        = string
  default     = "0 23 * * *"
}

// By default, uses UTC
variable "scheduler_cron_timezone" {
  description = "Crontab entry timezone."
  type        = string
  default     = "Etc/UTC"
}

////////////////
// Monitoring //
////////////////

variable "monitoring_channel_name" {
  description = "Monitoring channel name pegged to PubSub."
  type        = string
  default     = "bq-sampler-error-monitoring-channel"
}

variable "notification_monitoring_channel_name" {
  description = "Monitoring channel name pegged to email."
  type        = string
  default     = "bq-sampler-notification-error-monitoring-channel"
}

variable "notification_monitoring_email_address" {
  description = "When the notification function fails, it needs to send the alert to a specific email."
  type        = string
}

variable "monitoring_alert_severity" {
  description = "Severity, included, above which it should generate an alert."
  type        = string
  default     = "ERROR"
}

//////////////////////
// Integ Tests Data //
//////////////////////

variable "create_integ_test_data" {
  description = "If true will create the BigQuery tables with the data required for running the integration tests."
  type        = bool
  default     = false
}

variable "integ_tests_project_id" {
  description = "Project ID containing the datasets to be cloned for integration tests."
  type        = string
  default     = "bigquery-public-data"
}

variable "integ_test_datasets" {
  description = "Which (public) BigQuery datasets to clone, from 'integ_tests_project_id'."
  type        = list(string)
  default = [
    "census_bureau_acs",
    "new_york_taxi_trips",
  ]
}