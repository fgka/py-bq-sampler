////////////////////
// Global/General //
////////////////////

variable "services" {
  type        = list(string)
  description = "All GCP services that need to be enabled"
  default = [
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
  ]
}

variable "target_services" {
  type        = list(string)
  description = "Minimum GCP services that need to be enabled on target project"
  default = [
    "bigquery.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "storage.googleapis.com",
  ]
}

variable "project_id" {
  type        = string
  description = "Project ID where to deploy and source of data"
}

variable "target_project_id" {
  type        = string
  description = "Project ID where the data will land"
}

variable "region" {
  type        = string
  description = "Default region where to create resources"
  default     = "us-central1"
}

variable "function_bundle_exclude_list_files" {
  type        = list(string)
  description = "All files to provide exclude patterns in the function deployment"
  default = [
    "../.gcloudignore",
    "../.gitignore",
    "function_bundle_extra_exclude.txt"
  ]
}

variable "function_runtime" {
  type        = string
  description = "Default Cloud Function runtime"
  default     = "python39"
}

/////////////
// Buckets //
/////////////

variable "policy_bucket_name_prefix" {
  type        = string
  description = "Prefix to name the policy bucket created in the source project, the suffix is the project numerical ID"
  default     = "sample-policy"
}

variable "request_bucket_name_prefix" {
  type        = string
  description = "Prefix to name the request bucket created in the target project, the suffix is the project numerical ID"
  default     = "sample-request"
}

/////////////////////////
// Special GCS objects //
/////////////////////////

variable "default_policy_object_path" {
  type        = string
  description = "Path, in the policy bucket, for the default policy JSON file"
  default     = "default-policy.json"
}

variable "sampling_lock_object_path" {
  type        = string
  description = "Path, in the request bucket, to indicate that a sampling should be skipped"
  default     = "block-sampling"
}

//////////////////////
// Sampler Function //
//////////////////////

variable "function_name" {
  type        = string
  description = "Name of the sampler Cloud Function"
  default     = "bq-sampler"
}

variable "function_handler" {
  type        = string
  description = "Cloud Function handler for sampler"
  default     = "handler"
}

variable "function_log_level" {
  type        = string
  description = "Sampler function log level"
  default     = "INFO"
}

variable "function_service_account_name" {
  type        = string
  description = "Service account to be assigned to the sampler Cloud Function"
  default     = "bq-sampler-sa"
}

///////////////////////////
// Notification Function //
///////////////////////////

variable "notification_function_type" {
  type        = string
  description = "Which kind of notification to use, either SMTP or SENDGRID"
  default     = "SMTP"
  validation {
    condition     = contains(["SMTP", "SENDGRID"], var.notification_function_type)
    error_message = "The supported types are \"SMTP\" and \"SENDGRID\"."
  }
}

variable "notification_function_name_prefix" {
  type        = string
  description = "Name prefix for notification Cloud Function"
  default     = "bq-sampler-notification-"
}

variable "notification_function_handler_prefix" {
  type        = string
  description = "Cloud Function handler prefix for notification"
  default     = "handler_"
}

variable "notification_function_log_level" {
  type        = string
  description = "Notification function log level"
  default     = "INFO"
}

variable "notification_config_json_suffix" {
  type        = string
  description = "GCS object name suffix for notification configuration"
  default     = "_config.json"
}

variable "notification_function_service_account_name" {
  type        = string
  description = "Service account to be assigned to the notification Cloud Function"
  default     = "bq-sampler-notification-sa"
}

////////////
// PubSub //
////////////

variable "pubsub_cmd_topic_name" {
  type        = string
  description = "Name of the PubSub topic to send commands to the Cloud Function"
  default     = "bq-sampler-cmd"
}

variable "pubsub_error_topic_name" {
  type        = string
  description = "Name of the PubSub topic to send error notifications from the Cloud Function"
  default     = "bq-sampler-err"
}

///////////////////////
// Scheduler/Cronjob //
///////////////////////

variable "scheduler_name" {
  type        = string
  description = "Name of the Cloud Scheduler that triggers the Cloud Function"
  default     = "cronjob-bq-sampler"
}

variable "scheduler_data" {
  type        = string
  description = "Sampling trigger payload"
  default     = "{\"type\": \"START\"}"
}

// Set to run, by default, every day at 23:00 (11pm)
variable "scheduler_cron_entry" {
  type        = string
  description = "Crontab entry to define when the sample should start"
  default     = "0 23 * * *"
}

// By default, uses UTC
variable "scheduler_cron_timezone" {
  type        = string
  description = "Crontab entry timezone"
  default     = "Etc/UTC"
}

////////////////
// Monitoring //
////////////////

variable "monitoring_channel_name" {
  type        = string
  description = "Monitoring channel name pegged to PubSub"
  default     = "bq-sampler-error-monitoring-channel"
}

variable "notification_monitoring_channel_name" {
  type        = string
  description = "Monitoring channel name pegged to email"
  default     = "bq-sampler-notification-error-monitoring-channel"
}

variable "notification_monitoring_email_address" {
  type        = string
  description = "When the notification function fails, it needs to send the alert to a specific email"
}

variable "monitoring_alert_severity" {
  type        = string
  description = "Severity, included, above which it should generate an alert"
  default     = "ERROR"
}