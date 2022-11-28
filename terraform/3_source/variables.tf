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
    "../../.gcloudignore",
    "../../.gitignore",
    "function_bundle_extra_exclude.txt"
  ]
}

variable "function_runtime" {
  description = "Default Cloud Function runtime."
  type        = string
  default     = "python39"
}

//////////////////////
// Service Account  //
//////////////////////

variable "sampler_service_account_email" {
  description = "Service account email to be assigned to the sampler Cloud Function."
  type        = string
}

variable "notification_function_service_account_email" {
  description = "Service account email to be assigned to the notification Cloud Function."
  type        = string
}

variable "pubsub_cmd_service_account_email" {
  description = "Service account email to be used by PuSub to trigger sampler function."
  type        = string
}

/////////////
// Buckets //
/////////////

variable "policy_bucket_name" {
  description = "Name the policy bucket created in the source project, the suffix is the project numerical ID."
  type        = string
}

variable "request_bucket_name" {
  description = "Name of the request bucket created in the target project."
  type        = string
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
  default     = 256
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

////////////
// PubSub //
////////////

variable "pubsub_cmd_topic_id" {
  description = "Full resource name of the PubSub topic to send commands to the Cloud Function."
  type        = string
}

variable "pubsub_err_topic_id" {
  description = "Full resource name of the PubSub topic to send error notifications from the Cloud Function."
  type        = string
}

variable "pubsub_bq_notification_topic_id" {
  description = "Full resource name of the PubSub topic to send BigQuery transfer runs' notifications to."
  type        = string
}

////////////////
// Monitoring //
////////////////

variable "monitoring_channel_name" {
  description = "Monitoring channel name pegged to PubSub."
  type        = string
}

variable "notification_monitoring_channel_name" {
  description = "Monitoring channel name pegged to email."
  type        = string
}

variable "monitoring_alert_severity" {
  description = "Severity, included, above which it should generate an alert."
  type        = string
  default     = "ERROR"
}
