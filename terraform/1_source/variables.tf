////////////////////
// Global/General //
////////////////////

variable "project_id" {
  description = "Project ID where to deploy and source of data."
  type        = string
}

variable "region" {
  description = "Default region where to create resources."
  type        = string
  default     = "us-central1"
}

//////////////////////
// Service Accounts //
//////////////////////

variable "sampler_service_account_name" {
  description = "Service account to be assigned to the sampler Cloud Function."
  type        = string
  default     = "bq-sampler-sa"
}

variable "pubsub_cmd_service_account_name" {
  description = "Service account to be used by PuSub to trigger sampler function."
  type        = string
  default     = "bq-sampler-pubsub-sa"
}

variable "notification_function_service_account_name" {
  description = "Service account to be assigned to the notification Cloud Function."
  type        = string
  default     = "bq-sampler-notification-sa"
}

/////////////
// Buckets //
/////////////

variable "policy_bucket_name_prefix" {
  description = "Prefix to name the policy bucket created in the source project, the suffix is the project numerical ID."
  type        = string
  default     = "sample-policy"
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
