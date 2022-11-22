////////////////////
// Global/General //
////////////////////

variable "project_id" {
  description = "Project ID where the data will land."
  type        = string
}

variable "region" {
  description = "Default region where to create resources."
  type        = string
  default     = "us-central1"
}

/////////////////////
// Service Account //
/////////////////////

variable "sampler_service_account_email" {
  description = "Service account assigned to the sampler Cloud Function."
  type        = string
}

/////////
// GCS //
/////////

variable "request_bucket_name_prefix" {
  description = "Prefix to name the request bucket created in the target project, the suffix is the project numerical ID."
  type        = string
  default     = "sample-request"
}

////////////
// PubSub //
////////////

variable "pubsub_bq_notification_topic_name" {
  description = "Name of the PubSub topic to send BigQuery transfer runs' notifications to."
  type        = string
  default     = "bq-sampler-bq-transfer-notification"
}

