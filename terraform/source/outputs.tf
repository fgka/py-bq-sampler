//////////////////////
// Service Accounts //
//////////////////////

output "sampler_function_service_account" {
  value = module.sampler_service_account.service_account
}

output "notification_function_service_account" {
  value = module.notification_function_service_account.service_account
}

output "cmd_pubsub_service_account" {
  value = module.cmd_pubsub_service_account.service_account
}

/////////////////////
// Cloud Functions //
/////////////////////

output "sampler_function" {
  value = module.sampler.function
}

output "sampler_function_http_url" {
  value = module.sampler.function.https_trigger_url
}

output "notification_function" {
  value = module.notification.function
}

/////////
// GCS //
/////////

output "policy_bucket" {
  value = module.policy_bucket.bucket
}

////////////
// PubSub //
////////////

output "pubsub_cmd" {
  value = module.pubsub_cmd.topic
}

output "pubsub_cmd_sampler" {
  value = google_pubsub_subscription.pubsub_cmd_sampler
}

output "pubsub_err" {
  value = module.pubsub_err.topic
}

output "pubsub_bq_notification_sampler" {
  value = google_pubsub_subscription.pubsub_bq_notification_sampler
}

///////////////
// Scheduler //
///////////////

output "trigger_job" {
  value = google_cloud_scheduler_job.trigger_job
}

/////////////////////////////
// Monitoring and Alerting //
/////////////////////////////

output "error_monitoring_channel" {
  value = google_monitoring_notification_channel.error_monitoring_channel
}

output "notification_error_monitoring_channel" {
  value = google_monitoring_notification_channel.notification_error_monitoring_channel
}

output "alert_error_log_policy" {
  value = google_monitoring_alert_policy.alert_error_log_policy
}

output "alert_not_executed_policy" {
  value = google_monitoring_alert_policy.alert_not_executed_policy
}

output "notification_alert_error_log_policy" {
  value = google_monitoring_alert_policy.notification_alert_error_log_policy
}

////////////////////
// Secret Manager //
////////////////////

output "notification_secret" {
  value     = module.notification_secret.secrets
  sensitive = true
}

//////////////////////
// Integ Tests Data //
//////////////////////

output "integ_test_datasets" {
  value = google_bigquery_dataset.integ_test_datasets
}

output "integ_test_data_transfer" {
  value = google_bigquery_data_transfer_config.integ_test_data_transfer
}