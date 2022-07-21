output "sampler_function" {
  value = module.sampler.function
}

output "sampler_function_service_account" {
  value = module.sampler_service_account.service_account
}

output "policy_bucket" {
  value = module.policy_bucket.bucket
}

output "request_bucket" {
  value = module.request_bucket.bucket
}

output "pubsub_cmd" {
  value = module.pubsub_cmd.topic
}

output "pubsub_err" {
  value = module.pubsub_err.topic
}

output "notification_function" {
  value = module.notification.function
}

output "notification_function_service_account" {
  value = module.notification_function_service_account.service_account
}

output "trigger_job" {
  value = google_cloud_scheduler_job.trigger_job
}

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

output "notification_secret" {
  value     = module.notification_secret.secrets
  sensitive = true
}

output "integ_test_datasets" {
  value = google_bigquery_dataset.integ_test_datasets
}

output "integ_test_data_transfer" {
  value = google_bigquery_data_transfer_config.integ_test_data_transfer
}