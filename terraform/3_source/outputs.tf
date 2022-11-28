/////////////////////
// Cloud Functions //
/////////////////////

output "sampler_function" {
  value = module.sampler.function
}

output "sampler_run" {
  value = module.sampler.function.service_config[0].service
}

output "sampler_function_http_url" {
  value = module.sampler.uri
}

output "notification_function" {
  value = module.notification.function
}

////////////
// PubSub //
////////////

output "pubsub_cmd_sampler" {
  value = google_pubsub_subscription.pubsub_cmd_sampler
}

output "pubsub_bq_notification_sampler" {
  value = google_pubsub_subscription.pubsub_bq_notification_sampler
}

/////////////////////////////
// Monitoring and Alerting //
/////////////////////////////

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
