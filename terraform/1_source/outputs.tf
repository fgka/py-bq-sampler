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

output "pubsub_err" {
  value = module.pubsub_err.topic
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

//////////////////////
// Integ Tests Data //
//////////////////////

output "integ_test_datasets" {
  value = google_bigquery_dataset.integ_test_datasets
}

output "integ_test_data_transfer" {
  value = google_bigquery_data_transfer_config.integ_test_data_transfer
}