output "region" {
  value = var.region
}

output "project_id" {
  value = local.project_id
}

output "target_project_id" {
  value = local.target_project_id
}

output "default_policy_uri" {
  value = "gs://${module.policy_bucket.name}/${var.default_policy_object_path}"
}

output "function_name" {
  value = module.sampler.function_name
}

output "notification_type" {
  value = var.notification_function_type
}

output "notification_function_name" {
  value = module.notification.function_name
}

output "notification_secret" {
  value = local.notification_secret_name
}

output "notification_config_uri" {
  value = local.notification_config_uri
}

output "pubsub_error_topic" {
  value = module.pubsub_err.topic.name
}

output "scheduler_job" {
  value = google_cloud_scheduler_job.trigger_job.name
}
