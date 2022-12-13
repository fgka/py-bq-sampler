////////////////////
// Global/General //
////////////////////

module "source_1" {
  source                                     = "./1_source"
  project_id                                 = var.project_id
  region                                     = var.region
  sampler_service_account_name               = var.sampler_service_account_name
  pubsub_cmd_service_account_name            = var.pubsub_cmd_service_account_name
  notification_function_service_account_name = var.notification_function_service_account_name
  policy_bucket_name_prefix                  = var.policy_bucket_name_prefix
  pubsub_cmd_topic_name                      = var.pubsub_cmd_topic_name
  pubsub_error_topic_name                    = var.pubsub_error_topic_name
  scheduler_name                             = var.scheduler_name
  scheduler_data                             = var.scheduler_data
  scheduler_cron_entry                       = var.scheduler_cron_entry
  scheduler_cron_timezone                    = var.scheduler_cron_timezone
  monitoring_channel_name                    = var.monitoring_channel_name
  notification_monitoring_channel_name       = var.notification_monitoring_channel_name
  notification_monitoring_email_address      = var.notification_monitoring_email_address
  create_integ_test_data                     = var.create_integ_test_data
  integ_tests_project_id                     = var.integ_tests_project_id
  integ_test_datasets                        = var.integ_test_datasets
}

module "target_2" {
  source                            = "./2_target"
  project_id                        = var.target_project_id
  region                            = var.region
  sampler_service_account_email     = module.source_1.sampler_function_service_account.email
  request_bucket_name_prefix        = var.request_bucket_name_prefix
  pubsub_bq_notification_topic_name = var.pubsub_bq_notification_topic_name
  depends_on                        = [module.source_1]
}

module "source_3" {
  source                                      = "./3_source"
  project_id                                  = var.project_id
  target_project_id                           = var.target_project_id
  region                                      = var.region
  bq_target_location                          = var.bq_target_location
  function_bundle_exclude_list_files          = var.function_bundle_exclude_list_files
  function_runtime                            = var.function_runtime
  code_dir                                    = var.code_dir
  sampler_service_account_email               = module.source_1.sampler_function_service_account.email
  notification_function_service_account_email = module.source_1.notification_function_service_account.email
  pubsub_cmd_service_account_email            = module.source_1.cmd_pubsub_service_account.email
  policy_bucket_name                          = module.source_1.policy_bucket.name
  request_bucket_name                         = module.target_2.request_bucket.name
  default_policy_object_path                  = var.default_policy_object_path
  sampling_lock_object_path                   = var.sampling_lock_object_path
  sampler_function_max_instances              = var.sampler_function_max_instances
  sampler_function_memory                     = var.sampler_function_memory
  sampler_function_timeout                    = var.sampler_function_timeout
  sampler_function_name                       = var.sampler_function_name
  sampler_function_handler                    = var.sampler_function_handler
  sampler_function_log_level                  = var.sampler_function_log_level
  notification_function_max_instances         = var.notification_function_max_instances
  notification_function_type                  = var.notification_function_type
  notification_function_name_prefix           = var.notification_function_name_prefix
  notification_function_handler_prefix        = var.notification_function_handler_prefix
  notification_function_log_level             = var.notification_function_log_level
  notification_config_json_suffix             = var.notification_config_json_suffix
  pubsub_cmd_topic_id                         = module.source_1.pubsub_cmd.id
  pubsub_err_topic_id                         = module.source_1.pubsub_err.id
  pubsub_bq_notification_topic_id             = module.target_2.pubsub_bq_notification.id
  monitoring_channel_name                     = module.source_1.error_monitoring_channel.display_name
  notification_monitoring_channel_name        = module.source_1.notification_error_monitoring_channel.display_name
  monitoring_alert_severity                   = var.monitoring_alert_severity
  depends_on                                  = [module.source_1, module.target_2]
}