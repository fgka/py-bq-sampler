/////////
// GCS //
/////////

output "request_bucket" {
  value = module.request_bucket.bucket
}

////////////
// PubSub //
////////////

output "pubsub_bq_notification" {
  value = module.pubsub_bq_notification.topic
}
