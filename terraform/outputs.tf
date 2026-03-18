output "gcs_bucket_url" {
  description = "GCS bucket URI for raw CSV files"
  value       = "gs://${google_storage_bucket.nz_electricity.name}"
}

output "bq_dataset_raw" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_core" {
  value = google_bigquery_dataset.core.dataset_id
}

output "bq_dataset_marts" {
  value = google_bigquery_dataset.marts.dataset_id
}
