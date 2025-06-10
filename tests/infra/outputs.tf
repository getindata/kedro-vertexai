output "data_bucket_url" {
  description = "The URL of the data bucket"
  value       = google_storage_bucket.data_bucket.url
}

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "The GCP region"
  value       = var.region
}

output "artifact_registry_repository" {
  description = "The Docker Artifact Registry repository"
  value       = google_artifact_registry_repository.kedro_e2e.name
}

output "artifact_registry_location" {
  description = "The location of the Docker Artifact Registry"
  value       = google_artifact_registry_repository.kedro_e2e.location
}

output "kedro_e2e_service_account_email" {
  description = "The email of the Kedro E2E service account"
  value       = google_service_account.kedro_e2e.email
}

output "github_actions_service_account_email" {
  description = "The email of the GitHub Actions service account"
  value       = google_service_account.github_actions.email
}

output "workload_identity_pool_id" {
  description = "The ID of the Workload Identity Pool"
  value       = google_iam_workload_identity_pool.github_pool.id
}

output "workload_identity_provider_id" {
  description = "The ID of the Workload Identity Provider"
  value       = google_iam_workload_identity_pool_provider.github_provider.id
}
