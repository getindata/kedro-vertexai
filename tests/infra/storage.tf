# Enable required APIs for storage
resource "google_project_service" "artifact_registry" {
  project            = var.project_id
  service            = "artifactregistry.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  project            = var.project_id
  service            = "storage.googleapis.com"
  disable_on_destroy = false
}

# Create a GCS bucket for data storage
resource "google_storage_bucket" "data_bucket" {
  name                        = "${var.project_id}-kedro-data"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30 # 30 days (1 month)
    }
    action {
      type = "Delete"
    }
  }

  labels = local.common_tags
}

# Create Docker Artifact Registry
resource "google_artifact_registry_repository" "kedro_e2e" {
  provider      = google
  location      = var.region
  repository_id = "kedro-e2e"
  description   = "Docker repository for Kedro E2E testing"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-minimum-versions"
    action = "DELETE"
    condition {
      older_than = "1209600s" # 2 weeks in seconds (14 days * 24 hours * 60 minutes * 60 seconds)
    }
  }

  depends_on = [google_project_service.artifact_registry]

  # Disable container scanning
  docker_config {
    immutable_tags = false
  }

  # Vulnerability scanning is disabled by default in Artifact Registry

  labels = local.common_tags
}