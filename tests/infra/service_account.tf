# Enable required APIs for IAM
resource "google_project_service" "iam" {
  project            = var.project_id
  service            = "iam.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "iam_credentials" {
  project            = var.project_id
  service            = "iamcredentials.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "vertex_ai" {
  project            = var.project_id
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

# Create service account for GitHub Actions
resource "google_service_account" "kedro_e2e" {
  account_id   = "kedro-e2e"
  display_name = "Kedro E2E Service Account"
  description  = "Service account for Kedro E2E testing with GitHub Actions"

  depends_on = [google_project_service.iam]
}

# Create Workload Identity Pool for GitHub Actions
resource "google_iam_workload_identity_pool" "github_pool" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions Identity Pool"
  description               = "Identity pool for GitHub Actions"

  depends_on = [google_project_service.iam]
}

# Create Workload Identity Provider for GitHub
resource "google_iam_workload_identity_pool_provider" "github_provider" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub Actions Provider"
  attribute_condition                = "attribute.repository==\"getindata/kedro-vertexai\""

  attribute_mapping = {
    "google.subject"             = "assertion.sub"
    "attribute.actor"            = "assertion.actor"
    "attribute.aud"              = "assertion.aud"
    "attribute.repository"       = "assertion.repository"
    "attribute.repository_owner" = "assertion.repository_owner"
  }
  oidc {
    allowed_audiences = []
    issuer_uri        = "https://token.actions.githubusercontent.com"
  }
}

# Allow GitHub Actions to impersonate the service account
resource "google_service_account_iam_binding" "workload_identity_binding" {
  service_account_id = google_service_account.kedro_e2e.name
  role               = "roles/iam.workloadIdentityUser"

  members = [
    "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github_pool.name}/*"
  ]
}

# Grant the kedro-e2e service account write access to the data bucket
resource "google_storage_bucket_iam_binding" "data_bucket_writer" {
  bucket = google_storage_bucket.data_bucket.name
  role   = "roles/storage.objectAdmin"

  members = [
    "serviceAccount:${google_service_account.kedro_e2e.email}"
  ]
}

# Grant the kedro-e2e service account vertexai editor role on the project level
resource "google_project_iam_binding" "vertexai_editor" {
  project = var.project_id
  role    = "roles/aiplatform.user"

  members = [
    "serviceAccount:${google_service_account.kedro_e2e.email}"
  ]
}