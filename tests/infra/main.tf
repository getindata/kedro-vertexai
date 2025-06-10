provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# Define common tags for all resources
locals {
  common_tags = {
    group = "kedro-vertexai-e2e"
  }
}
