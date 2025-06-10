# Terraform Infrastructure for Kedro Vertex AI

This directory contains Terraform configuration files for setting up the necessary infrastructure for the Kedro Vertex AI plugin's end-to-end testing.

## Directory Structure

- `main.tf` - Provider configuration and common local variables
- `storage.tf` - Storage-related resources (GCS bucket and Artifact Registry)
- `service_account.tf` - Service account and IAM-related resources
- `variables.tf` - Input variable definitions
- `outputs.tf` - Output value definitions
- `backend.tf.sample` - Sample Terraform backend configuration
- `infra.env.sample` - Sample environment variables file

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) installed (version >= 1.0.0)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) installed and configured
- A GCP project with billing enabled
- A GCS bucket for storing Terraform state

## Setup

1. Copy the sample environment file and update it with your values:

```bash
cp infra.env.sample infra.env
# Edit infra.env with your actual values
```

2. Copy the sample backend configuration and update it with your values:

```bash
cp backend.tf.sample backend.tf
# Edit backend.tf with your actual values
```

3. Source the environment file:

```bash
source infra.env
```

4. Initialize Terraform:

```bash
terraform init
```

5. Plan the infrastructure changes:

```bash
terraform plan
```

6. Apply the infrastructure changes:

```bash
terraform apply
```

## Infrastructure Components

The Terraform configuration creates the following resources:

- **APIs**: Enables required GCP APIs (Vertex AI, Storage, IAM, Artifact Registry)
- **Storage**:
  - GCS bucket for data storage with lifecycle rules (objects older than 30 days are deleted)
  - Artifact Registry repository for Docker images with cleanup policies
- **IAM and Security**:
  - Service account for E2E testing
  - Workload Identity Pool and Provider for GitHub Actions integration
  - IAM bindings for necessary permissions

## Cleanup

To destroy the infrastructure:

```bash
terraform destroy
```

Note: This will not delete the Terraform state bucket, as it's managed outside of this configuration.
