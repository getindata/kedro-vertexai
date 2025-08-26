# Development Utilities

This directory contains scripts and tools for kedro-vertexai development and testing.

## E2E Test Runner Script

The `run_tests.sh` script runs end-to-end tests against a real Vertex AI project.

### Prerequisites

#### Required Tools
- **Python 3.10+** - For running Kedro and kedro-vertexai
- **Poetry** - For package building (`pip install poetry`)
- **Docker** - For building pipeline images (must be running)
- **Google Cloud CLI** - For GCP authentication and project access

#### Google Cloud Authentication Setup

**IMPORTANT**: You need TWO types of authentication:

1. **Standard gcloud authentication**:
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

2. **Application Default Credentials** (required for Vertex AI Python client):
   ```bash
   gcloud auth application-default login
   ```

3. **Docker registry authentication**:
   ```bash
   gcloud auth configure-docker europe-west1-docker.pkg.dev
   ```

The test script will verify both types of authentication before proceeding.

### Configuration

Create a `local.env` file in the `dev-utils` directory with your configuration:

```bash
# Copy and modify the local.env template
cp dev-utils/local.env.template dev-utils/local.env
# Edit local.env with your actual values
```

Required variables in `local.env`:
```bash
GCP_PROJECT_ID="your-gcp-project-id"
GCP_REGION="europe-west4"
VERTEX_AI_DOCKER_REGISTRY="europe-west1-docker.pkg.dev/your-project/your-repo"
VERTEX_AI_SERVICE_ACCOUNT="your-service-account@your-project.iam.gserviceaccount.com"
GCS_BUCKET_ROOT="your-bucket-name/kedro-data"
```

### Fixed Configuration

The script uses these fixed settings:
- **E2E test cases**: standard
- **Kedro starter version**: 1.0.0  
- **Test directory**: `e2e-testing/` (no timestamps)
- **Cleanup**: Resources are NOT cleaned up (preserved for debugging)

### Usage Examples

```bash
# Run E2E tests (after setting up local.env)
./dev-utils/run_tests.sh

# Get help
./dev-utils/run_tests.sh --help
```

### What the Script Does

1. Loads configuration from `local.env`
2. Builds the kedro-vertexai package
3. Creates a Kedro spaceflights project in `e2e-testing/`
4. Configures Docker and Vertex AI settings
5. Builds and pushes Docker images to GCP
6. Runs the pipeline on Vertex AI
7. Waits for completion
8. Preserves all resources for debugging

### Required GCP Permissions

Your service account needs these permissions:

- Vertex AI Pipeline User
- Storage Admin (for GCS bucket access)
- Container Registry Service Agent
- Cloud Run Developer (for pipeline execution)

### Troubleshooting

1. **Docker authentication issues:**
   ```bash
   gcloud auth configure-docker europe-west1-docker.pkg.dev
   ```

2. **GCP authentication issues:**
   ```bash
   gcloud auth login
   gcloud config set project YOUR_PROJECT_ID
   ```

3. **Permission issues:**
   - Ensure your user/service account has the required IAM roles
   - Check that the Vertex AI API is enabled in your project

4. **Resource cleanup:**
   - If cleanup fails, manually delete resources from:
     - Vertex AI Pipelines console
     - Container Registry
     - Cloud Storage bucket

### Notes

- The script automatically generates unique experiment names and image tags to avoid conflicts
- Test artifacts are stored in the specified GCS bucket root
- Each test case runs in isolation with its own experiment and image