#!/bin/bash

# Kedro VertexAI E2E Test Runner
# This script runs end-to-end tests against a real Vertex AI project

set -e  # Exit on any error

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Clean up any existing e2e test directory
if [[ -d "$SCRIPT_DIR/../e2e-testing/spaceflights" ]]; then
    rm -rf "$SCRIPT_DIR/../e2e-testing/spaceflights"
fi

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Load environment variables from local.env if it exists
if [[ -f "$SCRIPT_DIR/local.env" ]]; then
    source "$SCRIPT_DIR/local.env"
    log_info "Loaded configuration from local.env"
fi

# Required environment variables
REQUIRED_ENV_VARS=(
    "GCP_PROJECT_ID"
    "GCP_REGION"
    "VERTEX_AI_DOCKER_REGISTRY"
    "VERTEX_AI_SERVICE_ACCOUNT"
    "GCS_BUCKET_ROOT"
)

# Fixed configuration (reasonable defaults)
E2E_CASES="standard"
KEDRO_STARTER_VERSION="1.0.0"

# Derived variables
EXPERIMENT_NAME="kedro-vertexai-dev-$(date +%Y%m%d-%H%M%S)"
DOCKER_IMAGE_TAG="kedro-vertexai-test-$(date +%Y%m%d-%H%M%S)"

print_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

This script runs kedro-vertexai end-to-end tests against a real Vertex AI project.

Required Environment Variables (or set in dev-utils/local.env):
  GCP_PROJECT_ID              Google Cloud Project ID
  GCP_REGION                  GCP region (e.g., europe-west4)
  VERTEX_AI_DOCKER_REGISTRY   Docker registry for Vertex AI images (e.g., europe-west1-docker.pkg.dev/PROJECT/REPO)
  VERTEX_AI_SERVICE_ACCOUNT   Service account email for Vertex AI pipelines
  GCS_BUCKET_ROOT            GCS bucket root path for storing pipeline artifacts

Fixed Configuration:
  E2E test cases: standard, grouping
  Kedro starter version: 1.0.0
  Resources are NOT cleaned up after tests (for debugging)

Options:
  -h, --help                 Show this help message

Examples:
  # Run e2e tests (after setting up local.env)
  $0

EOF
}

check_requirements() {
    log_info "Checking requirements..."
    
    # Check required environment variables
    for var in "${REQUIRED_ENV_VARS[@]}"; do
        if [[ -z "${!var}" ]]; then
            log_error "Required environment variable $var is not set"
            exit 1
        fi
    done
    
    # Check required tools
    local required_tools=("python" "pip" "poetry" "docker" "gcloud")
    for tool in "${required_tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            log_error "Required tool '$tool' is not installed"
            exit 1
        fi
    done
    
    # Check Docker is running
    if ! docker ps &> /dev/null; then
        log_error "Docker is not running or not accessible"
        exit 1
    fi
    
    # Check GCP authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | head -n1 &> /dev/null; then
        log_error "No active GCP authentication found. Run 'gcloud auth login' first"
        exit 1
    fi
    
    # Check Application Default Credentials for Vertex AI
    if ! gcloud auth application-default print-access-token &> /dev/null; then
        log_error "Application Default Credentials not found. Run 'gcloud auth application-default login' first"
        exit 1
    fi
    
    log_success "All requirements satisfied"
}

run_e2e_tests() {
    log_info "Running end-to-end tests..."
    
    # Build the package
    log_info "Building kedro-vertexai package..."
    poetry build -f sdist
    
    # Convert comma-separated cases to array
    IFS=',' read -ra CASES <<< "$E2E_CASES"
    
    for case in "${CASES[@]}"; do
        log_info "Running e2e test case: $case"
        run_single_e2e_test "$case"
    done
    
    log_success "End-to-end tests completed successfully"
}

run_single_e2e_test() {
    local case=$1
    local test_dir="e2e-testing"
    
    log_info "Setting up e2e test for case: $case"
    
    # Get absolute paths before changing directories
    local project_root=$(pwd)
    local package_path=$(find "$project_root/dist" -name "*.tar.gz" | head -1)
    local config_path="$project_root/tests/e2e/$case/starter-config.yml"
    local overwrite_path="$project_root/tests/e2e/$case/overwrite"
    
    # Create test directory
    mkdir -p "$test_dir"
    cd "$test_dir"
    
    # Install kedro-vertexai from built package
    pip install "$package_path"
    
    # Initialize starter project
    log_info "Initializing Kedro starter project..."
    kedro new --starter spaceflights-pandas \
        --config "$config_path" \
        --verbose \
        --checkout="$KEDRO_STARTER_VERSION"
    
    cd spaceflights
    
    # Install requirements and kedro-vertexai
    log_info "Installing project requirements..."
    cp "$package_path" ./kedro-vertexai.tar.gz
    echo -e "\n./kedro-vertexai.tar.gz\n" >> requirements.txt
    echo -e "kedro-docker\n" >> requirements.txt
    sed -i '' '/kedro-telemetry/d' requirements.txt
    pip install -r requirements.txt
    
    # Remove kedro-mlflow to prevent mlflow-start-run node in e2e tests
    pip uninstall kedro-mlflow -y
    
    # Initialize Docker and VertexAI configurations
    log_info "Configuring Docker and VertexAI..."
    kedro docker init
    sed -i '' "s/\(COPY requirements.txt.*\)$/\1\nCOPY kedro-vertexai.tar.gz ./g" Dockerfile
    sed -i '' "s/python:3.9-slim/python:3.10-slim/g" Dockerfile
    echo '!data/01_raw' >> .dockerignore
    
    # Initialize VertexAI with provided credentials
    kedro vertexai init "$GCP_PROJECT_ID" "$GCP_REGION"
    
    # Backup the generated configuration files before overwriting
    log_info "Backing up generated configuration files..."
    mkdir -p backup
    if [[ -f "conf/base/vertexai.yml" ]]; then
        cp "conf/base/vertexai.yml" "backup/vertexai.yml.backup"
    fi
    if [[ -f "conf/base/catalog.yml" ]]; then
        cp "conf/base/catalog.yml" "backup/catalog.yml.backup"
    fi
    
    # Update VertexAI configuration with environment variables
    create_vertexai_config "$case"
    
    # Copy test-specific overwrite files (after base config creation)
    if [[ -d "$overwrite_path" ]]; then
        log_info "Applying test-specific configuration overrides..."
        cp -r "$overwrite_path"/* .
        
        # Update override files with environment variables
        if [[ -f "conf/base/vertexai.yml" ]]; then
            log_info "Updating vertexai.yml with environment variables..."
            sed -i '' "s/project_id: .*/project_id: $GCP_PROJECT_ID/g" conf/base/vertexai.yml
            sed -i '' "s/region: .*/region: $GCP_REGION/g" conf/base/vertexai.yml
            sed -i '' "s|root: .*|root: $GCS_BUCKET_ROOT|g" conf/base/vertexai.yml
            sed -i '' "s/service_account: .*/service_account: $VERTEX_AI_SERVICE_ACCOUNT/g" conf/base/vertexai.yml
            sed -i '' "s/experiment_name: .*/experiment_name: $EXPERIMENT_NAME-$case/g" conf/base/vertexai.yml
            sed -i '' "s/scheduled_run_name: .*/scheduled_run_name: $EXPERIMENT_NAME-$case-\${oc.env:KEDRO_CONFIG_COMMIT_ID,unknown-commit}/g" conf/base/vertexai.yml
        fi
    fi
    
    # Configure settings.py for environment variable resolution
    sed -i '' "s/\(CONFIG_LOADER_ARGS.*\)$/from omegaconf.resolvers import oc\n\1\n      \"custom_resolvers\": { \"oc.env\": oc.env },/g" src/spaceflights/settings.py

    # Build and push Docker image
    local full_image_name="$VERTEX_AI_DOCKER_REGISTRY/$DOCKER_IMAGE_TAG-$case"
    log_info "Building and pushing Docker image: $full_image_name"
    docker build --no-cache --platform linux/amd64 -t "$full_image_name" .
    docker push "$full_image_name"
    
    # Update vertexai.yml with the built image
    update_vertexai_config_image "$full_image_name"
    
    # Run the pipeline on Vertex AI
    log_info "Running pipeline on Vertex AI..."
    export KEDRO_CONFIG_COMMIT_ID="test-$(date +%s)"
    kedro vertexai run-once --wait-for-completion
    
    # Return to original directory
    cd "$project_root"
    
    log_success "E2E test case '$case' completed successfully"
    log_info "Test artifacts preserved in: $test_dir"
    log_info "Docker image: $full_image_name"
}

create_vertexai_config() {
    local case=$1
    
    cat > conf/base/vertexai.yml << EOF
project_id: $GCP_PROJECT_ID
region: $GCP_REGION
run_config:
  # Name of the image to run as the pipeline steps (will be updated later)
  image: placeholder
  
  # Location of Vertex AI GCS root
  root: $GCS_BUCKET_ROOT
  
  # Name of the kubeflow experiment to be created
  experiment_name: $EXPERIMENT_NAME-$case
  
  # Name of the scheduled run, templated with the schedule parameters
  scheduled_run_name: $EXPERIMENT_NAME-$case-\${oc.env:KEDRO_CONFIG_COMMIT_ID,unknown-commit}
  
  # Service account to run vertex AI Pipeline with
  service_account: $VERTEX_AI_SERVICE_ACCOUNT
  
  # Pipeline description
  description: "Kedro VertexAI E2E Test - $case"
  
  # How long to keep underlying Argo workflow (together with pods and data
  # volume after pipeline finishes) [in seconds]. Default: 1 week
  ttl: 604800

  # Optional section allowing adjustment of the resources, reservations and limits
  # for the nodes. When not provided they're set to 500m cpu and 1024Mi memory.
  resources:
    # Default settings for the nodes
    __default__:
      cpu: 500m
      memory: 1024Mi
EOF
}

update_vertexai_config_image() {
    local image_name=$1
    sed -i '' "s|image: placeholder|image: $image_name|g" conf/base/vertexai.yml
}

main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done
    
    log_info "Starting kedro-vertexai E2E test runner..."
    log_info "Configuration:"
    log_info "  E2E cases: $E2E_CASES"
    log_info "  Kedro starter version: $KEDRO_STARTER_VERSION"
    log_info "  GCP Project: $GCP_PROJECT_ID"
    log_info "  GCP Region: $GCP_REGION"
    log_info "  Experiment name: $EXPERIMENT_NAME"
    
    # Check requirements
    check_requirements
    
    # Set up Python environment
    log_info "Setting up Python environment..."
    python --version
    
    # Run E2E tests
    run_e2e_tests
    
    log_success "All E2E tests completed successfully! 🎉"
    log_info "Note: Test resources and artifacts have been preserved for debugging"
}

# Run main function
main "$@"