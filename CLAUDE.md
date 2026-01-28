# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kedro plugin enabling pipeline execution on Google Cloud Vertex AI Pipelines. Translates Kedro DSL to Kubeflow Pipelines (KFP).

## Development Commands

```bash
# Setup
poetry install --all-extras
pre-commit install

# Testing
poetry run pytest --cov kedro_vertexai --cov-report term-missing
tox  # Cross-version testing
./dev-utils/run_tests.sh  # E2E tests (requires GCP)

# Code quality
pre-commit run --all-files
```

## Architecture

### Core Components
- `cli.py` - Commands: `compile`, `run-once`, `list-pipelines`, `init`, `ui`
- `client.py` - Vertex AI Pipelines deployment client
- `generator.py` - Kedro to KFP translation
- `config.py` - Pydantic models, configuration templates
- `utils.py` - Docker operations, config materialization

### Key Concepts
- **Pipeline Translation**: Node grouping via `grouping.py` (Identity/Tag-based)
- **Authentication**: GCP auth in `auth/gcp.py`, MLflow integration
- **Configuration**: Template-based, expects `conf/base/vertexai.yml`

## CLI Usage

```bash
kedro vertexai init                                    # Initialize config
kedro vertexai compile [--image IMG] [--params k:t]   # Compile to pipeline.yml
kedro vertexai run-once [--image IMG] [--params k=v]  # Deploy and run
kedro vertexai list-pipelines                         # List deployments
kedro vertexai ui                                      # Open UI
```

## Environment Setup

- Development config: env variables or `dev-utils/local.env` (see `dev-utils/README.md`)
- Authentication: `gcloud auth login` + `gcloud auth application-default login` unless you're already authorized

## Debugging Vertex AI Pipelines

```bash
# Find errors
gcloud logging read "PIPELINE_NAME_OR_ID AND severity=ERROR" --project=PROJECT --limit=10

# Get job logs (extract job_id from errors first)
gcloud logging read "resource.type=ml_job AND resource.labels.job_id=JOB_ID" \
  --project=PROJECT --format=json | jq -r '.[] | select(.jsonPayload.message) | .jsonPayload.message'

# Get pipeline definition
kedro vertexai compile  # Output: pipeline.yml
```

## Dependencies
- See `pyproject.toml` for current versions and constraints
- Python support: Check `pyproject.toml` python field
- Optional MLflow integration via `mlflow` extra

## Development Workflow
- Check CONTRIBUTING.md for PR and release guidelines
- Code style: See `.pre-commit-config.yaml` for formatting rules