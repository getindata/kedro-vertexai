# GitHub Workflows Setup

## Overview

This repository uses GitHub Actions for CI/CD and GitHub Agentic Workflows for automated Dependabot PR management.

| Workflow | File | Purpose |
|---|---|---|
| Test & Publish | `workflows/test_and_publish.yml` | Unit tests, SonarCloud, CodeQL, E2E tests, PyPI publish |
| Dependabot Auto-Manager | `workflows/dependabot-auto.yml` | Orchestrates auto-merge, review requests, and fix dispatching |
| Dependabot Fix Agent | `workflows/dependabot-fix.md` | AI agent that analyzes and fixes failing CI on Dependabot PRs |
| OK to Test | `workflows/ok-to-test.yml` | `/ok-to-test` slash command for forked PRs |
| Prepare Release | `workflows/prepare-release.yml` | Version bump and release PR creation |
| Spellcheck | `workflows/spellcheck.yml` | Documentation spell checking |

## Dependabot Auto-Manager Setup

The Dependabot Auto-Manager automatically handles Dependabot PRs after CI completes:

- **Non-core deps + CI passes** → squash-merged automatically
- **Core deps + CI passes** → comments tagging `@em-pe` with changelog summary for manual review
- **CI fails** → dispatches the AI fix agent (up to 3 attempts)
- **3 failed attempts** → comments tagging `@em-pe` for manual intervention

Core dependencies (require manual review): `kedro`, `kfp`, `google-cloud-aiplatform`

### Prerequisites

- GitHub CLI v2.0.0+ (`gh --version`)
- GitHub Actions enabled in the repository
- An AI engine account (GitHub Copilot, Anthropic Claude, or OpenAI)

### Step 1: Install `gh aw` extension

```bash
gh extension install github/gh-aw
```

### Step 2: Initialize the repository

```bash
gh aw init
```

### Step 3: Set up AI engine secrets

Choose one AI engine and add the corresponding secret in **Settings → Secrets and variables → Actions**:

| Engine | Secret Name | Where to get it |
|---|---|---|
| GitHub Copilot | `COPILOT_GITHUB_TOKEN` | GitHub Copilot subscription |
| Anthropic Claude | `ANTHROPIC_API_KEY` | [console.anthropic.com](https://console.anthropic.com) |
| OpenAI | `OPENAI_API_KEY` | [platform.openai.com](https://platform.openai.com) |

### Step 4: Compile the agentic workflow

```bash
gh aw compile
```

This generates `dependabot-fix.lock.yml` from `dependabot-fix.md`. Commit both files.

### Step 5: Enable GitHub Actions PR permissions

Go to **Settings → Actions → General → Workflow permissions**:

- Select "Read and write permissions"
- Check "Allow GitHub Actions to create and approve pull requests"

### Step 6: Commit and push

```bash
git add .github/workflows/dependabot-auto.yml \
       .github/workflows/dependabot-fix.md \
       .github/workflows/dependabot-fix.lock.yml
git commit -m "Add Dependabot auto-manager with agentic fix workflow"
git push
```

## Existing Workflow Secrets

These secrets are required by the existing CI/CD workflows:

| Secret | Used by | Purpose |
|---|---|---|
| `SONARCLOUD_TOKEN` | Test & Publish | SonarCloud code analysis |
| `GCP_PROJECT_ID` | Test & Publish | E2E tests on Vertex AI |
| `GCP_SA_EMAIL` | Test & Publish | GCP service account for E2E |
| `GCP_WIF_PROVIDER_ID` | Test & Publish | Workload Identity Federation |
| `PYPI_PASSWORD` | Test & Publish | PyPI package publishing |
| `OK_TO_TEST_APP_ID` | OK to Test | GitHub App for fork testing |
| `OK_TO_TEST_PRIVATE_KEY` | OK to Test | GitHub App private key |

## Customization

### Adding/removing core dependencies

Edit `dependabot-auto.yml`, line with `CORE_DEPS=` — it's a regex pattern matching Dependabot PR titles:

```bash
CORE_DEPS="^Bump (kedro|kfp|google-cloud-aiplatform) "
```

### Changing the fix attempt limit

Edit `dependabot-auto.yml`, the condition `if [ "$ATTEMPT_COUNT" -ge 3 ]` — change `3` to your desired maximum.

### Adjusting the AI agent timeout

Edit `dependabot-fix.md` frontmatter: `timeout-minutes: 30`
