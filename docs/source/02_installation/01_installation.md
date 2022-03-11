# Installation guide

## Kedro setup

First, you need to install base Kedro package

```console
$ pip install 'kedro'
```

## Plugin installation

### Install from PyPI

You can install ``kedro-vertexai`` plugin from ``PyPi`` with `pip`:

```console
pip install --upgrade kedro-vertexai
```

### Install from sources

You may want to install the develop branch which has unreleased features:

```console
pip install git+https://github.com/getindata/kedro-vertexai.git@develop
```

## Available commands

You can check available commands by going into project directory and runnning:

```console
$ kedro vertexai
Usage: kedro vertexai [OPTIONS] COMMAND [ARGS]...

  Interact with GCP Vertex AI Pipelines

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile          Translates Kedro pipeline into YAML file with Kubeflow...
  init             Initializes configuration for the plugin
  list-pipelines   List deployed pipeline definitions
  run-once         Deploy pipeline as a single run within given experiment.
  schedule         Schedules recurring execution of latest version of the...
  ui               Open Kubeflow Pipelines UI in new browser tab
  upload-pipeline  Uploads pipeline to Kubeflow server
```

### `init`

`init` command takes one argument (that is the kubeflow pipelines root url) and generates sample
configuration file in `conf/base/vertexai.yaml`. The YAML file content is described in the 
[Configuration section](../02_installation/02_configuration.md).

### `ui`

`ui` command opens a web browser pointing to the currently configured VertexAI Pipelines UI on GCP web console.

### `list-pipelines`

`list-pipelines` uses Kubeflow Pipelines to retrieve all registered pipelines

### `compile`

`compile` transforms Kedro pipeline into Vertex AI workflow. The
resulting `yaml` file can be uploaded to Vertex AI Pipelines via web UI.

### `run-once`

`run-once` is all-in-one command to compile the pipeline and run it in the GCP Vertex AI Pipelines environment.
