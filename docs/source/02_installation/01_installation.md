# Installation guide

## Kedro setup

First, you need to install base Kedro package

```console
$ pip install "kedro>=0.18.1,<0.19.0"
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

You can check available commands by going into project directory and running:

```console
$ kedro vertexai
Usage: kedro vertexai [OPTIONS] COMMAND [ARGS]...

  Interact with Google Cloud Platform :: Vertex AI Pipelines

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile         Translates Kedro pipeline into JSON file with VertexAI...
  init            Initializes configuration for the plugin
  list-pipelines  List deployed pipeline definitions
  run-once        Deploy pipeline as a single run within given experiment...
  schedule        Schedules recurring execution of latest version of the...
  ui              Open VertexAI Pipelines UI in new browser tab
```

````{warning}
`vertexai` sub-command group only becomes visible when used inside kedro project context. Make sure that you're inside one, in case you see the message:
```
Error: No such command 'vertexai'.
```
````

### `init`

`init` command takes two arguments: `PROJECT_ID` and `REGION`. This command generates a sample
configuration file in `conf/base/vertexai.yaml`. The YAML file content is described in the 
[Configuration section](../02_installation/02_configuration.md).

### `ui`

`ui` command opens a web browser pointing to the currently configured Vertex AI Pipelines UI on GCP web console.

### `list-pipelines`

`list-pipelines` uses Vertex AI API to retrieve list of all pipelines

### `compile`

`compile` transforms Kedro pipeline into Vertex AI workflow. The
resulting `json` file can be uploaded to Vertex AI Pipelines via [Python Client](https://cloud.google.com/vertex-ai/docs/pipelines/build-pipeline#submit_your_pipeline_run) e.g. from your CI/CD job.

### `run-once`

`run-once` is all-in-one command to compile the pipeline and run it in the GCP Vertex AI Pipelines environment.
