# Kedro Vertex AI Plugin

[![Python Version](https://img.shields.io/badge/python-3.8-blue.svg)](https://github.com/getindata/kedro-vertexai)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 
[![SemVer](https://img.shields.io/badge/semver-2.0.0-green)](https://semver.org/)
[![PyPI version](https://badge.fury.io/py/kedro-vertexai.svg)](https://pypi.org/project/kedro-vertexai/)
[![Downloads](https://pepy.tech/badge/kedro-vertexai)](https://pepy.tech/project/kedro-vertexai)

[![Maintainability](https://api.codeclimate.com/v1/badges/a2ef6b63553ed42c9031/maintainability)](https://codeclimate.com/github/getindata/kedro-vertexai/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a2ef6b63553ed42c9031/test_coverage)](https://codeclimate.com/github/getindata/kedro-vertexai/test_coverage)
[![Documentation Status](https://readthedocs.org/projects/kedro-vertexai/badge/?version=latest)](https://kedro-vertexai.readthedocs.io/en/latest/?badge=latest)

## About

The main purpose of this plugin is to enable running kedro pipeline on Google Cloud Platform - Vertex AI Pipelines.
It supports translation from Kedro pipeline DSL to [kfp](https://www.kubeflow.org/docs/pipelines/sdk/sdk-overview/) 
(pipelines SDK) and deployment to Vertex AI service with some convenient commands.

The plugin can be used together with `kedro-docker` to simplify preparation of docker image for pipeline execution.   

## Documentation

For detailed documentation refer to https://kedro-vertexai.readthedocs.io/

## Usage guide 

```
Usage: kedro vertexai [OPTIONS] COMMAND [ARGS]...

  Interact with Google Cloud Platform :: Vertex AI Pipelines

Options:
  -e, --env TEXT  Environment to use.
  -h, --help      Show this message and exit.

Commands:
  compile         Translates Kedro pipeline into YAML file with Kubeflow...
  init            Initializes configuration for the plugin
  list-pipelines  List deployed pipeline definitions
  run-once        Deploy pipeline as a single run within given experiment.
  schedule        Schedules recurring execution of latest version of the...
  ui              Open VertexAI Pipelines UI in new browser tab
```

## Configuration file

`kedro init` generates configuration file for the plugin, but users may want to adjust it to match the run environment 
requirements. Check documentation for details - [kedro-kubeflow.readthedocs.io](https://kedro-kubeflow.readthedocs.io/en/latest/source/02_installation/02_configuration.html)
