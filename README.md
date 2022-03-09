# Kedro Vertex AI Plugin

### THIS PLUGIN IS A WORK IN PROGRESS NOT READY TO BE PUBLISHED YET


[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8-blue.svg)](https://github.com/getindata/kedro-kubeflow)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) 

## About

The main purpose of this plugin is to enable running kedro pipeline on Google Cloud Platform - Vertex AI Pipelines.
It supports translation from Kedro pipeline DSL to [kfp](https://www.kubeflow.org/docs/pipelines/sdk/sdk-overview/) 
(pipelines SDK) and deployment to Vertex AI service with some convenient commands.

The plugin can be used together with `kedro-docker` to simplify preparation of docker image for pipeline execution.   
