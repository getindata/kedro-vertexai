# Changelog

## [Unreleased]

- Added --auto-build option to run-once that calls 'docker build' and 'docker push' for you before running the job on VertexAI. It introduces '--yes' option to disable confirmation prompt
## [0.7.0] - 2022-09-08

-   Add better MLflow authorization entrypoints (via Hooks and MLflow Request Header Provider Plugin)

## [0.6.0] - 2022-08-22

-   Add auto-dataset creation, to make intermediate dataset creation transparent to the end-user (no need to explicitly add them in the Data Catalog) (#8)

## [0.5.0] - 2022-07-13

-   Add support for `kedro>=0.18.1,<0.19` (#36)
-   Dependency update `kfp==0.18.1` (#45)
-   Added tests and support for python 3.9 and 3.10 (#37)

## [0.4.1] - 2022-04-14

-   Add missing `initialize-job` for `mlflow-start-run` step, added MLFlowGoogleIAMCredentialsProvider. 

## [0.4.0] - 2022-04-08

-   Add support for list type in parameters (+ restore dynamic parameters functionality (#23))
-   Add support for dynamic configuration generation within VertexAI job (#18)
-   Support config globals via optional `KEDRO_GLOBALS_PATTERN` environment variable in `EnvTemplatedConfigLoader` (#28)

## [0.3.0] - 2022-03-28

-   Fix issues with data catalog namespacing for new spaceflights starter (#19)
-   Add end 2 end tests based on Kedro Spaceflights quickstart guide from our docs.  
-   Move service account configuration from env variables to config file. (#7)
-   Refactored config to use `pydantic` for validation instead of homemade code. (#1)
-   Add `--wait-for-completion` and `--timeout-seconds` parameters to `run-once` command to wait for the Vertex AI job to complete when launched from CLI

## [0.2.0] - 2022-03-23

-   Added quickstart guide to plugin documentation

## [0.1.0] - 2022-03-15

-   Initial version of **kedro-vertexai** plugin extracted from [kedro-kubeflow v0.6.0](https://github.com/getindata/kedro-kubeflow/tree/0.6.0)

[Unreleased]: https://github.com/getindata/kedro-vertexai/compare/0.7.0...HEAD

[0.7.0]: https://github.com/getindata/kedro-vertexai/compare/0.6.0...0.7.0

[0.6.0]: https://github.com/getindata/kedro-vertexai/compare/0.5.0...0.6.0

[0.5.0]: https://github.com/getindata/kedro-vertexai/compare/0.4.1...0.5.0

[0.4.1]: https://github.com/getindata/kedro-vertexai/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/getindata/kedro-vertexai/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/kedro-vertexai/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/kedro-vertexai/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/kedro-vertexai/compare/a04849cfd88d3d6386d99f4494df7de524f12c1e...0.1.0
