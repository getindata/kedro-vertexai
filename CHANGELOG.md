# Changelog

## [Unreleased] 2024-07-29

- Brought back the Vertex AI Pipelines scheduling capability
- Migrated to kfp 2
- Removed `image_pull_policy` parameter from configuration, as it only applies to Kubernetes backend and not Vertex AI,
and it's only available in `kfp-kubernetes` extension package
- Removed `--timeout-seconds` parameter from `run-once` command for now, as in the old version of the plugin exceeding the specified time
didn't alter the remote pipeline execution, and only escaped the local Python processs. The timeout funcionality will be added later on,
with the proper remote pipeline execution handling, and possibly per-task timeout enabled by [the new kfp feature](https://github.com/kubeflow/pipelines/pull/10481).
- Assign pipelines to Vertex AI experiments
- Migrated `pydantic` library to v2
- Migrated to `actions/upload-artifact@v4` in the Github Actions

## [0.11.1] - 2024-07-01

## [0.11.0] - 2024-03-22

-   Applied copier template config for consistency - refactoring and configuration small details
-   Updated dependencies and tested for kedro `0.19.3`
-   Node grouping: Changed convention from `:` to `.` due to kedro limitation on colons in node tags
-   Removed EnvTemplatedConfigLoader that gets replaced by default OmegaConf capabilities

## [0.10.0] - 2023-11-22

-   Added explicite pyarrow dependency to avoid critical vulnerability
-   Updated dependencies and tested for kedro `0.18.14`
-   [Feature 🚀] Node grouping: added option to group multiple Kedro nodes together at execution in single Vertex AI process to allow better optimization - less steps, shorter delays while running Vertex AI nodes and less wasted time of data serialization thanks to possibility to use the MemoryDataset

## [0.9.1] - 2023-08-16

-   Updated dependencies of kedro to `0.18.8`, mlflow to `2.3.2` and others
-   Upgrade dependencies to resolve [GHSA-6628-q6j9-w8vg](https://github.com/advisories/GHSA-6628-q6j9-w8vg).

## [0.9.0] - 2023-05-15

-   Add cache to Kedro's context in the `ContextHelper` class to prevent re-loading
-   Upgrade dependencies to support `kedro>=0.18.8`
-   Add support for `OmegaConfigLoader`
-   Upgrade misc. dependencies
-   Remove deprecated `KedoVertexAIConfigLoaderHook`
-   ⚠️ Change default behaviour of config loader in the plugin to rely on project's one instead of `EnvTemplatedConfigLoader`
-   [Docs 📝] Update documentation
-   Improve E2E tests config

## [0.8.1] - 2022-12-30

-   Add cache to Kedro's context in the `ContextHelper` class to prevent re-loading

## [0.8.0] - 2022-12-09

-   Added support for configuration of resources and node selectors with [Kedro node tags](https://kedro.readthedocs.io/en/stable/nodes_and_pipelines/nodes.html#how-to-tag-a-node)
-   Added support for gpu configuration on Vertex AI (by adding `node_selectors` section and `gpu` resources entry in `vertexai.yml` configuration file)
-   Added --auto-build option to run-once that calls 'docker build' and 'docker push' for you before running the job on VertexAI. It introduces '--yes' option to disable confirmation prompt

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

[Unreleased]: https://github.com/getindata/kedro-vertexai/compare/0.11.1...HEAD

[0.11.1]: https://github.com/getindata/kedro-vertexai/compare/0.11.0...0.11.1

[0.11.0]: https://github.com/getindata/kedro-vertexai/compare/0.10.0...0.11.0

[0.10.0]: https://github.com/getindata/kedro-vertexai/compare/0.9.1...0.10.0

[0.9.1]: https://github.com/getindata/kedro-vertexai/compare/0.9.0...0.9.1

[0.9.0]: https://github.com/getindata/kedro-vertexai/compare/0.8.1...0.9.0

[0.8.1]: https://github.com/getindata/kedro-vertexai/compare/0.8.0...0.8.1

[0.8.0]: https://github.com/getindata/kedro-vertexai/compare/0.7.0...0.8.0

[0.7.0]: https://github.com/getindata/kedro-vertexai/compare/0.6.0...0.7.0

[0.6.0]: https://github.com/getindata/kedro-vertexai/compare/0.5.0...0.6.0

[0.5.0]: https://github.com/getindata/kedro-vertexai/compare/0.4.1...0.5.0

[0.4.1]: https://github.com/getindata/kedro-vertexai/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/getindata/kedro-vertexai/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/kedro-vertexai/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/kedro-vertexai/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/kedro-vertexai/compare/a04849cfd88d3d6386d99f4494df7de524f12c1e...0.1.0
