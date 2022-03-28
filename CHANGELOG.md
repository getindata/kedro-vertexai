# Changelog

## [Unreleased]

- Add end 2 end tests based on Kedro Spaceflights quickstart guide from our docs.  
- Move service account configuration from env variables to config file. (#7)
- Refactored config to use `pydantic` for validation instead of homemade code. (#1)
- Add `--wait-for-completion` and `--timeout-seconds` parameters to `run-once` command to wait for the Vertex AI job to complete when launched from CLI

## [0.2.0] - 2022-03-23

-   Added quickstart guide to plugin documentation

## [0.1.0] - 2022-03-15

-   Initial version of **kedro-vertexai** plugin extracted from [kedro-kubeflow v0.6.0](https://github.com/getindata/kedro-kubeflow/tree/0.6.0)

[Unreleased]: https://github.com/getindata/kedro-vertexai/compare/0.2.0...HEAD

[0.2.0]: https://github.com/getindata/kedro-vertexai/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/kedro-vertexai/compare/a04849cfd88d3d6386d99f4494df7de524f12c1e...0.1.0
