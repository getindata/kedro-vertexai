import os
from typing import Dict, List, Optional

from pydantic import BaseModel
from pydantic.networks import IPvAnyAddress

DEFAULT_CONFIG_TEMPLATE = """
# Configuration used to run the pipeline
project_id: {project_id}
region: {region}
run_config:
  # Name of the image to run as the pipeline steps
  image: {image}

  # Pull policy to be used for the steps. Use Always if you push the images
  # on the same tag, or Never if you use only local images
  image_pull_policy: IfNotPresent

  # Location of Vertex AI GCS root
  root: bucket_name/gcs_suffix

  # Prefix of Vertex AI pipeline run
  experiment_name: {project}

  # Name of the scheduled run, templated with the schedule parameters
  scheduled_run_name: {run_name}

  # Optional service account to run vertex AI Pipeline with
  # service_account: pipelines-account@my-project.iam.gserviceaccount.com

  # Optional pipeline description
  # description: "Very Important Pipeline"

  # How long to keep underlying Argo workflow (together with pods and data
  # volume after pipeline finishes) [in seconds]. Default: 1 week
  ttl: 604800

  # Optional network configuration
  # network:

    # Name of the vpc to use for running Vertex Pipeline
    # vpc: my-vpc

    # Hosts aliases to be placed in /etc/hosts when pipeline is executed
    # host_aliases:
    #  - ip: 127.0.0.1
    #    hostnames:
    #     - me.local

  # What Kedro pipeline should be run as the last step regardless of the
  # pipeline status. Used to send notifications or raise the alerts
  # on_exit_pipeline: notify_via_slack

  # Optional section allowing adjustment of the resources, reservations and limits
  # for the nodes. When not provided they're set to 500m cpu and 1024Mi memory.
  # If you don't want to specify pipeline resources set both to None in __default__.
  resources:

    # For nodes that require more RAM you can increase the "memory"
    data_import_step:
      memory: 4Gi

    # Training nodes can utilize more than one CPU if the algoritm
    # supports it
    model_training:
      cpu: 8
      memory: 8Gi
      gpu: 1

    # Default settings for the nodes
    __default__:
      cpu: 1000m
      memory: 2048Mi

  node_selectors:
    model_training:
      cloud.google.com/gke-accelerator: NVIDIA_TESLA_T4

  # Optional section allowing to generate config files at runtime,
  # useful e.g. when you need to obtain credentials dynamically and store them in credentials.yaml
  # but the credentials need to be refreshed per-node
  # (which in case of Vertex AI would be a separate container / machine)
  # Example:
  # dynamic_config_providers:
  #  - cls: kedro_vertexai.auth.gcp.MLFlowGoogleOAuthCredentialsProvider
  #    params:
  #      client_id: iam-client-id

  dynamic_config_providers: []

  # Additional configuration for MLflow request header providers, e.g. to generate access tokens at runtime
  # mlflow:
  #   request_header_provider_params:
  #       key: value
"""


class HostAliasConfig(BaseModel):
    ip: IPvAnyAddress
    hostnames: List[str]


class ResourcesConfig(BaseModel):
    cpu: Optional[str]
    gpu: Optional[str]
    memory: Optional[str]


class NetworkConfig(BaseModel):
    vpc: Optional[str]
    host_aliases: Optional[List[HostAliasConfig]] = []


class DynamicConfigProviderConfig(BaseModel):
    cls: str
    params: Optional[Dict[str, str]] = {}


class MLFlowVertexAIConfig(BaseModel):
    request_header_provider_params: Optional[Dict[str, str]]


class RunConfig(BaseModel):
    image: str
    image_pull_policy: Optional[str] = "IfNotPresent"
    root: Optional[str]
    description: Optional[str]
    experiment_name: str
    scheduled_run_name: Optional[str]
    service_account: Optional[str]
    network: Optional[NetworkConfig] = NetworkConfig()
    ttl: int = 3600 * 24 * 7
    resources: Optional[Dict[str, ResourcesConfig]] = dict(
        __default__=ResourcesConfig(cpu="500m", memory="1024Mi")
    )
    node_selectors: Optional[Dict[str, Dict[str, str]]] = {}
    dynamic_config_providers: Optional[List[DynamicConfigProviderConfig]] = []
    mlflow: Optional[MLFlowVertexAIConfig] = None

    def resources_for(self, node: str, tags: Optional[set] = None):
        default_config = self.resources["__default__"].dict()
        return self._config_for(node, tags, self.resources, default_config)

    def node_selectors_for(self, node: str, tags: Optional[set] = None):
        return self._config_for(node, tags, self.node_selectors)

    @staticmethod
    def _config_for(
        node: str, tags: set, params: dict, default_config: Optional[dict] = None
    ):
        tags = tags or set()
        names = [*tags, node]
        filled_names = [x for x in names if x in params.keys()]
        results = default_config or {}
        if filled_names:
            for name in filled_names:
                configs = (
                    params[name]
                    if isinstance(params[name], dict)
                    else params[name].dict()
                )
                results.update({k: v for k, v in configs.items() if v is not None})
        return results


class KedroVertexAIRunnerConfig(BaseModel):
    # This is intentionally a separate dataclass, for future extensions
    storage_root: str


class PluginConfig(BaseModel):
    project_id: str
    region: str
    run_config: RunConfig

    @staticmethod
    def sample_config(**kwargs):
        return DEFAULT_CONFIG_TEMPLATE.format(**kwargs)

    @staticmethod
    def initialize_github_actions(project_name, where, templates_dir):
        os.makedirs(where / ".github/workflows", exist_ok=True)
        for template in ["on-push.yml"]:
            file_path = where / ".github/workflows" / template
            template_file = templates_dir / f"github-{template}"
            with open(template_file, "r") as tfile, open(file_path, "w") as f:
                f.write(tfile.read().format(project_name=project_name))
