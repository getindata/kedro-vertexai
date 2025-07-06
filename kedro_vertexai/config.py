import logging
import os
from importlib import import_module
from inspect import signature
from typing import Dict, List, Optional

from pydantic import BaseModel, field_validator
from pydantic.networks import IPvAnyAddress

DEFAULT_CONFIG_TEMPLATE = """
# Configuration used to run the pipeline
project_id: {project_id}
region: {region}
run_config:
  # Name of the image to run as the pipeline steps
  image: {image}

  # Location of Vertex AI GCS root
  root: bucket_name/gcs_suffix

  # Name of the Vertex AI experiment to be created
  experiment_name: {project}-experiment

  # Optional description of the Vertex AI experiment to be created
  # experiment_description: "My experiment description."

  # Name of the scheduled run, templated with the schedule parameters
  scheduled_run_name: {run_name}

  # Optional service account to run vertex AI Pipeline with
  # service_account: pipelines-account@my-project.iam.gserviceaccount.com

  # Optional pipeline description
  # description: "Very Important Pipeline"

  # Optional config for node execution grouping. - 2 classes are provided:
  # - default no-grouping option IdentityNodeGrouper
  # - tag based grouping with TagNodeGrouper
  grouping:
    cls: kedro_vertexai.grouping.IdentityNodeGrouper
    # cls: kedro_vertexai.grouping.TagNodeGrouper
    # params:
        # tag_prefix: "group."

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
  # for the nodes. You can specify node names or tags to select which nodes the requirements
  # apply to (also in node selectors). When not provided they're set to 500m cpu and 1024Mi memory.
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

  # Schedules configuration
  schedules:
    default_schedule:
      cron_expression: "0 * * * *"
      timezone: Etc/UTC
      start_time: none
      end_time: none
      allow_queueing: false
      max_run_count: none
      max_concurrent_run_count: 1
    # training_pipeline:
    #   cron_expression: "0 0 * * *"
    #   timezone: America/New_York
    #   start_time: none
    #   end_time: none
    #   allow_queueing: false
    #   max_run_count: none
    #   max_concurrent_run_count: 1

  # Optional distributed training configuration
  # distributed_training:
  #   # Enable distributed training for specific node names
  #   enabled_for_node_names:
  #     - "training_node"
  #     - "model_training"
  #   
  #   # Enable distributed training for nodes with specific tags
  #   enabled_for_tags:
  #     - "distributed"
  #     - "gpu-intensive"
  #   
  #   # Primary replica configuration (must have replica_count = 1)
  #   primary_pool:
  #     machine_type: "n1-standard-4"
  #     replica_count: 1
  #     accelerator_type: "NVIDIA_TESLA_T4"
  #     accelerator_count: 1
  #   
  #   # Worker pool configuration (can have replica_count > 1)
  #   worker_pool:
  #     machine_type: "n1-standard-4"
  #     replica_count: 2
  #     accelerator_type: "NVIDIA_TESLA_T4"
  #     accelerator_count: 1
  #   
  #   # Base output directory for distributed training jobs
  #   base_output_directory: "gs://your-bucket/distributed-training-output/"
"""


logger = logging.getLogger(__name__)


def dynamic_load_class(load_class):
    try:
        module_name, class_name = load_class.rsplit(".", 1)
        logger.info(f"Initializing {class_name}")
        class_load = getattr(import_module(module_name), class_name)
        return class_load
    except:  # noqa: E722
        logger.error(
            f"Could not dynamically load class {load_class}, "
            f"make sure it's valid and accessible from the current Python interpreter",
            exc_info=True,
        )
    return None


def dynamic_init_class(load_class, *args, **kwargs):
    if args is None:
        args = []
    if kwargs is None:
        kwargs = {}
    try:
        loaded_class = dynamic_load_class(load_class)
        if loaded_class is None:
            return None
        return loaded_class(*args, **kwargs)
    except:  # noqa: E722
        logger.error(
            f"Could not dynamically init class {load_class} with its init params, "
            f"make sure the configured params match the ",
            exc_info=True,
        )


class GroupingConfig(BaseModel):
    cls: str = "kedro_vertexai.grouping.IdentityNodeGrouper"
    params: Optional[dict] = {}

    @field_validator("cls")
    def class_valid(cls, v, values, **kwargs):
        try:
            grouper_class = dynamic_load_class(v)
            if grouper_class is None:
                raise ValueError(f"Could not load grouping class {v}")
            class_sig = signature(grouper_class)
            if "params" in values.data:
                class_sig.bind(None, **values.data["params"])
            else:
                class_sig.bind(None)
        except:  # noqa: E722
            raise ValueError(
                f"Invalid parameters for grouping class {v}, validation failed."
            )
        return v

    # @computed_field
    # @cached_property
    # def used_provider(self):
    #     load_class = dynamic_load_class(self.cls)
    #     # fail gracefully here if wrong params are provided here?
    #     self._grouping_object = load_class(**self.params)
    #     return self._grouping_object


class HostAliasConfig(BaseModel):
    ip: IPvAnyAddress
    hostnames: List[str]


class ResourcesConfig(BaseModel):
    cpu: Optional[str] = None
    gpu: Optional[str] = None
    memory: Optional[str] = None


class NetworkConfig(BaseModel):
    vpc: Optional[str] = None
    host_aliases: Optional[List[HostAliasConfig]] = []


class DynamicConfigProviderConfig(BaseModel):
    cls: str
    params: Optional[Dict[str, str]] = {}


class MLFlowVertexAIConfig(BaseModel):
    request_header_provider_params: Optional[Dict[str, str]] = None


class ScheduleConfig(BaseModel):
    cron_expression: Optional[str] = "0 * * * *"
    timezone: Optional[str] = "Etc/UTC"
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    allow_queueing: Optional[bool] = False
    max_run_count: Optional[int] = None
    max_concurrent_run_count: Optional[int] = 1


class WorkerPoolConfig(BaseModel):
    machine_type: str = "n1-standard-4"
    replica_count: int = 1
    accelerator_type: Optional[str] = None
    accelerator_count: Optional[int] = None


class DistributedTrainingConfig(BaseModel):
    enabled_for_node_names: Optional[List[str]] = []
    enabled_for_tags: Optional[List[str]] = []
    primary_pool: Optional[WorkerPoolConfig] = WorkerPoolConfig()
    worker_pool: Optional[WorkerPoolConfig] = WorkerPoolConfig(replica_count=2)
    base_output_directory: Optional[str] = None


class RunConfig(BaseModel):
    image: str
    root: Optional[str] = None
    description: Optional[str] = None
    experiment_name: str
    experiment_description: Optional[str] = None
    scheduled_run_name: Optional[str] = None
    grouping: Optional[GroupingConfig] = GroupingConfig()
    service_account: Optional[str] = None
    network: Optional[NetworkConfig] = NetworkConfig()
    ttl: int = 3600 * 24 * 7
    resources: Optional[Dict[str, ResourcesConfig]] = dict(
        __default__=ResourcesConfig(cpu="500m", memory="1024Mi")
    )
    node_selectors: Optional[Dict[str, Dict[str, str]]] = {}
    dynamic_config_providers: Optional[List[DynamicConfigProviderConfig]] = []
    mlflow: Optional[MLFlowVertexAIConfig] = None
    schedules: Optional[Dict[str, ScheduleConfig]] = None
    distributed_training: Optional[DistributedTrainingConfig] = None

    def resources_for(self, node: str, tags: Optional[set] = None):
        if self.resources is None:
            return {}
        default_config = self.resources["__default__"].dict()
        return self._config_for(node, tags or set(), self.resources, default_config)

    def node_selectors_for(self, node: str, tags: Optional[set] = None):
        if self.node_selectors is None:
            return {}
        return self._config_for(node, tags or set(), self.node_selectors)

    def should_use_distributed_training(self, node: str, tags: Optional[set] = None) -> bool:
        """Check if a node should use distributed training based on configuration."""
        if not self.distributed_training:
            return False
        
        tags = tags or set()
        
        # Check node names
        if node in (self.distributed_training.enabled_for_node_names or []):
            return True
            
        # Check tags
        if any(tag in (self.distributed_training.enabled_for_tags or []) for tag in tags):
            return True
            
        return False

    @staticmethod
    def _config_for(
        node: str, tags: set, params: dict, default_config: Optional[dict] = None
    ):
        tags = tags or set()
        names = [*tags, node]
        filled_names = [x for x in names if x in params.keys()]
        results = default_config or {}
        for name in filled_names:
            configs = (
                params[name] if isinstance(params[name], dict) else params[name].dict()
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
