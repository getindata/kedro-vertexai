import os

from kedro.config import MissingConfigException

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

  # Name of the kubeflow experiment to be created
  experiment_name: {project}

  # Name of the scheduled run, templated with the schedule parameters
  scheduled_run_name: {run_name}

  # Optional pipeline description
  #description: "Very Important Pipeline"

  # How long to keep underlying Argo workflow (together with pods and data
  # volume after pipeline finishes) [in seconds]. Default: 1 week
  ttl: 604800

  # What Kedro pipeline should be run as the last step regardless of the
  # pipeline status. Used to send notifications or raise the alerts
  # on_exit_pipeline: notify_via_slack

  # Optional section allowing adjustment of the resources
  # reservations and limits for the nodes
  resources:

    # For nodes that require more RAM you can increase the "memory"
    data_import_step:
      memory: 2Gi

    # Training nodes can utilize more than one CPU if the algoritm
    # supports it
    model_training:
      cpu: 8
      memory: 1Gi

    # GPU-capable nodes can request 1 GPU slot
    tensorflow_step:
      nvidia.com/gpu: 1

    # Default settings for the nodes
    __default__:
      cpu: 200m
      memory: 64Mi
"""


class Config(object):
    def __init__(self, raw):
        self._raw = raw

    def _get_or_default(self, prop, default):
        return self._raw.get(prop, default)

    def _get_or_fail(self, prop):
        if prop in self._raw.keys():
            return self._raw[prop]
        else:
            raise MissingConfigException(
                f"Missing required configuration: '{self._get_prefix()}{prop}'."
            )

    def _get_prefix(self):
        return ""

    def __eq__(self, other):
        if isinstance(other, Config):
            return self._raw == other._raw
        else:
            return False


class VertexAiNetworkingConfig(Config):
    @property
    def vpc(self):
        return self._get_or_default("vpc", None)

    @property
    def host_aliases(self):
        aliases = self._get_or_default("host_aliases", [])
        return {alias["ip"]: alias["hostnames"] for alias in aliases}


class NodeResources(Config):
    def is_set_for(self, node_name):
        return self.get_for(node_name) != {}

    def get_for(self, node_name):
        defaults = self._get_or_default("__default__", {})
        node_specific = self._get_or_default(node_name, {})
        return {**defaults, **node_specific}


class RunConfig(Config):
    @property
    def image(self):
        return self._get_or_fail("image")

    @property
    def image_pull_policy(self):
        return self._get_or_default("image_pull_policy", "IfNotPresent")

    @property
    def root(self):
        return self._get_or_fail("root")

    @property
    def experiment_name(self):
        return self._get_or_fail("experiment_name")

    @property
    def scheduled_run_name(self):
        return self._get_or_default(
            "scheduled_run_name", self._get_or_fail("experiment_name")
        )

    @property
    def description(self):
        return self._get_or_default("description", None)

    @property
    def resources(self):
        return NodeResources(self._get_or_default("resources", {}))

    @property
    def store_kedro_outputs_as_kfp_artifacts(self):
        return bool(
            self._get_or_default("store_kedro_outputs_as_kfp_artifacts", True)
        )

    @property
    def max_cache_staleness(self):
        return str(self._get_or_default("max_cache_staleness", None))

    @property
    def ttl(self):
        return int(self._get_or_default("ttl", 3600 * 24 * 7))

    @property
    def on_exit_pipeline(self):
        return self._get_or_default("on_exit_pipeline", None)

    @property
    def vertex_ai_networking(self):
        return VertexAiNetworkingConfig(
            self._get_or_default("vertex_ai_networking", {})
        )

    @property
    def node_merge_strategy(self):
        strategy = str(self._get_or_default("node_merge_strategy", "none"))
        if strategy not in ["none", "full"]:
            raise ValueError(
                f"Invalid {self._get_prefix()}node_merge_strategy: {strategy}"
            )
        else:
            return strategy

    def _get_prefix(self):
        return "run_config."


class PluginConfig(Config):
    @property
    def run_config(self) -> RunConfig:
        cfg = self._get_or_fail("run_config")
        return RunConfig(cfg)

    @staticmethod
    def sample_config(**kwargs):
        return DEFAULT_CONFIG_TEMPLATE.format(**kwargs)

    @property
    def project_id(self):
        return self._get_or_fail("project_id")

    @property
    def region(self):
        return self._get_or_fail("region")

    @staticmethod
    def initialize_github_actions(project_name, where, templates_dir):
        os.makedirs(where / ".github/workflows", exist_ok=True)
        for template in ["on-push.yml"]:
            file_path = where / ".github/workflows" / template
            template_file = templates_dir / f"github-{template}"
            with open(template_file, "r") as tfile, open(file_path, "w") as f:
                f.write(tfile.read().format(project_name=project_name))
