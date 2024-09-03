import os
from functools import cached_property

from kedro.config import (
    AbstractConfigLoader,
    MissingConfigException,
    OmegaConfigLoader,
)
from omegaconf import DictConfig, OmegaConf

from kedro_vertexai.client import VertexAIPipelinesClient

from .config import PluginConfig


class ContextHelper(object):

    CONFIG_FILE_PATTERN = "vertexai*"
    CONFIG_KEY = "vertexai"

    def __init__(self, metadata, env):
        self._metadata = metadata
        self._env = env

    @property
    def project_name(self):
        return self._metadata.project_name

    @cached_property
    def session(self):
        from kedro.framework.session import KedroSession

        return KedroSession.create(self._metadata.project_path, env=self._env)

    @cached_property
    def context(self):
        assert self.session is not None, "Session not initialized"
        return self.session.load_context()

    def _ensure_obj_is_dict(self, obj):
        if isinstance(obj, DictConfig):
            obj = OmegaConf.to_container(obj)
        elif isinstance(obj, dict) and any(
            isinstance(v, DictConfig) for v in obj.values()
        ):
            obj = {
                k: (OmegaConf.to_container(v) if isinstance(v, DictConfig) else v)
                for k, v in obj.items()
            }
        return obj

    @cached_property
    def config(self) -> PluginConfig:
        cl: AbstractConfigLoader = self.context.config_loader
        try:
            if self.CONFIG_KEY not in cl.config_patterns.keys():
                cl.config_patterns.update(
                    {
                        self.CONFIG_KEY: [
                            self.CONFIG_FILE_PATTERN,
                            f"{self.CONFIG_FILE_PATTERN}/**",
                        ]
                    }
                )
            vertex_conf = self._ensure_obj_is_dict(cl.get(self.CONFIG_KEY))
        except MissingConfigException:
            if not isinstance(cl, OmegaConfigLoader):
                raise ValueError(
                    f"You're using a custom config loader: {cl.__class__.__qualname__}{os.linesep}"
                    f"you need to add the {self.CONFIG_KEY} config to it.{os.linesep}"
                    f"Make sure you add {self.CONFIG_FILE_PATTERN} to config_pattern in CONFIG_LOADER_ARGS "
                    f"in the settings.py file.{os.linesep}"
                    """Example:
CONFIG_LOADER_ARGS = {
    # other args
    "config_patterns": {"vertexai": ["vertexai*"]}
}
                    """.strip()
                )
            else:
                raise ValueError(
                    "Missing vertexai.yml files in configuration. "
                    "Make sure that you configure your project first"
                )
        return PluginConfig.model_validate(vertex_conf)

    @cached_property
    def vertexai_client(self) -> VertexAIPipelinesClient:
        return VertexAIPipelinesClient(self.config, self.project_name, self.context)

    @staticmethod
    def init(metadata, env):
        return ContextHelper(metadata, env)
