import os
import warnings
from functools import cached_property
from typing import Any, Dict

from kedro.config import (
    AbstractConfigLoader,
    ConfigLoader,
    MissingConfigException,
    TemplatedConfigLoader,
)
from omegaconf import DictConfig, OmegaConf

from kedro_vertexai.client import VertexAIPipelinesClient

from .config import PluginConfig
from .constants import KEDRO_GLOBALS_PATTERN


class EnvTemplatedConfigLoader(TemplatedConfigLoader):
    """Config loader that can substitute $(commit_id) and $(branch_name)
    placeholders with information taken from env variables."""

    VAR_PREFIX = "KEDRO_CONFIG_"
    # defaults provided so default variables ${commit_id|dirty} work for some entries
    ENV_DEFAULTS = {"commit_id": None, "branch_name": None, "run_id": ""}

    def __init__(
        self,
        conf_source: str,
        env: str = None,
        runtime_params: Dict[str, Any] = None,
        *,
        base_env: str = "base",
        default_run_env: str = "local",
    ):
        warnings.warn(
            "EnvTemplatedConfigLoader is deprecated and will be removed in future releases, "
            "use kedro.config.omegaconf_config.OmegaConfigLoader instead.",
            DeprecationWarning,
        )
        super().__init__(
            conf_source,
            env=env,
            runtime_params=runtime_params,
            globals_dict=self.read_env(),
            globals_pattern=os.getenv(KEDRO_GLOBALS_PATTERN, None),
            base_env=base_env,
            default_run_env=default_run_env,
        )

    def read_env(self) -> Dict:
        config = EnvTemplatedConfigLoader.ENV_DEFAULTS.copy()
        overrides = {
            k.replace(EnvTemplatedConfigLoader.VAR_PREFIX, "").lower(): v
            for k, v in os.environ.copy().items()
            if k.startswith(EnvTemplatedConfigLoader.VAR_PREFIX)
        }
        config.update(**overrides)
        return config


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

        return KedroSession.create(self._metadata.package_name, env=self._env)

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
            obj = self.context.config_loader.get(self.CONFIG_FILE_PATTERN)
        except:  # noqa
            obj = None

        if obj is None:
            try:
                obj = self._ensure_obj_is_dict(
                    self.context.config_loader[self.CONFIG_KEY]
                )
            except (KeyError, MissingConfigException):
                obj = None

        if obj is None:
            if not isinstance(cl, ConfigLoader):
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
                    "Missing vertexai.yml files in configuration. Make sure that you configure your project first"
                )
        return PluginConfig.parse_obj(obj)

    @cached_property
    def vertexai_client(self) -> VertexAIPipelinesClient:
        return VertexAIPipelinesClient(self.config, self.project_name, self.context)

    @staticmethod
    def init(metadata, env):
        return ContextHelper(metadata, env)
