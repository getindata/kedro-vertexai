import os
from functools import cached_property
from typing import Any, Dict

from kedro.config import TemplatedConfigLoader

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
        default_run_env: str = "local"
    ):
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

    @cached_property
    def config(self) -> PluginConfig:
        raw = EnvTemplatedConfigLoader(self.context.config_loader.conf_source).get(
            self.CONFIG_FILE_PATTERN
        )
        return PluginConfig.parse_obj(raw)

    @cached_property
    def vertexai_client(self) -> VertexAIPipelinesClient:
        return VertexAIPipelinesClient(self.config, self.project_name, self.context)

    @staticmethod
    def init(metadata, env):
        return ContextHelper(metadata, env)
