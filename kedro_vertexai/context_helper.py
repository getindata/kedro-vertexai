import os
from functools import lru_cache
from typing import Dict, Iterable

from kedro.config import TemplatedConfigLoader

from kedro_vertexai.client import VertexAIPipelinesClient

from .config import PluginConfig


class EnvTemplatedConfigLoader(TemplatedConfigLoader):
    """Config loader that can substitute $(commit_id) and $(branch_name)
    placeholders with information taken from env variables."""

    VAR_PREFIX = "KEDRO_CONFIG_"
    # defaults provided so default variables ${commit_id|dirty} work for some entries
    ENV_DEFAULTS = {"commit_id": None, "branch_name": None, "run_id": ""}

    def __init__(self, conf_paths: Iterable[str]):
        super().__init__(conf_paths, globals_dict=self.read_env())

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

    @property
    @lru_cache()
    def session(self):
        from kedro.framework.session import KedroSession

        return KedroSession.create(self._metadata.package_name, env=self._env)

    @property
    def context(self):
        return self.session.load_context()

    @property
    @lru_cache()
    def config(self) -> PluginConfig:
        raw = EnvTemplatedConfigLoader(
            self.context.config_loader.conf_paths
        ).get(self.CONFIG_FILE_PATTERN)
        return PluginConfig.parse_obj(raw)

    @property
    @lru_cache()
    def vertexai_client(self) -> VertexAIPipelinesClient:
        return VertexAIPipelinesClient(
            self.config, self.project_name, self.context
        )

    @staticmethod
    def init(metadata, env):
        return ContextHelper(metadata, env)
