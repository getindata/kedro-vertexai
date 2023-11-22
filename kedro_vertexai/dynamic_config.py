import logging
from abc import ABC, abstractmethod

from kedro_vertexai.config import (
    DynamicConfigProviderConfig,
    PluginConfig,
    dynamic_init_class,
)

logger = logging.getLogger(__name__)


class DynamicConfigProvider(ABC):
    @classmethod
    def full_name(cls):
        return f"{cls.__module__}.{cls.__qualname__}"

    @staticmethod
    def build(
        config: PluginConfig,
        provider_config: DynamicConfigProviderConfig,
    ) -> "DynamicConfigProvider":
        return dynamic_init_class(provider_config.cls, config, **provider_config.params)

    def __init__(self, config: PluginConfig, **kwargs):
        self.config = config

    @property
    def target_env(self) -> str:
        return "base"

    @property
    @abstractmethod
    def target_config_file(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def generate_config(self) -> dict:
        raise NotImplementedError()

    def merge_with_existing(  # noqa
        self, existing_config: dict, generated_config: dict
    ) -> dict:
        new_config = existing_config
        new_config.update(generated_config)
        return new_config
