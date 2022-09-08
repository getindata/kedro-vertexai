import logging
from abc import ABC, abstractmethod
from importlib import import_module

from kedro_vertexai.config import DynamicConfigProviderConfig, PluginConfig

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
        module_name, class_name = provider_config.cls.rsplit(".", 1)
        logger.info(f"Initializing {class_name}")

        try:
            cls = getattr(import_module(module_name), class_name)
            return cls(config, **provider_config.params)
        except:  # noqa: E722
            logger.error(
                f"Could not load dynamic config loader class {provider_config.cls}, "
                f"make sure it's accessible from the current Python interpreter",
                exc_info=True,
            )

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
