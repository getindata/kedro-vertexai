import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, PropertyMock, patch

from kedro_vertexai.config import PluginConfig
from kedro_vertexai.context_helper import ContextHelper
from kedro_vertexai.dynamic_config import DynamicConfigProvider
from kedro_vertexai.utils import (
    _generate_and_save_dynamic_config,
    _load_yaml_or_empty_dict,
    materialize_dynamic_configuration,
)

from .utils import test_config


class UnitTestsDynamicConfigProvider(DynamicConfigProvider):
    def __init__(self, config: PluginConfig, key, value, **kwargs):
        super().__init__(config, **kwargs)
        self.key = key
        self.value = value

    @property
    def target_config_file(self) -> str:
        return "unittests.yaml"

    def generate_config(self) -> dict:
        return {self.key: self.value}


class TestDynamicConfigProviders(unittest.TestCase):
    def _get_test_config_with_dynamic_provider(self) -> PluginConfig:
        config_raw = deepcopy(test_config.dict())
        config_raw["run_config"]["dynamic_config_providers"] = [
            {
                "cls": "kedro_vertexai.auth.gcp.MLFlowGoogleOAuthCredentialsProvider",
                "params": {"client_id": "unit-tests-client-id"},
            }
        ]
        config = PluginConfig.parse_obj(config_raw)
        return config

    def test_initialization_from_config(self):
        with TemporaryDirectory() as tmp_dir:
            context_helper: ContextHelper = MagicMock(ContextHelper)
            type(context_helper.context).project_path = PropertyMock(
                return_value=Path(tmp_dir)
            )
            (Path(tmp_dir) / "conf" / "base").mkdir(parents=True)

            params = dict(
                key="unit_tests_param_key", value="unit_tests_param_value"
            )

            provider = UnitTestsDynamicConfigProvider(test_config, **params)
            _generate_and_save_dynamic_config(provider, context_helper)
            self.assertDictEqual(
                _load_yaml_or_empty_dict(
                    Path(tmp_dir) / "conf" / "base" / "unittests.yaml"
                ),
                {"unit_tests_param_key": "unit_tests_param_value"},
            )

    def test_can_create_provider_from_config(self):
        config = self._get_test_config_with_dynamic_provider()

        provider = DynamicConfigProvider.build(
            config, config.run_config.dynamic_config_providers[0]
        )

        assert provider is not None and isinstance(
            provider, DynamicConfigProvider
        )

    def test_config_materialization(self):
        expected = {"unit_tests": "value"}
        with patch(
            "kedro_vertexai.auth.gcp.MLFlowGoogleOAuthCredentialsProvider.generate_config",
            return_value=expected,
        ) as patched:
            with TemporaryDirectory() as tmp_dir:
                context_helper: ContextHelper = MagicMock(ContextHelper)
                type(context_helper.context).project_path = PropertyMock(
                    return_value=Path(tmp_dir)
                )
                output_dir = Path(tmp_dir) / "conf" / "base"
                output_dir.mkdir(parents=True)
                materialize_dynamic_configuration(
                    self._get_test_config_with_dynamic_provider(),
                    context_helper,
                )

                self.assertDictEqual(
                    _load_yaml_or_empty_dict(output_dir / "credentials.yml"),
                    expected,
                )

                patched.assert_called_once()
