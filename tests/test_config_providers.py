import logging
import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

from kedro_vertexai.auth.gcp import (
    MLFlowGoogleIAMCredentialsProvider,
    MLFlowGoogleOAuthCredentialsProvider,
)
from kedro_vertexai.config import PluginConfig
from kedro_vertexai.context_helper import ContextHelper
from kedro_vertexai.dynamic_config import DynamicConfigProvider
from kedro_vertexai.utils import (
    _generate_and_save_dynamic_config,
    _load_yaml_or_empty_dict,
    materialize_dynamic_configuration,
    store_parameters_in_yaml,
)

from .utils import test_config


def _disable_logging(func):
    def wrapped(*args, **kwargs):
        logging.disable(logging.CRITICAL)
        loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
        levels = [logger.level for logger in loggers]
        for logger in loggers:
            logger.setLevel(logging.CRITICAL)
        func(*args, **kwargs)
        for logger, state in zip(loggers, levels):
            logger.setLevel(state)
        logging.disable(logging.NOTSET)

    return wrapped


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
    def _get_test_config_with_dynamic_provider(
        self,
        class_name=MLFlowGoogleOAuthCredentialsProvider.full_name(),
    ) -> PluginConfig:
        config_raw = deepcopy(test_config.dict())
        config_raw["run_config"]["dynamic_config_providers"] = [
            {
                "cls": class_name,
                "params": {
                    "client_id": "unit-tests-client-id",
                    "service_account": "asd@example.com",
                },
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

            params = dict(key="unit_tests_param_key", value="unit_tests_param_value")

            provider = UnitTestsDynamicConfigProvider(test_config, **params)
            _generate_and_save_dynamic_config(provider, context_helper)
            self.assertDictEqual(
                _load_yaml_or_empty_dict(
                    Path(tmp_dir) / "conf" / "base" / "unittests.yaml"
                ),
                {"unit_tests_param_key": "unit_tests_param_value"},
            )

    @patch("kedro_vertexai.config.logger.error")
    @_disable_logging
    def test_create_provider_from_invalid_config(self, log_error):
        config = self._get_test_config_with_dynamic_provider(
            class_name="totally.not.existing.class"
        )
        provider = DynamicConfigProvider.build(
            config, config.run_config.dynamic_config_providers[0]
        )
        assert provider is None
        log_error.assert_called_once()

    def test_can_create_provider_from_config(self):
        for cls_name in (
            MLFlowGoogleOAuthCredentialsProvider.full_name(),
            MLFlowGoogleIAMCredentialsProvider.full_name(),
        ):
            with self.subTest(msg=cls_name):
                config = self._get_test_config_with_dynamic_provider(cls_name)

                provider = DynamicConfigProvider.build(
                    config, config.run_config.dynamic_config_providers[0]
                )

                assert provider is not None and isinstance(
                    provider, DynamicConfigProvider
                )

                assert isinstance(
                    provider.target_config_file, str
                ) and provider.target_config_file.endswith(".yml")

    def test_config_materialization(self):
        token = uuid4().hex
        for (call_to_patch, cls) in (
            (
                "kedro_vertexai.auth.gcp.AuthHandler.obtain_id_token",
                MLFlowGoogleOAuthCredentialsProvider.full_name(),
            ),
            (
                "kedro_vertexai.auth.gcp.AuthHandler.obtain_iam_token",
                MLFlowGoogleIAMCredentialsProvider.full_name(),
            ),
        ):
            with self.subTest(msg=call_to_patch):
                with patch(call_to_patch, return_value=token) as patched:
                    with TemporaryDirectory() as tmp_dir:
                        context_helper: ContextHelper = MagicMock(ContextHelper)
                        type(context_helper.context).project_path = PropertyMock(
                            return_value=Path(tmp_dir)
                        )
                        output_dir = Path(tmp_dir) / "conf" / "base"
                        output_dir.mkdir(parents=True)
                        materialize_dynamic_configuration(
                            self._get_test_config_with_dynamic_provider(cls),
                            context_helper,
                        )

                        self.assertDictEqual(
                            _load_yaml_or_empty_dict(output_dir / "credentials.yml"),
                            {"gcp_credentials": {"MLFLOW_TRACKING_TOKEN": token}},
                        )

                        patched.assert_called_once()

    @_disable_logging
    @patch("kedro_vertexai.utils._generate_and_save_dynamic_config")
    @patch("kedro_vertexai.utils.logger.warning")
    def test_config_materialization_skips_invalid_configs(
        self, log_warning, generate_and_save_fn
    ):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        materialize_dynamic_configuration(
            self._get_test_config_with_dynamic_provider("totally.not.existing.class"),
            context_helper,
        )

        log_warning.assert_called_once()
        generate_and_save_fn.assert_not_called()

    @patch("kedro_vertexai.utils.logger.debug")
    def test_empty_params_saving_is_skipped(self, log_debug):
        with TemporaryDirectory() as tmp_dir:
            store_parameters_in_yaml("", tmp_dir)
            log_debug.assert_called_once()
            assert len(list(Path(tmp_dir).glob("*"))) == 0, "No files should be saved"
