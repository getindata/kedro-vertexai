import os
import unittest
from unittest.mock import MagicMock, Mock, patch

from kedro.framework.session import KedroSession

from kedro_vertexai.config import PluginConfig, RunConfig
from kedro_vertexai.context_helper import (
    ContextHelper,
    EnvTemplatedConfigLoader,
)

from .utils import environment


class TestContextHelper(unittest.TestCase):
    def test_project_name(self):
        metadata = Mock()
        metadata.project_name = "test_project"

        helper = ContextHelper.init(metadata, "test")
        assert helper.project_name == "test_project"

    def test_context(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        kedro_session = MagicMock(KedroSession)
        kedro_session.load_context.return_value = "sample_context"

        with patch.object(KedroSession, "create") as create:
            create().load_context.return_value = "sample_context"
            helper = ContextHelper.init(metadata, "test")
            assert helper.context == "sample_context"
            create.assert_called_with("test_package", env="test")

    def test_config(self):
        cfg = PluginConfig(
            project_id="test-project",
            region="test-region",
            run_config=RunConfig(
                image="test-image", experiment_name="test-experiment"
            ),
        )
        metadata = Mock()
        metadata.package_name = "test_package"
        context = MagicMock()
        context.config_loader.return_value.get.return_value = ["one", "two"]
        with patch.object(KedroSession, "create", context), patch(
            "kedro_vertexai.context_helper.EnvTemplatedConfigLoader"
        ) as config_loader:
            config_loader.return_value.get.return_value = cfg.dict()
            helper = ContextHelper.init(metadata, "test")
            assert helper.config == cfg


class TestEnvTemplatedConfigLoader(unittest.TestCase):
    @staticmethod
    def get_config():
        config_path = [os.path.dirname(os.path.abspath(__file__))]
        loader = EnvTemplatedConfigLoader(config_path)
        return loader.get("test_config.yml")

    def test_loader_with_defaults(self):
        config = self.get_config()
        assert config["run_config"]["image"] == "gcr.io/project-image/dirty"
        assert config["run_config"]["experiment_name"] == "[Test] local"

    def test_loader_with_env(self):
        with environment(
            {
                "KEDRO_CONFIG_COMMIT_ID": "123abc",
                "KEDRO_CONFIG_BRANCH_NAME": "feature-1",
                "KEDRO_CONFIG_XYZ123": "123abc",
            }
        ):
            config = self.get_config()

        assert config["run_config"]["image"] == "gcr.io/project-image/123abc"
        assert config["run_config"]["experiment_name"] == "[Test] feature-1"
