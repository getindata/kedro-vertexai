import os
import shutil
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import yaml
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
            run_config=RunConfig(image="test-image", experiment_name="test-experiment"),
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
    @property
    def test_dir(self) -> str:
        return str(Path(os.path.dirname(os.path.abspath(__file__))) / "conf")

    def get_config(self, config_dir=None):
        config_path: str = self.test_dir if not config_dir else config_dir
        loader = EnvTemplatedConfigLoader(config_path, default_run_env="base")
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

    def test_loader_with_globals(self):
        test_config_file = Path(self.test_dir) / "base" / "test_config.yml"
        with environment({"KEDRO_GLOBALS_PATTERN": "*globals.yml"}):
            with TemporaryDirectory() as tmp_dir:
                tmp_config_dir = (Path(tmp_dir)) / "base"
                tmp_config_dir.mkdir()

                shutil.copy(test_config_file, tmp_config_dir / "test_config.yml")

                globals_path = tmp_config_dir / "globals.yml"
                with globals_path.open("w") as f:
                    yaml.safe_dump(
                        {"image_pull_policy": "GlobalsTestPullPolicy"},
                        f,
                    )

                config = self.get_config(tmp_dir)
                assert (
                    config["run_config"]["image_pull_policy"] == "GlobalsTestPullPolicy"
                ), "Variable defined in globals.yml was not used in the target config"
                assert config["run_config"]["experiment_name"] == "[Test] local"
