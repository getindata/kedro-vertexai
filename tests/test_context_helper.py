import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import yaml
from kedro.framework.session import KedroSession

from kedro_vertexai.config import PluginConfig, RunConfig
from kedro_vertexai.context_helper import ContextHelper


class TestContextHelper(unittest.TestCase):
    def test_project_name(self):
        metadata = Mock()
        metadata.project_name = "test_project"

        helper = ContextHelper.init(metadata, "test")
        assert helper.project_name == "test_project"

    def test_context(self):
        metadata = Mock()
        metadata.project_path = "/test/path"
        kedro_session = MagicMock(KedroSession)
        kedro_session.load_context.return_value = "sample_context"

        with patch.object(KedroSession, "create") as create:
            create().load_context.return_value = "sample_context"
            helper = ContextHelper.init(metadata, "test")
            assert helper.context == "sample_context"
            create.assert_called_with(metadata.project_path, env="test")

    def test_config(self):
        cfg = PluginConfig(
            project_id="test-project",
            region="test-region",
            run_config=RunConfig(image="test-image", experiment_name="test-experiment"),
        )
        metadata = Mock()
        metadata.package_name = "test_package"
        session = MagicMock()
        session.load_context().config_loader.get.return_value = cfg.dict()
        with patch.object(KedroSession, "create", return_value=session):
            helper = ContextHelper.init(metadata, "test")
            assert helper.config == cfg

    def test_config_with_omegaconf(self):
        from kedro.config import OmegaConfigLoader

        with TemporaryDirectory() as tmp_dir_raw:
            tmp_dir = Path(tmp_dir_raw)
            (tmp_dir / "conf" / "base").mkdir(parents=True, exist_ok=False)
            (conf_dir := tmp_dir / "conf" / "local").mkdir(parents=True, exist_ok=False)
            cfg = PluginConfig(
                project_id="test-project",
                region="test-region",
                run_config=RunConfig(
                    image="test-image", experiment_name="test-experiment"
                ),
            )
            (conf_dir / "vertexai.yml").write_text(yaml.dump(cfg.dict()))

            metadata = Mock()
            metadata.package_name = "test_package"
            for config_pattern in [{}, {"vertexai": ["vertexai*"]}]:
                session = MagicMock()
                session.load_context().config_loader = OmegaConfigLoader(
                    str(tmp_dir / "conf"),
                    config_patterns=config_pattern,
                    default_run_env="local",
                )
                with patch.object(KedroSession, "create", return_value=session):
                    helper = ContextHelper.init(metadata, "test")
                    assert helper.config == cfg

    @unittest.expectedFailure
    def test_config_empty(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        session = MagicMock()
        session.load_context().config_loader.get.return_value = None
        with patch.object(KedroSession, "create", return_value=session):
            helper = ContextHelper.init(metadata, "test")
            _ = helper.config

    @unittest.expectedFailure
    def test_config_raises(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        session = MagicMock()
        session.load_context().config_loader.get.side_effect = ValueError()
        with patch.object(KedroSession, "create", return_value=session):
            helper = ContextHelper.init(metadata, "test")
            _ = helper.config

    @unittest.expectedFailure
    def test_config_invalid(self):
        metadata = Mock()
        metadata.package_name = "test_package"
        session = MagicMock()
        session.load_context().config_loader.get.return_value = None
        session.load_context().config_loader.__getitem__.side_effect = KeyError()
        with patch.object(KedroSession, "create", return_value=session):
            helper = ContextHelper.init(metadata, "test")
            _ = helper.config
