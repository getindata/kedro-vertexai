import os
import unittest
from collections import namedtuple
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import yaml
from click.testing import CliRunner

from kedro_vertexai.cli import (
    compile,
    init,
    list_pipelines,
    mlflow_start,
    run_once,
    schedule,
    ui,
    vertexai_group,
)
from kedro_vertexai.config import PluginConfig
from kedro_vertexai.constants import VERTEXAI_RUN_ID_TAG
from kedro_vertexai.context_helper import ContextHelper

test_config = PluginConfig.parse_obj(
    {
        "project_id": "test-project-id",
        "region": "test",
        "run_config": {
            "image": "gcr.io/project-image/test",
            "image_pull_policy": "Always",
            "experiment_name": "Test Experiment",
            "run_name": "test run",
            "volume": {
                "storageclass": "default",
                "size": "3Gi",
                "access_modes": "[ReadWriteOnce]",
            },
        },
    }
)


class TestPluginCLI(unittest.TestCase):
    def test_list_pipelines(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(list_pipelines, [], obj=config)

        assert result.exit_code == 0
        context_helper.vertexai_client.list_pipelines.assert_called_with()

    def test_run_once(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(
            run_once,
            [
                "-i",
                "new_img",
                "-p",
                "new_pipe",
                "--param",
                "key1:some value",
            ],
            obj=config,
        )

        assert result.exit_code == 0
        context_helper.vertexai_client.run_once.assert_called_with(
            image="new_img",
            image_pull_policy="Always",
            pipeline="new_pipe",
            parameters={"key1": "some value"},
        )

    @patch("webbrowser.open_new_tab")
    def test_ui(self, open_new_tab):
        context_helper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(ui, [], obj=config)

        assert result.exit_code == 0
        open_new_tab.assert_called_with(
            f"https://console.cloud.google.com/vertex-ai/"
            f"pipelines?project={context_helper.config.project_id}"
        )

    def test_compile(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(
            compile, ["-p", "pipe", "-i", "img", "-o", "output"], obj=config
        )

        assert result.exit_code == 0
        context_helper.vertexai_client.compile.assert_called_with(
            image="img",
            image_pull_policy="Always",
            output="output",
            pipeline="pipe",
        )

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_schedule(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(
            schedule,
            [
                "-c",
                "* * *",
                "-p",
                "my-pipeline",
                "--param",
                "key1:some value",
            ],
            obj=config,
        )

        assert result.exit_code == 0
        context_helper.vertexai_client.schedule.assert_called_with(
            "my-pipeline",
            "test_experiment",
            None,
            "* * *",
            run_name="test run",
            parameters={"key1": "some value"},
        )

    @patch.object(Path, "cwd")
    def test_init(self, cwd):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        context_helper.context.project_name = "Test Project"
        context_helper.context.project_path.name = "test_project_path"
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            cwd.return_value = path
            os.makedirs(path.joinpath("conf/base"))
            result = runner.invoke(
                init, ["test-project-id", "region"], obj=config
            )

            assert result.exit_code == 0, result.output
            assert result.output.startswith("Configuration generated in ")
            with open(path.joinpath("conf/base/vertexai.yml"), "r") as f:
                cfg = yaml.safe_load(f)
                assert isinstance(cfg, dict), "Could not parse config as yaml"

    @patch.object(Path, "cwd")
    def test_init_with_github_actions(self, cwd):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        context_helper.context.project_name = "Test Project"
        context_helper.context.project_path.name = "test_project_path"
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            cwd.return_value = path
            os.makedirs(path.joinpath("conf/base"))
            result = runner.invoke(
                init,
                ["test-project-id", "region", "--with-github-actions"],
                obj=config,
            )

            assert result.exit_code == 0
            on_push_actions = path / ".github" / "workflows" / "on-push.yml"
            assert on_push_actions.exists()
            with open(on_push_actions, "r") as f:
                assert "kedro vertexai run-once" in f.read()

    @patch("kedro_mlflow.framework.context.get_mlflow_config")
    @patch("mlflow.start_run")
    @patch("mlflow.set_tag")
    def test_mlflow_start(
        self, set_tag_mock, start_run_mock, get_mlflow_config_mock
    ):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        config = dict(context_helper=context_helper)
        runner = CliRunner()
        start_run_mock.return_value = namedtuple("InfoObject", "info")(
            namedtuple("RunIdObject", "run_id")("MLFLOW_RUN_ID")
        )

        with TemporaryDirectory() as temp_dir:
            run_id_file_path = f"{temp_dir}/run_id"
            result = runner.invoke(
                mlflow_start,
                ["test-run-id", "--output", run_id_file_path],
                obj=config,
            )

            assert "Started run: MLFLOW_RUN_ID" in result.output
            assert result.exit_code == 0
            with open(run_id_file_path) as f:
                assert f.read() == "MLFLOW_RUN_ID"

        set_tag_mock.assert_called_with(VERTEXAI_RUN_ID_TAG, "test-run-id")

    @patch.object(ContextHelper, "init")
    def test_handle_env_arguments(self, context_helper_init):
        for testname, env_var, cli, expected in [
            (
                "CLI arg should have preference over environment variable",
                "pipelines",
                "custom",
                "custom",
            ),
            (
                "KEDRO_ENV should be taken into account",
                "pipelines",
                None,
                "pipelines",
            ),
            ("CLI arg should be taken into account", None, "custom", "custom"),
            ("default value should be set", None, None, "local"),
        ]:
            runner = CliRunner()
            with self.subTest(msg=testname):
                cli = ["--env", cli] if cli else []
                env = dict(KEDRO_ENV=env_var) if env_var else dict()

                runner.invoke(
                    vertexai_group, cli + ["compile", "--help"], env=env
                )
                context_helper_init.assert_called_with(None, expected)
