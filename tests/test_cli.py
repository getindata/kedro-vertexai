import json
import os
import unittest
from collections import namedtuple
from itertools import product
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, Mock, patch

import yaml
from click.testing import CliRunner

from kedro_vertexai.cli import (
    compile,
    init,
    initialize_job,
    list_pipelines,
    mlflow_start,
    run_once,
    schedule,
    ui,
    vertexai_group,
)
from kedro_vertexai.constants import VERTEXAI_RUN_ID_TAG
from kedro_vertexai.context_helper import ContextHelper
from kedro_vertexai.utils import docker_build, docker_push

from .utils import test_config


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
            pipeline="new_pipe",
            parameters={"key1": "some value"},
        )

    def test_run_once_with_wait(self):
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
                "--wait-for-completion",
            ],
            obj=config,
        )

        assert result.exit_code == 0

    def test_docker_build(self):
        for exit_code in range(10):
            with self.subTest(exit_code=exit_code):
                with patch(
                    "subprocess.run", return_value=Mock(returncode=exit_code)
                ) as subprocess_run:
                    result = docker_build(".", "my_image:latest")
                    self.assertEqual(exit_code, result)
                    subprocess_run.assert_called_once()

    def test_docker_push(self):
        for exit_code in range(10):
            with self.subTest(exit_code=exit_code):
                with patch(
                    "subprocess.run", return_value=Mock(returncode=exit_code)
                ) as subprocess_run:
                    result = docker_push("my_image:latest")
                    self.assertEqual(exit_code, result)
                    subprocess_run.assert_called_once()

    def test_run_once_auto_build(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        for build_exit_code, push_exit_code in product(range(10), range(10)):
            with self.subTest(
                build_exit_code=build_exit_code, push_exit_code=push_exit_code
            ):
                with patch(
                    "kedro_vertexai.cli.docker_build", return_value=build_exit_code
                ), patch("kedro_vertexai.cli.docker_push", return_value=push_exit_code):
                    result = runner.invoke(
                        run_once,
                        [
                            "-i",
                            "new_img",
                            "-p",
                            "new_pipe",
                            "--param",
                            "key1:some value",
                            "--auto-build",
                            "--yes",
                        ],
                        obj=config,
                    )

                    expected_exit_code = (
                        build_exit_code if build_exit_code != 0 else push_exit_code
                    )
                    self.assertEqual(result.exit_code, expected_exit_code)

                    if expected_exit_code == 0:
                        context_helper.vertexai_client.run_once.assert_called_once()

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
            output="output",
            pipeline="pipe",
        )

    def test_store_params_empty(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        params = {"p1": 1, "p2": "value", "p3": [3.0, 4.0, 5.0]}

        with TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "config.yaml"

            result = runner.invoke(
                initialize_job,
                [
                    "--params",
                    f"'{json.dumps(params)}'",
                    "--output",
                    str(output_path.absolute()),
                ],
                obj=config,
            )

            assert result.exit_code == 0

            with output_path.open("r") as f:
                data = yaml.safe_load(f)
                assert (
                    "run" in data and "params" in data["run"]
                ), "Invalid keys in output file"
                self.assertDictEqual(
                    data["run"]["params"],
                    params,
                    "Saved parameters are different form input ones",
                )

    def test_store_params_exiting_config_yaml(self):
        """
        Covers the case when there is an exiting config.yaml in the pwd
        """
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        params = {"p1": 1, "p2": "value", "p3": [3.0, 4.0, 5.0]}
        exiting_config_yaml_content = {
            "run": {"data": "abc"},
            "other_keys": 66.6,
        }

        with TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir) / "config.yaml"
            with output_path.open("w") as f:
                yaml.safe_dump(exiting_config_yaml_content, f)

            result = runner.invoke(
                initialize_job,
                [
                    "--params",
                    f"'{json.dumps(params)}'",
                    "--output",
                    str(output_path.absolute()),
                ],
                obj=config,
            )

            assert result.exit_code == 0

            with output_path.open("r") as f:
                data = yaml.safe_load(f)
                assert (
                    "run" in data and "params" in data["run"]
                ), "Invalid keys in output file"
                self.assertDictEqual(
                    data["run"]["params"],
                    params,
                    "Saved parameters are different form input ones",
                )

                assert (
                    data["run"]["data"] == "abc" and data["other_keys"] == 66.6
                ), "Other keys were modified"

    def test_schedule(self):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config

        mock_schedule = MagicMock()
        context_helper.config.run_config.schedules = {
            "default_schedule": MagicMock(),
            "my-pipeline": mock_schedule,
        }
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        result = runner.invoke(
            schedule,
            [
                "--pipeline",
                "my-pipeline",
                "--cron-expression",
                "2 * * * *",
                "--timezone",
                "test-timezone",
                "--start-time",
                None,
                "--end-time",
                None,
                "--allow-queueing",
                True,
                "--max-run-count",
                10,
                "--max-concurrent-run-count",
                1,
                "--param",
                "key1:some value",
            ],
            obj=config,
            catch_exceptions=False,
        )

        assert result.exit_code == 0

        context_helper.vertexai_client.schedule.assert_called_with(
            pipeline="my-pipeline",
            schedule_config=mock_schedule,
            parameter_values={"key1": "some value"},
        )

        assert mock_schedule.cron_expression == "2 * * * *"
        assert mock_schedule.timezone == "test-timezone"
        assert mock_schedule.allow_queueing
        assert mock_schedule.max_run_count == 10

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
            result = runner.invoke(init, ["test-project-id", "region"], obj=config)

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

    @patch("mlflow.start_run")
    @patch("mlflow.set_tag")
    @patch("mlflow.get_experiment_by_name")
    def test_mlflow_start(
        self, get_experiment_by_name_mock, set_tag_mock, start_run_mock
    ):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.context.mlflow.tracking.experiment.name = "asd"
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

            assert result.exit_code == 0
            assert "Started run: MLFLOW_RUN_ID" in result.output
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

                runner.invoke(vertexai_group, cli + ["compile", "--help"], env=env)
                context_helper_init.assert_called_with(None, expected)
