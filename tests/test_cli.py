import json
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
            image_pull_policy="Always",
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
                "--timeout-seconds",
                "666",
            ],
            obj=config,
        )

        assert result.exit_code == 0
        context_helper.vertexai_client.wait_for_completion.assert_called_with(666)

    @patch("subprocess.run")
    def test_run_once_auto_build(self, subp_run):
        context_helper: ContextHelper = MagicMock(ContextHelper)
        context_helper.config = test_config
        config = dict(context_helper=context_helper)
        runner = CliRunner()

        # Simulating no error run for Popen
        foo = subp_run()
        foo.returncode = 0

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
                "--yes-confirm",
            ],
            obj=config,
        )

        self.assertEqual(subp_run.call_count, 3)
        for p in ("docker", "build", "-t", "new_img"):
            self.assertIn(p, subp_run.call_args_list[1][0][0])
        for p in ("docker", "push", "new_img"):
            self.assertIn(p, subp_run.call_args_list[2][0][0])
        self.assertEqual(result.exit_code, 0)
        # Proper call example args
        # [call(),
        # call(['docker','build',"<MagicMock name='mock.context.project_path' id='...'>",'-t','new_img']),
        # call(['docker','push','new_img'])]

        # Testing fail during build
        foo.returncode = 1
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
                "--yes-confirm",
            ],
            obj=config,
        )

        self.assertNotEqual(result.exit_code, 0)

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
