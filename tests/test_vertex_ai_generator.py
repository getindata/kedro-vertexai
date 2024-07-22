"""Test generator"""

import unittest
from copy import deepcopy
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import kfp
import yaml
from kedro.pipeline import Pipeline, node

from kedro_vertexai.config import PluginConfig
from kedro_vertexai.constants import (
    KEDRO_CONFIG_RUN_ID,
    KEDRO_GLOBALS_PATTERN,
    KEDRO_VERTEXAI_RUNNER_CONFIG,
)
from kedro_vertexai.generator import PipelineGenerator
from kedro_vertexai.vertex_ai.runner import VertexAIPipelinesRunner
from tests.utils import environment


def identity(input1: str):
    return input1  # pragma: no cover


class TestGenerator(unittest.TestCase):
    def create_pipeline(self):
        return Pipeline(
            [
                node(identity, "A", "B", name="node1", tags=["foo", "group.nodegroup"]),
                node(identity, "B", "C", name="node2", tags=["bar", "group.nodegroup"]),
            ]
        )

    def test_should_group_when_enabled(self):
        # given
        expected1 = {"cpuLimit": 0.1, "cpuRequest": 0.1}
        expected2 = {
            "cpuLimit": 0.4,
            "cpuRequest": 0.4,
            "memoryLimit": 68.719476736,
            "memoryRequest": 68.719476736,
        }
        base = {
            "grouping": {"cls": "kedro_vertexai.grouping.TagNodeGrouper"},
            "resources": {"__default__": {"cpu": "100m"}},
        }
        tags = ["node1", "nodegroup", "foo", "bar", "group.nodegroup"]

        configs = []
        for tag in tags:
            testcfg = deepcopy(base)
            testcfg["resources"][tag] = {"cpu": "400m", "memory": "64Gi"}
            configs.append(testcfg)

        expected = [expected1] + 4 * [expected2]
        for cfg, exp in zip(configs, expected):
            with self.subTest(
                msg=str(next(key for key in cfg["resources"] if key != "__default__"))
            ):
                self.create_generator(config=cfg)
                # when
                with patch(
                    "kedro.framework.project.pipelines",
                    new=self.pipelines_under_test,
                ):
                    pipeline = self.generator_under_test.generate_pipeline(
                        "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
                    )
                    with NamedTemporaryFile(
                        mode="rt", prefix="pipeline", suffix=".yaml"
                    ) as spec_output:
                        kfp.compiler.Compiler().compile(pipeline, spec_output.name)
                        with open(spec_output.name) as f:
                            pipeline_spec = yaml.safe_load(f)

                        component_args = pipeline_spec["deploymentSpec"]["executors"][
                            "exec-component"
                        ]["container"]["args"][0]
                        assert (
                            '--nodes "node1,node2"' in component_args
                            or '"node2,node1"' in component_args
                        )

                        self.assertDictEqual(
                            pipeline_spec["deploymentSpec"]["executors"][
                                "exec-component"
                            ]["container"]["resources"],
                            exp,
                        )

    def test_should_not_add_resources_spec_if_not_requested(self):
        # given
        self.create_generator(
            config={
                "resources": {
                    "__default__": {"cpu": None, "memory": None},
                }
            }
        )

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with NamedTemporaryFile(
                mode="rt", prefix="pipeline", suffix=".yaml"
            ) as spec_output:
                kfp.compiler.Compiler().compile(pipeline, spec_output.name)
                with open(spec_output.name) as f:
                    pipeline_spec = yaml.safe_load(f)

            # then
            for component in ["exec-component", "exec-component-2"]:
                spec = pipeline_spec["deploymentSpec"]["executors"][component][
                    "container"
                ]
                assert "resources" not in spec

    def test_should_add_resources_spec(self):
        # given
        self.create_generator(
            config={
                "resources": {
                    "__default__": {"cpu": "100m"},
                    "node1": {"cpu": "400m", "gpu": "1", "memory": "64Gi"},
                },
                "node_selectors": {
                    "node1": {"cloud.google.com/gke-accelerator": "NVIDIA_TESLA_K80"},
                },
            }
        )

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with NamedTemporaryFile(
                mode="rt", prefix="pipeline", suffix=".yaml"
            ) as spec_output:
                kfp.compiler.Compiler().compile(pipeline, spec_output.name)
                with open(spec_output.name) as f:
                    pipeline_spec = yaml.safe_load(f)

                    # then
                    component1_resources = pipeline_spec["deploymentSpec"]["executors"][
                        "exec-component"
                    ]["container"]["resources"]
                    assert component1_resources["cpuLimit"] == 0.4
                    assert component1_resources["memoryLimit"] == 68.719476736
                    assert component1_resources["cpuRequest"] == 0.4
                    assert component1_resources["memoryRequest"] == 68.719476736
                    assert component1_resources["accelerator"]["count"] == "1"
                    assert (
                        component1_resources["accelerator"]["type"]
                        == "NVIDIA_TESLA_K80"
                    )

                    component2_resources = pipeline_spec["deploymentSpec"]["executors"][
                        "exec-component-2"
                    ]["container"]["resources"]
                    assert component2_resources["cpuLimit"] == 0.1
                    assert component2_resources["cpuRequest"] == 0.1

    def test_should_set_description(self):
        # given
        self.create_generator(config={"description": "DESC"})

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )

            # then
            assert pipeline.description == "DESC"

    @patch("kedro_vertexai.generator.is_mlflow_enabled", return_value=True)
    def test_should_add_env_and_pipeline_in_the_invocations(
        self, mock_is_mlflow_enabled
    ):
        # given
        self.create_generator()

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with NamedTemporaryFile(
                mode="rt", prefix="pipeline", suffix=".yaml"
            ) as spec_output:
                kfp.compiler.Compiler().compile(pipeline, spec_output.name)
                with open(spec_output.name) as f:
                    pipeline_spec = yaml.safe_load(f)

                assert (
                    "kedro vertexai -e unittests mlflow-start"
                    in pipeline_spec["deploymentSpec"]["executors"][
                        "exec-mlflow-start-run"
                    ]["container"]["args"][0]
                )

    @patch("kedro_vertexai.generator.is_mlflow_enabled", return_value=True)
    def test_should_add_runner_and_runner_config(self, mock_is_mlflow_enabled):
        # given
        self.create_generator()

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with NamedTemporaryFile(
                mode="rt", prefix="pipeline", suffix=".yaml"
            ) as spec_output:
                kfp.compiler.Compiler().compile(pipeline, spec_output.name)
                with open(spec_output.name) as f:
                    pipeline_spec = yaml.safe_load(f)

            # then
            assert all(
                check
                in pipeline_spec["deploymentSpec"]["executors"]["exec-component"][
                    "container"
                ]["args"][0]
                for check in (
                    f"{KEDRO_CONFIG_RUN_ID}=",
                    f"{KEDRO_VERTEXAI_RUNNER_CONFIG}='{{",
                    f"--runner {VertexAIPipelinesRunner.runner_name()}",
                )
            )

    def test_should_dump_params_and_add_config_if_params_are_set(self):
        self.create_generator(params={"my_params1": 1.0, "my_param2": ["a", "b", "c"]})
        self.mock_mlflow(False)

        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with kfp.dsl.Pipeline(None) as dsl_pipeline:
                pipeline()

            assert (
                "kedro vertexai -e unittests initialize-job --params="
                in dsl_pipeline.ops["node1"].container.args[0]
            )

            assert (
                'kedro run -e unittests --pipeline pipeline --nodes "node1"'
                in (args := dsl_pipeline.ops["node1"].container.args[0])
            ) and args.endswith("--config config.yaml")

    def test_should_add_globals_env_if_present(self):
        with environment({"KEDRO_GLOBALS_PATTERN": "*globals.yml"}):
            self.create_generator(
                params={"my_params1": 1.0, "my_param2": ["a", "b", "c"]}
            )
            self.mock_mlflow(False)
            with patch(
                "kedro.framework.project.pipelines",
                new=self.pipelines_under_test,
            ):

                pipeline = self.generator_under_test.generate_pipeline(
                    "pipeline",
                    "unittest-image",
                    "MLFLOW_TRACKING_TOKEN",
                )
                with kfp.dsl.Pipeline(None) as dsl_pipeline:
                    pipeline()

                expected = f'{KEDRO_GLOBALS_PATTERN}="*globals.yml"'
                assert expected in dsl_pipeline.ops["node1"].container.args[0]

                assert (
                    dsl_pipeline.ops["node1"].container.args[0].count(expected) == 2
                ), "Globals variable should be added twice - once for initialize-job, once for kedro run"

    def test_should_add_host_aliases_if_requested(self):
        # given
        self.create_generator(
            config={
                "network": {
                    "host_aliases": [
                        {
                            "ip": "10.10.10.10",
                            "hostnames": ["mlflow.internal", "mlflow.cloud"],
                        }
                    ]
                }
            }
        )
        self.mock_mlflow(True)

        # when
        with patch(
            "kedro.framework.project.pipelines",
            new=self.pipelines_under_test,
        ):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "unittest-image", "MLFLOW_TRACKING_TOKEN"
            )
            with kfp.dsl.Pipeline(None) as dsl_pipeline:
                pipeline()

            # then
            hosts_entry_cmd = (
                "echo 10.10.10.10\tmlflow.internal mlflow.cloud >> /etc/hosts;"
            )
            assert (
                hosts_entry_cmd
                in dsl_pipeline.ops["mlflow-start-run"].container.args[0]
            )
            assert hosts_entry_cmd in dsl_pipeline.ops["node1"].container.args[0]

    def mock_mlflow(self, enabled=False):
        def fakeimport(name, *args, **kw):
            if not enabled and (name == "mlflow" or name == "kedro_mlflow"):
                raise ImportError
            return self.realimport(name, *args, **kw)

        __builtins__["__import__"] = fakeimport

    def setUp(self):
        self.realimport = __builtins__["__import__"]
        self.mock_mlflow(False)

    def tearDown(self):
        __builtins__["__import__"] = self.realimport

    def create_generator(self, config={}, params={}, catalog={}):
        project_name = "my-awesome-project"
        config_loader = MagicMock()
        config_loader.get.return_value = catalog
        context = type(
            "obj",
            (object,),
            {
                "env": "unittests",
                "params": params,
                "config_loader": config_loader,
            },
        )

        self.pipelines_under_test = {"pipeline": self.create_pipeline()}

        config_with_defaults = {
            "image": "test",
            "root": "sample-bucket/sample-suffix",
            "experiment_name": "test-experiment",
            "run_name": "test-run",
        }
        config_with_defaults.update(config)
        self.generator_under_test = PipelineGenerator(
            PluginConfig.parse_obj(
                {
                    "project_id": "test-project",
                    "region": "test-region",
                    "run_config": config_with_defaults,
                }
            ),
            project_name,
            context,
            "run-name",
        )
