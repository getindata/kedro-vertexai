"""
Generator for Vertex AI pipelines
"""
import json
import logging
import os
from tempfile import NamedTemporaryFile
from typing import Dict

import kfp
from kedro.framework.context import KedroContext
from kfp.components.structures import (
    ComponentSpec,
    ContainerImplementation,
    ContainerSpec,
    OutputPathPlaceholder,
    OutputSpec,
)
from kfp.v2 import dsl

from kedro_vertexai.config import (
    KedroVertexAIRunnerConfig,
    RunConfig,
    dynamic_init_class,
)
from kedro_vertexai.constants import (
    KEDRO_CONFIG_JOB_NAME,
    KEDRO_CONFIG_RUN_ID,
    KEDRO_GLOBALS_PATTERN,
    KEDRO_VERTEXAI_RUNNER_CONFIG,
)
from kedro_vertexai.grouping import Grouping, NodeGrouper
from kedro_vertexai.utils import clean_name, is_mlflow_enabled
from kedro_vertexai.vertex_ai.io import generate_mlflow_inputs
from kedro_vertexai.vertex_ai.runner import VertexAIPipelinesRunner


class PipelineGenerator:
    """
    Generator creates Vertex AI pipeline function that operatoes with Vertex AI specific
    opertator spec.
    """

    log = logging.getLogger(__name__)

    def __init__(self, config, project_name, context, run_name: str):
        assert run_name, "run_name cannot be empty / None"
        self.run_name = run_name
        self.project_name = project_name
        self.context: KedroContext = context
        self.run_config: RunConfig = config.run_config
        self.catalog = context.config_loader.get("catalog*")
        self.grouping: NodeGrouper = dynamic_init_class(
            self.run_config.grouping.cls,
            context,
            **self.run_config.grouping.params,
        )

    def get_pipeline_name(self):
        """
        Returns Vertex-compatible pipeline name
        """
        return self.project_name.lower().replace(" ", "-").replace("_", "-")

    def generate_pipeline(self, pipeline, image, image_pull_policy, token):
        """
        This method return @dsl.pipeline annotated function that contains
        dynamically generated pipelines.
        :param pipeline: kedro pipeline
        :param image: full docker image name
        :param image_pull_policy: docker pull policy
        :param token: mlflow authentication token
        :return: kfp pipeline function
        """

        def set_dependencies(node_name, dependencies, kfp_ops):
            for dependency_group in dependencies:
                name = clean_name(node_name)
                dependency_name = clean_name(dependency_group)
                kfp_ops[name].after(kfp_ops[dependency_name])

        @dsl.pipeline(
            name=self.get_pipeline_name(),
            description=self.run_config.description,
        )
        def convert_kedro_pipeline_to_kfp() -> None:
            from kedro.framework.project import pipelines

            node_dependencies = pipelines[pipeline].node_dependencies
            grouping = self.grouping.group(node_dependencies)

            kfp_ops = self._build_kfp_ops(grouping, image, pipeline, token)
            for group_name, dependencies in grouping.dependencies.items():
                set_dependencies(group_name, dependencies, kfp_ops)

            for operator in kfp_ops.values():
                operator.container.set_image_pull_policy(image_pull_policy)

        return convert_kedro_pipeline_to_kfp

    def _generate_hosts_file(self):
        host_aliases = self.run_config.network.host_aliases
        return " ".join(
            f"echo {ha.ip}\t{' '.join(ha.hostnames)} >> /etc/hosts;"
            for ha in host_aliases
        )

    def _create_mlflow_op(self, image, should_add_params) -> dsl.ContainerOp:

        mlflow_command = " ".join(
            [
                self._generate_hosts_file(),
                "mkdir --parents",
                "`dirname {{$.outputs.parameters['output'].output_file}}`",
                "&&",
                self._generate_params_command(should_add_params),
                f"kedro vertexai -e {self.context.env} mlflow-start",
                "--output {{$.outputs.parameters['output'].output_file}}",
                self.run_name,
            ]
        ).strip()

        spec = ComponentSpec(
            name="mlflow-start-run",
            inputs=[],
            outputs=[OutputSpec("output", "String")],
            implementation=ContainerImplementation(
                container=ContainerSpec(
                    image=image,
                    command=["/bin/bash", "-c"],
                    args=[
                        mlflow_command,
                        OutputPathPlaceholder(output_name="output"),
                    ],
                )
            ),
        )
        with NamedTemporaryFile(
            mode="w", prefix="kedro-vertexai-spec", suffix=".yaml"
        ) as spec_file:
            spec.save(spec_file.name)
            component = kfp.components.load_component_from_file(spec_file.name)
        return component()

    def _build_kfp_ops(
        self,
        node_grouping: Grouping,
        image,
        pipeline,
        tracking_token=None,
    ) -> Dict[str, dsl.ContainerOp]:
        """Build kfp container graph from Kedro node dependencies."""
        kfp_ops = {}

        should_add_params = len(self.context.params) > 0

        mlflow_enabled = is_mlflow_enabled()
        if mlflow_enabled:
            kfp_ops["mlflow-start-run"] = self._create_mlflow_op(
                image, should_add_params
            )

        for group_name, nodes_group in node_grouping.nodes_mapping.items():
            name = clean_name(group_name)
            tags = {tag for tagging in nodes_group for tag in tagging.tags}

            mlflow_inputs, mlflow_envs = generate_mlflow_inputs()
            component_params = (
                [kfp_ops["mlflow-start-run"].output] if mlflow_enabled else []
            )

            runner_config = KedroVertexAIRunnerConfig(storage_root=self.run_config.root)

            kedro_command = " ".join(
                [
                    f"{KEDRO_CONFIG_RUN_ID}={dsl.PIPELINE_JOB_ID_PLACEHOLDER}",
                    f"{KEDRO_CONFIG_JOB_NAME}={dsl.PIPELINE_JOB_NAME_PLACEHOLDER}",
                    f"{KEDRO_VERTEXAI_RUNNER_CONFIG}='{runner_config.json()}'",
                    self._globals_env(),
                    f"kedro run -e {self.context.env}",
                    f"--pipeline {pipeline}",
                    f'--nodes "{",".join([n.name for n in nodes_group])}"',
                    f"--runner {VertexAIPipelinesRunner.runner_name()}",
                    "--config config.yaml" if should_add_params else "",
                ]
            )

            node_command = " ".join(
                [
                    h + " " if (h := self._generate_hosts_file()) else "",
                    self._generate_params_command(should_add_params),
                    mlflow_envs,
                    kedro_command,
                ]
            ).strip()

            spec = ComponentSpec(
                name=name,
                inputs=mlflow_inputs,
                outputs=[],
                implementation=ContainerImplementation(
                    container=ContainerSpec(
                        image=image,
                        command=["/bin/bash", "-c"],
                        args=[node_command],  # TODO: re-enable? + output_placeholders,
                    )
                ),
            )
            kfp_ops[name] = self._create_kedro_op(name, tags, spec, component_params)

        return kfp_ops

    def _globals_env(self) -> str:
        return (
            f'{KEDRO_GLOBALS_PATTERN}="{globals_env}"'
            if (globals_env := os.getenv(KEDRO_GLOBALS_PATTERN, None))
            else ""
        )

    def _generate_params_command(self, should_add_params) -> str:
        return (
            " ".join(
                [
                    self._globals_env(),
                    f"kedro vertexai -e {self.context.env} initialize-job --params='{json.dumps(self.context.params, indent=None)}' &&",  # noqa: E501
                ]
            ).strip()
            if should_add_params
            else ""
        )

    def _create_kedro_op(
        self, name: str, tags: set, spec: ComponentSpec, op_function_parameters
    ):
        with NamedTemporaryFile(
            mode="w", prefix="kedro-vertexai-node-spec", suffix=".yaml"
        ) as spec_file:
            spec.save(spec_file.name)
            component = kfp.components.load_component_from_file(spec_file.name)

        operator = component(*op_function_parameters)
        self._configure_resources(name, tags, operator)
        return operator

    def _configure_resources(self, name: str, tags: set, operator):
        resources = self.run_config.resources_for(name, tags)
        node_selectors = self.run_config.node_selectors_for(name, tags)
        if "cpu" in resources and resources["cpu"]:
            operator.set_cpu_limit(resources["cpu"])
            operator.set_cpu_request(resources["cpu"])
        if "gpu" in resources and resources["gpu"]:
            operator.set_gpu_limit(resources["gpu"])
        if "memory" in resources and resources["memory"]:
            operator.set_memory_limit(resources["memory"])
            operator.set_memory_request(resources["memory"])
        for constraint, value in node_selectors.items():
            operator.add_node_selector_constraint(constraint, value)
