"""
Generator for Vertex AI pipelines
"""
import json
import logging
import os
from typing import Dict, List, Union  # noqa

from kedro.framework.context import KedroContext
from kfp import dsl
from kfp.dsl import PipelineTask
from makefun import with_signature

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
        self.catalog = context.config_loader.get("catalog")
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

    def _generate_params_signature(self, params: str) -> str:
        params = params.split(",") if len(params) > 0 else []

        signature_parts = []
        for param in params:
            param = param.strip()
            if ":" not in param:
                raise ValueError(
                    f"Invalid parameter format: '{param}'. "
                    f"Expected format: 'param_name:param_type' (e.g., 'test_param:str'). "
                    f"Use --params for parameter type definitions during compilation, "
                    f"not parameter values."
                )

            param_parts = param.split(":")
            if len(param_parts) != 2:
                raise ValueError(
                    f"Invalid parameter format: '{param}'. "
                    f"Expected exactly one colon separating name and type."
                )

            param_name = param_parts[0].strip()
            param_type = param_parts[1].strip()

            if not param_name:
                raise ValueError("Parameter name cannot be empty.")
            if not param_type:
                raise ValueError(
                    f"Parameter type cannot be empty for parameter '{param_name}'."
                )

            signature_parts.append(f"{param_name}: {param_type}")

        return ", ".join(signature_parts)

    def generate_pipeline(self, pipeline, image, token, params: str = ""):
        """
        This method return @dsl.pipeline annotated function that contains
        dynamically generated pipelines.
        :param pipeline: kedro pipeline
        :param image: full docker image name
        :param token: mlflow authentication token
        :param params: Pipeline parameters to be specified at run time.
        :return: kfp pipeline function
        """
        params_signature = self._generate_params_signature(params)

        def set_dependencies(
            node_name, dependencies, kfp_tasks: Dict[str, PipelineTask]
        ):
            for dependency_group in dependencies:
                name = clean_name(node_name)
                dependency_name = clean_name(dependency_group)
                kfp_tasks[name].after(kfp_tasks[dependency_name])

        @dsl.pipeline(
            name=self.get_pipeline_name(),
            description=self.run_config.description,
        )
        @with_signature(f"pipeline({params_signature}) -> None")
        def convert_kedro_pipeline_to_kfp(*args, **kwargs) -> None:
            from kedro.framework.project import pipelines

            node_dependencies = pipelines[pipeline].node_dependencies
            grouping = self.grouping.group(node_dependencies)

            kfp_tasks = self._build_kfp_tasks(
                grouping, image, pipeline, token, kwargs, params_signature
            )
            for group_name, dependencies in grouping.dependencies.items():
                set_dependencies(group_name, dependencies, kfp_tasks)

        return convert_kedro_pipeline_to_kfp

    def _generate_hosts_file(self):
        host_aliases = self.run_config.network.host_aliases
        return " ".join(
            f"echo {ha.ip}\t{' '.join(ha.hostnames)} >> /etc/hosts;"
            for ha in host_aliases
        )

    def _create_mlflow_task(self, image, should_add_params) -> PipelineTask:
        @dsl.container_component
        def mlflow_start_run(mlflow_run_id: dsl.OutputPath(str)):

            mlflow_command = " ".join(
                [
                    self._generate_hosts_file(),
                    f"mkdir -p $(dirname {mlflow_run_id})",
                    "&&",
                    self._generate_params_command(should_add_params),
                    f"kedro vertexai -e {self.context.env} mlflow-start",
                    f"--output {mlflow_run_id}",
                    self.run_name,
                ]
            ).strip()

            return dsl.ContainerSpec(
                image=image,
                command=["/bin/bash", "-c"],
                args=[mlflow_command],
            )

        return mlflow_start_run()

    def _add_mlflow_param_to_signature(self, params_signature: str) -> str:
        mlflow_signature = "mlflow_run_id: Union[str, None] = None"

        params_signature = (
            f"{params_signature}, {mlflow_signature}"
            if len(params_signature) > 0
            else mlflow_signature
        )
        return params_signature

    def _build_kfp_tasks(
        self,
        node_grouping: Grouping,
        image,
        pipeline,
        tracking_token=None,
        params: List[str] = [],
        params_signature: str = "",
    ) -> Dict[str, PipelineTask]:
        """Build kfp container graph from Kedro node dependencies."""
        kfp_tasks = {}

        should_add_params = len(self.context.params) > 0

        mlflow_enabled = is_mlflow_enabled()
        if mlflow_enabled:
            kfp_tasks["mlflow-start-run"] = self._create_mlflow_task(
                image, should_add_params
            )

            params_signature = self._add_mlflow_param_to_signature(params_signature)

        for group_name, nodes_group in node_grouping.nodes_mapping.items():
            name = clean_name(group_name)
            tags = {tag for tagging in nodes_group for tag in tagging.tags}

            mlflow_params = (
                kfp_tasks["mlflow-start-run"].outputs if mlflow_enabled else {}
            )
            component_params = {**mlflow_params, **params}

            runner_config = KedroVertexAIRunnerConfig(storage_root=self.run_config.root)

            kedro_command = " ".join(
                [
                    f"{KEDRO_CONFIG_RUN_ID}={dsl.PIPELINE_JOB_ID_PLACEHOLDER}",
                    f"{KEDRO_CONFIG_JOB_NAME}={dsl.PIPELINE_JOB_NAME_PLACEHOLDER}",
                    f"{KEDRO_VERTEXAI_RUNNER_CONFIG}='{runner_config.model_dump_json()}'",
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
                    "MLFLOW_RUN_ID=\"{{$.inputs.parameters['mlflow_run_id']}}\" "
                    if is_mlflow_enabled()
                    else "",
                    self._generate_gcp_env_vars_command(),
                    kedro_command,
                ]
            ).strip()

            @dsl.container_component
            @with_signature(f"{name.replace('-', '_')}({params_signature})")
            def component(*args, **kwargs):
                # Build dynamic parameters from kwargs, filtering out mlflow parameters
                dynamic_parameters = ",".join(
                    [
                        f"{k}={kwargs[k]}"
                        for k in kwargs.keys()
                        if k != "mlflow_run_id"
                    ]  # Exclude mlflow parameter
                )

                # Only add --params if there are dynamic parameters
                # Fix: Concatenate parameters into single command instead of separate args
                full_command = node_command
                if dynamic_parameters:
                    full_command += f" --params {dynamic_parameters}"

                return dsl.ContainerSpec(
                    image=image,
                    command=["/bin/bash", "-c"],
                    args=[full_command],
                )

            task = component(**component_params)
            self._configure_resources(name, tags, task)
            kfp_tasks[name] = task

        return kfp_tasks

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

    def _generate_gcp_env_vars_command(self) -> str:
        vertex_conf = self.context.config_loader.get("vertexai")
        project_id = vertex_conf.get("project_id")
        region = vertex_conf.get("region")
        return f"GCP_PROJECT_ID={project_id} GCP_REGION={region}"

    def _configure_resources(self, name: str, tags: set, task: PipelineTask):
        resources = self.run_config.resources_for(name, tags)
        node_selectors = self.run_config.node_selectors_for(name, tags)
        if "cpu" in resources and resources["cpu"]:
            task.set_cpu_limit(resources["cpu"])
            task.set_cpu_request(resources["cpu"])
        if "gpu" in resources and resources["gpu"]:
            task.set_gpu_limit(resources["gpu"])
        if "memory" in resources and resources["memory"]:
            task.set_memory_limit(resources["memory"])
            task.set_memory_request(resources["memory"])
        for constraint, value in node_selectors.items():
            task.set_accelerator_type(constraint)
            task.add_node_selector_constraint(value)
