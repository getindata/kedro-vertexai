"""
Vertex AI Pipelines specific client, based on AIPlatformClient.
"""

import datetime as dt
import logging
import os
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

from google.cloud import aiplatform as aip
from google.cloud.aiplatform import PipelineJob
from kfp.compiler import Compiler
from tabulate import tabulate

from .config import PluginConfig, ScheduleConfig
from .generator import PipelineGenerator


class VertexAIPipelinesClient:
    """
    Client for Vertex AI Pipelines.
    """

    log = logging.getLogger(__name__)

    def __init__(self, config: PluginConfig, project_name, context):

        aip.init(
            project=config.project_id,
            location=config.region,
            experiment=config.run_config.experiment_name,
            experiment_description=config.run_config.experiment_description,
        )
        self.run_config = config.run_config
        self.run_name = self._generate_run_name(config)
        self.generator = PipelineGenerator(config, project_name, context, self.run_name)

    def list_pipelines(self):
        """
        List all the jobs (current and historical) on Vertex AI Pipelines
        :return:
        """
        headers = ["Name", "ID"]

        list_jobs_response = aip.PipelineJob.list()
        data = [(x.display_name, x.name) for x in list_jobs_response]

        return tabulate(data, headers=headers)

    def run_once(
        self,
        pipeline,
        image,
        parameters=None,
    ) -> PipelineJob:
        """
        Runs the pipeline in Vertex AI Pipelines
        :param pipeline:
        :param image:
        :param parameters:
        :return:
        """
        with NamedTemporaryFile(
            mode="rt", prefix="kedro-vertexai", suffix=".yaml"
        ) as spec_output:
            # Auto-detect parameter types from provided parameter values
            param_types = ""
            if parameters:
                param_type_list = []
                for param_name in parameters.keys():
                    # Default to string type for runtime parameters
                    param_type_list.append(f"{param_name}:str")
                param_types = ",".join(param_type_list)

            self.compile(
                pipeline,
                image,
                output=spec_output.name,
                params=param_types,
            )

            job = aip.PipelineJob(
                display_name=self.run_name,
                template_path=spec_output.name,
                job_id=self.run_name,
                pipeline_root=f"gs://{self.run_config.root}",
                parameter_values=parameters or {},
                enable_caching=False,
            )

            job.submit(
                service_account=self.run_config.service_account,
                network=self.run_config.network.vpc,
                experiment=self.run_config.experiment_name,
            )

            return job

    def _generate_run_name(self, config: PluginConfig):  # noqa
        return config.run_config.experiment_name.rstrip("-") + "-{}".format(
            dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        )

    def compile(self, pipeline, image, output, params: List[str] = []):
        """
        Creates json file in given local output path
        :param pipeline:
        :param image:
        :param output:
        :param params: Pipeline parameters to be specified at run time.
        :return:
        """
        token = os.getenv("MLFLOW_TRACKING_TOKEN", "")
        pipeline_func = self.generator.generate_pipeline(
            pipeline, image, token, params=params
        )
        Compiler().compile(
            pipeline_func=pipeline_func,
            package_path=output,
        )
        self.log.info("Generated pipeline definition was saved to %s", str(output))

    def _cleanup_old_schedule(self, display_name: str):
        """Cleanup old schedules with a given display name.

        Args:
            display_name (str): Display name of the schedule.
        """
        existing_schedules = aip.PipelineJobSchedule.list(
            filter=f'display_name="{display_name}"'
        )
        self.log.info(
            f"Found {len(existing_schedules)} existing schedules with display name {display_name}"
        )

        for schedule in existing_schedules:
            schedule.delete()

        self.log.info(
            f"Cleaned up existing old schedules with display name {display_name}"
        )

    def schedule(
        self,
        pipeline: str,
        schedule_config: ScheduleConfig,
        parameter_values: Optional[Dict[str, Any]] = None,
    ):
        """
        Schedule pipeline to Vertex AI with given cron expression
        :param pipeline: Name of the Kedro pipeline to schedule.
        :param schedule_config: Schedule config.
        :param parameter_values: Kubeflow pipeline parameter values.
        :return:
        """
        self._cleanup_old_schedule(display_name=self.run_config.scheduled_run_name)

        with NamedTemporaryFile(
            mode="rt", prefix="kedro-vertexai", suffix=".yaml"
        ) as spec_output:
            self.compile(
                pipeline,
                self.run_config.image,
                output=spec_output.name,
            )

            job = aip.PipelineJob(
                display_name=self.run_name,
                template_path=spec_output.name,
                job_id=self.run_name,
                pipeline_root=f"gs://{self.run_config.root}",
                parameter_values=parameter_values or {},
                enable_caching=False,
            )

            cron_with_timezone = (
                f"TZ={schedule_config.timezone} {schedule_config.cron_expression}"
            )

            job.create_schedule(
                cron=cron_with_timezone,
                display_name=self.run_config.scheduled_run_name,
                start_time=schedule_config.start_time,
                end_time=schedule_config.end_time,
                allow_queueing=schedule_config.allow_queueing,
                max_run_count=schedule_config.max_run_count,
                max_concurrent_run_count=schedule_config.max_concurrent_run_count,
                service_account=self.run_config.service_account,
                network=self.run_config.network.vpc,
            )

        self.log.info("Pipeline scheduled to %s", cron_with_timezone)
