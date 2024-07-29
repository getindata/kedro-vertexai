"""
Vertex AI Pipelines specific client, based on AIPlatformClient.
"""

import datetime as dt
import logging
import os
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional

from google.cloud import aiplatform as aip
from google.cloud.aiplatform import PipelineJob
from google.cloud.scheduler_v1.services.cloud_scheduler import (
    CloudSchedulerClient,
)
from kfp import compiler
from tabulate import tabulate

from .config import PluginConfig
from .generator import PipelineGenerator


class VertexAIPipelinesClient:
    """
    Client for Vertex AI Pipelines.
    """

    log = logging.getLogger(__name__)

    def __init__(self, config: PluginConfig, project_name, context):

        aip.init(project=config.project_id, location=config.region)
        self.cloud_scheduler_client = CloudSchedulerClient()
        self.location = f"projects/{config.project_id}/locations/{config.region}"
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
            self.compile(
                pipeline,
                image,
                output=spec_output.name,
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
            )

            return job

    def _generate_run_name(self, config: PluginConfig):  # noqa
        return config.run_config.experiment_name.rstrip("-") + "-{}".format(
            dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
        )

    def compile(
        self,
        pipeline,
        image,
        output,
    ):
        """
        Creates json file in given local output path
        :param pipeline:
        :param image:
        :param output:
        :return:
        """
        token = os.getenv("MLFLOW_TRACKING_TOKEN", "")
        pipeline_func = self.generator.generate_pipeline(pipeline, image, token)
        compiler.Compiler().compile(
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
        cron_expression: str,
        timezone: str,
        parameter_values: Optional[Dict[str, Any]] = None,
    ):
        """
        Schedule pipeline to Vertex AI with given cron expression
        :param pipeline: Name of the Kedro pipeline to schedule.
        :param cron_expression: Schedule cron expression.
        :param timezone: Cron expression timezone. May only be a valid string from IANA time zone database.
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
            cron_with_timezone = f"TZ={timezone} {cron_expression}"

            job.create_schedule(
                display_name=self.run_config.scheduled_run_name,
                cron=cron_with_timezone,
                service_account=self.run_config.service_account,
                network=self.run_config.network.vpc,
            )

        self.log.info("Pipeline scheduled to %s", cron_with_timezone)
