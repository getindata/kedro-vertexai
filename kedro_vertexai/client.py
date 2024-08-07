"""
Vertex AI Pipelines specific client, based on AIPlatformClient.
"""

import datetime as dt
import json
import logging
import os
from tempfile import NamedTemporaryFile

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

    def _cleanup_old_schedule(self, pipeline_name):
        """
        Removes old jobs scheduled for given pipeline name
        """
        for job in self.cloud_scheduler_client.list_jobs(parent=self.location):
            if "jobs/pipeline_pipeline" not in job.name:
                continue

            job_pipeline_name = json.loads(job.http_target.body)["pipelineSpec"][
                "pipelineInfo"
            ]["name"]
            if job_pipeline_name == pipeline_name:
                self.log.info(
                    "Found existing schedule for the pipeline at %s, deleting...",
                    job.schedule,
                )
                self.cloud_scheduler_client.delete_job(name=job.name)

    def schedule(
        self,
        pipeline,
        cron_expression,
        parameter_values=None,
        image_pull_policy="IfNotPresent",
    ):
        """
        Schedule pipeline to Vertex AI with given cron expression
        :param pipeline:
        :param cron_expression:
        :param parameter_values:
        :param image_pull_policy:
        :return:
        """
        self._cleanup_old_schedule(self.generator.get_pipeline_name())
        with NamedTemporaryFile(
            mode="rt", prefix="kedro-vertexai", suffix=".json"
        ) as spec_output:
            self.compile(
                pipeline,
                self.run_config.image,
                output=spec_output.name,
            )
            self.api_client.create_schedule_from_job_spec(
                job_spec_path=spec_output.name,
                time_zone="Etc/UTC",
                schedule=cron_expression,
                pipeline_root=f"gs://{self.run_config.root}",
                enable_caching=False,
                parameter_values=parameter_values or {},
            )

            self.log.info("Pipeline scheduled to %s", cron_expression)
