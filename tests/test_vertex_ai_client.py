"""Test kedro_vertexai module."""

import unittest
from unittest.mock import MagicMock, patch

from kedro_vertexai.client import VertexAIPipelinesClient
from kedro_vertexai.config import PluginConfig, ScheduleConfig
from kedro_vertexai.utils import strip_margin


class TestVertexAIClient(unittest.TestCase):
    def create_client(self):
        config = PluginConfig.parse_obj(
            {
                "project_id": "PROJECT_ID",
                "region": "REGION",
                "run_config": {
                    "image": "IMAGE",
                    "root": "BUCKET/PREFIX",
                    "network": {"vpc": "my-vpc"},
                    "experiment_name": "experiment-name",
                },
            }
        )
        return VertexAIPipelinesClient(config, MagicMock(), MagicMock())

    def test_compile(self):
        with patch("kedro_vertexai.client.PipelineGenerator"), patch(
            "kedro_vertexai.client.aip.init"
        ), patch("kfp.compiler.Compiler") as Compiler:
            compiler = Compiler.return_value

            client_under_test = self.create_client()
            client_under_test.compile(MagicMock("pipeline"), "image", "some_path")

            compiler.compile.assert_called_once()

    def test_should_list_pipelines(self):
        job1 = MagicMock()
        job1.display_name = "vertex-ai-plugin-demo-20240717134831"
        job1.name = "vertex-ai-plugin-demo-20240717134831"

        job2 = MagicMock()
        job2.display_name = "vertex-ai-plugin-demo-20240717134258"
        job2.name = "vertex-ai-plugin-demo-20240717134258"

        job3 = MagicMock()
        job3.display_name = "vertex-ai-plugin-demo-20240717120026"
        job3.name = "vertex-ai-plugin-demo-20240717120026"

        jobs = [job1, job2, job3]

        with patch("kedro_vertexai.client.aip.PipelineJob") as PipelineJob, patch(
            "kedro_vertexai.client.aip.init"
        ):
            PipelineJob.list.return_value = jobs

            client_under_test = self.create_client()
            tabulation = client_under_test.list_pipelines()

            expected_output = """
            |Name                                  ID
            |------------------------------------  ------------------------------------
            |vertex-ai-plugin-demo-20240717134831  vertex-ai-plugin-demo-20240717134831
            |vertex-ai-plugin-demo-20240717134258  vertex-ai-plugin-demo-20240717134258
            |vertex-ai-plugin-demo-20240717120026  vertex-ai-plugin-demo-20240717120026"""
            assert tabulation == strip_margin(expected_output)

    def test_should_schedule_pipeline(self):
        with patch("kedro_vertexai.client.PipelineGenerator"), patch(
            "kedro_vertexai.client.aip.PipelineJob"
        ) as PipelineJob, patch("kedro_vertexai.client.Compiler"), patch(
            "kedro_vertexai.client.aip.init"
        ):
            job = PipelineJob.return_value

            client_under_test = self.create_client()
            client_under_test.schedule(
                MagicMock("pipeline"),
                ScheduleConfig(cron_expression="0 0 12 * *", timezone="Etc/UTC"),
            )

            # job.create_schedule.assert_not_called()
            _, kwargs = job.create_schedule.call_args
            assert kwargs["cron"] == "TZ=Etc/UTC 0 0 12 * *"
            assert kwargs["display_name"] is None
            assert kwargs["start_time"] is None
            assert kwargs["end_time"] is None
            assert kwargs["allow_queueing"] is False
            assert kwargs["max_run_count"] is None
            assert kwargs["max_concurrent_run_count"] == 1
            assert kwargs["service_account"] is None
            assert kwargs["network"] == "my-vpc"

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_should_remove_old_schedule(self):
        def mock_job(job_name, pipeline_name=None):
            if pipeline_name:
                body = (
                    '{"pipelineSpec": {"pipelineInfo": {"name": "'
                    + pipeline_name
                    + '"}}}'
                )
            else:
                body = ""
            return type(
                "obj",
                (object,),
                {
                    "schedule": "* * * * *",
                    "name": job_name,
                    "http_target": type("obj", (object,), {"body": body}),
                },
            )

        with patch("kedro_vertexai.client.PipelineGenerator") as generator, patch(
            "kedro_vertexai.client.AIPlatformClient"
        ) as AIPlatformClient, patch("kfp.v2.compiler.Compiler"):
            # given
            ai_client = AIPlatformClient.return_value
            client_under_test = self.create_client()
            generator.return_value.get_pipeline_name.return_value = "unittest-pipeline"
            self.cloud_scheduler_client_mock.list_jobs.return_value = [
                # not removed (some other job)
                mock_job(job_name="some-job"),
                # not removed (some other pipeline)
                mock_job(
                    job_name="projects/.../locations/.../jobs/pipeline_pipeline_abc",
                    pipeline_name="some-other-pipeline",
                ),
                # removed
                mock_job(
                    job_name="projects/.../locations/.../jobs/pipeline_pipeline_def",
                    pipeline_name="unittest-pipeline",
                ),
            ]

            # when
            client_under_test.schedule(MagicMock("pipeline"), None, None, "0 0 12 * *")

            # then
            ai_client.create_schedule_from_job_spec.assert_called_once()
            self.cloud_scheduler_client_mock.delete_job.assert_called_once()
            (
                args,
                kwargs,
            ) = self.cloud_scheduler_client_mock.delete_job.call_args
            assert (
                kwargs["name"]
                == "projects/.../locations/.../jobs/pipeline_pipeline_def"
            )
