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
                    "scheduled_run_name": "scheduled-run",
                },
            }
        )
        return VertexAIPipelinesClient(config, MagicMock(), MagicMock())

    def test_compile(self):
        with patch("kedro_vertexai.client.PipelineGenerator"), patch(
            "kedro_vertexai.client.aip.init"
        ), patch("kedro_vertexai.client.Compiler") as Compiler:
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
        ), patch(
            "kedro_vertexai.client.aip.PipelineJobSchedule"
        ):
            job = PipelineJob.return_value

            client_under_test = self.create_client()
            client_under_test.schedule(
                MagicMock("pipeline"),
                ScheduleConfig(cron_expression="0 0 12 * *", timezone="Etc/UTC"),
            )

            _, kwargs = job.create_schedule.call_args
            assert kwargs["cron"] == "TZ=Etc/UTC 0 0 12 * *"
            assert kwargs["display_name"] == "scheduled-run"
            assert kwargs["start_time"] is None
            assert kwargs["end_time"] is None
            assert kwargs["allow_queueing"] is False
            assert kwargs["max_run_count"] is None
            assert kwargs["max_concurrent_run_count"] == 1
            assert kwargs["service_account"] is None
            assert kwargs["network"] == "my-vpc"

    def test_should_remove_old_schedule(self):
        with patch("kedro_vertexai.client.PipelineGenerator") as generator, patch(
            "kedro_vertexai.client.aip.PipelineJobSchedule"
        ) as PipelineJobSchedule, patch(
            "kedro_vertexai.client.aip.PipelineJob"
        ) as PipelineJob, patch(
            "kedro_vertexai.client.Compiler"
        ), patch(
            "kedro_vertexai.client.aip.init"
        ):
            # given
            job_schedule = PipelineJobSchedule.return_value
            job = PipelineJob.return_value
            client_under_test = self.create_client()
            generator.return_value.get_pipeline_name.return_value = "unittest-pipeline"
            PipelineJobSchedule.list.return_value = [job_schedule]

            # when
            client_under_test.schedule(MagicMock("pipeline"), MagicMock())

            # then
            job.create_schedule.assert_called_once()
            job_schedule.delete.assert_called_once()
