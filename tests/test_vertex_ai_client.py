"""Test kedro_vertexai module."""

import unittest
from time import sleep
from unittest.mock import MagicMock, patch

from kedro_vertexai.client import VertexAIPipelinesClient
from kedro_vertexai.config import PluginConfig
from kedro_vertexai.data_models import PipelineStatus
from kedro_vertexai.utils import strip_margin


class TestVertexAIClient(unittest.TestCase):
    @patch("kedro_vertexai.client.CloudSchedulerClient")
    def create_client(self, cloud_scheduler_client_mock):
        self.cloud_scheduler_client_mock = (
            cloud_scheduler_client_mock.return_value
        )
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
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kedro_vertexai.client.AIPlatformClient"
        ), patch("kfp.v2.compiler.Compiler") as Compiler:
            compiler = Compiler.return_value

            client_under_test = self.create_client()
            client_under_test.compile(
                MagicMock("pipeline"), "image", "some_path", "run-name"
            )

            compiler.compile.assert_called_once()

    def test_run_once(self):
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kedro_vertexai.client.AIPlatformClient"
        ) as AIPlatformClient, patch("kfp.v2.compiler.Compiler"):
            ai_client = AIPlatformClient.return_value

            run_mock = {"run": "mock"}
            ai_client.create_run_from_job_spec.return_value = run_mock
            client_under_test = self.create_client()
            run = client_under_test.run_once(MagicMock("pipeline"), "image")

            assert run_mock == run
            _, kwargs = ai_client.create_run_from_job_spec.call_args
            assert kwargs["network"] == "my-vpc"

    def test_should_list_pipelines(self):
        with patch(
            "kedro_vertexai.client.AIPlatformClient"
        ) as AIPlatformClient:
            ai_client = AIPlatformClient.return_value
            ai_client.list_jobs.return_value = {
                "pipelineJobs": [
                    {
                        "displayName": "run1",
                        "name": "projects/29350373243/locations/"
                        "europe-west4/pipelineJobs/run1",
                    },
                    {
                        "displayName": "run2",
                        "name": "projects/29350373243/locations/"
                        "europe-west4/pipelineJobs/run2",
                    },
                    {
                        "name": "projects/123/locations/"
                        "europe-west4/pipelineJobs/no-display-name",
                    },
                ]
            }

            client_under_test = self.create_client()
            tabulation = client_under_test.list_pipelines()

            expected_output = """
            |Name    ID
            |------  ----------------------------------------------------------------
            |run1    projects/29350373243/locations/europe-west4/pipelineJobs/run1
            |run2    projects/29350373243/locations/europe-west4/pipelineJobs/run2
            |        projects/123/locations/europe-west4/pipelineJobs/no-display-name"""
            assert tabulation == strip_margin(expected_output)

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_should_schedule_pipeline(self):
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kedro_vertexai.client.AIPlatformClient"
        ) as AIPlatformClient, patch("kfp.v2.compiler.Compiler"):
            ai_client = AIPlatformClient.return_value

            client_under_test = self.create_client()
            client_under_test.schedule(
                MagicMock("pipeline"), None, None, "0 0 12 * *"
            )

            ai_client.create_schedule_from_job_spec.assert_not_called()
            args, kwargs = ai_client.create_schedule_from_job_spec.call_args
            assert kwargs["time_zone"] == "Etc/UTC"
            assert kwargs["enable_caching"] is False
            assert kwargs["schedule"] == "0 0 12 * *"
            assert kwargs["pipeline_root"] == "gs://BUCKET/PREFIX"

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

        with patch(
            "kedro_vertexai.client.PipelineGenerator"
        ) as generator, patch(
            "kedro_vertexai.client.AIPlatformClient"
        ) as AIPlatformClient, patch(
            "kfp.v2.compiler.Compiler"
        ):
            # given
            ai_client = AIPlatformClient.return_value
            client_under_test = self.create_client()
            generator.return_value.get_pipeline_name.return_value = (
                "unittest-pipeline"
            )
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
            client_under_test.schedule(
                MagicMock("pipeline"), None, None, "0 0 12 * *"
            )

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

    @patch("kedro_vertexai.client.AIPlatformClient")
    def test_wait_for_completion_success_condition(self, ai_client):
        ai_client.return_value.get_job.return_value = {
            "state": PipelineStatus.PIPELINE_STATE_SUCCEEDED
        }

        client = self.create_client()
        result = client.wait_for_completion(30)
        assert result.is_success, "Pipeline should be determined as successful"
        assert isinstance(
            result.job_data, dict
        ), "Field job_data should have value in finished pipelines"

    def test_wait_for_completion_failure_condition(self):

        for state in (
            PipelineStatus.PIPELINE_STATE_CANCELLED,
            PipelineStatus.PIPELINE_STATE_FAILED,
        ):
            with patch(
                "kedro_vertexai.client.AIPlatformClient.get_job",
                return_value={"state": state},
            ):
                client = self.create_client()
                result = client.wait_for_completion(30)
                assert (
                    not result.is_success
                ), "Pipeline should be determined as failed"
                assert isinstance(
                    result.job_data, dict
                ), "Field job_data should have value in finished pipelines"

    @patch("kedro_vertexai.client.AIPlatformClient")
    @patch(
        "kedro_vertexai.client.AIPlatformClient.get_job",
        side_effect=lambda _: {
            "state": PipelineStatus.PIPELINE_STATE_SUCCEEDED,
            "hacky :)": sleep(60.0),
        },
    )
    def test_wait_for_completion_timeout(self, _, __):
        client = self.create_client()
        result = client.wait_for_completion(3)
        assert not result.is_success, "Pipeline should be determined as failed"
        assert (
            result.job_data is None
        ), "Timed-out pipelines will not have job details"
        assert (
            "max timeout" in result.state.lower()
        ), "Final state seems invalid"

    @patch(
        "kedro_vertexai.client.AIPlatformClient.get_job",
        return_value={"state": PipelineStatus.PIPELINE_STATE_RUNNING},
    )
    def test_wait_for_completion_intervals(self, get_job_fn):
        timeout = 2
        interval = 0.1
        tolerance = 2
        client = self.create_client()
        client.wait_for_completion(2, 0.1)
        assert (
            (timeout / interval) - tolerance
            <= get_job_fn.call_count
            < (timeout / interval) + tolerance
        ), "Number of calls to the API within the specified interval is invalid"

    @patch("kedro_vertexai.client.AIPlatformClient")
    @patch("kedro_vertexai.client.VertexAIPipelinesClient.log.error")
    def test_wait_for_completion_api_errors(self, logger, ai_client):
        ai_client.return_value.get_job.side_effect = Exception()

        client = self.create_client()
        result = client.wait_for_completion(
            5, interval_seconds=0.01, max_api_fails=7
        )
        assert (
            not result.is_success and result.state == "Internal exception"
        ), "When API rises many times, end status should be failed"
        assert (
            logger.call_count == 7
        ), "Invalid number of logger calls on exception"
        assert (
            ai_client.return_value.get_job.call_count == 7
        ), "Invalid number of API calls"
