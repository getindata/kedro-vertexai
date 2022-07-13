"""Test kedro_vertexai module."""

import unittest
from time import sleep
from unittest.mock import MagicMock, patch

from google.cloud.aiplatform.pipeline_jobs import PipelineJob

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
        return VertexAIPipelinesClient(config, 'test_project_name', MagicMock())

    def test_compile(self):
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kedro_vertexai.client.aiplatform"
        ), patch("kfp.compiler.Compiler") as Compiler:
            compiler = Compiler.return_value

            client_under_test = self.create_client()
            client_under_test.compile(
                MagicMock("pipeline"), "image", "some_path", "run-name"
            )

            compiler.compile.assert_called_once()

    def test_run_once(self):
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kfp.compiler.Compiler"
        ), patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob.return_value

            client_under_test = self.create_client()
            client_under_test.run_once(MagicMock("pipeline"), "image")

            pipeline_client.run.assert_called_once()
            _, kwargs = pipeline_client.run.call_args
            assert kwargs["network"] == "my-vpc"

    def test_should_list_pipelines(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob

            pipeline_job1 = MagicMock(spec=PipelineJob)
            pipeline_job1.configure_mock(
                name="projects/29350373243/locations/europe-west4/pipelineJobs/run1",
                display_name="run1"
            )
            pipeline_job2 = MagicMock(spec=PipelineJob)
            pipeline_job2.configure_mock(
                name="projects/29350373243/locations/europe-west4/pipelineJobs/run2",
                display_name="run2"
            )
            pipeline_job3 = MagicMock(spec=PipelineJob)
            pipeline_job3.configure_mock(
                name="projects/123/locations/europe-west4/pipelineJobs/run3",
                display_name="run3"
            )

            pipeline_client.list.return_value = [pipeline_job1, pipeline_job2, pipeline_job3]

            client_under_test = self.create_client()
            tabulation = client_under_test.list_pipelines()
            expected_output = """
            |Name    ID
            |------  -------------------------------------------------------------
            |run1    projects/29350373243/locations/europe-west4/pipelineJobs/run1
            |run2    projects/29350373243/locations/europe-west4/pipelineJobs/run2
            |run3    projects/123/locations/europe-west4/pipelineJobs/run3"""

            assert tabulation == strip_margin(expected_output)

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_should_schedule_pipeline(self):
        with patch("kedro_vertexai.generator.PipelineGenerator"), patch(
            "kedro_vertexai.client.aiplatform"
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

        with patch(
            "kedro_vertexai.client.PipelineGenerator"
        ) as generator, patch(
            "kedro_vertexai.client.aiplatform"
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

    def test_wait_for_completion_success_condition(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob

            job_state = MagicMock(spec=PipelineJob)
            job_state.state.name = PipelineStatus.PIPELINE_STATE_SUCCEEDED
            pipeline_client.get.return_value = job_state

            client = self.create_client()
            result = client.wait_for_completion(30)
            assert result.is_success, "Pipeline should be determined as successful"
            assert isinstance(
                result.job_data, PipelineJob
            ), "Field job_data should have value in finished pipelines"

    def test_wait_for_completion_failure_condition(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob

            for state in (
                PipelineStatus.PIPELINE_STATE_CANCELLED,
                PipelineStatus.PIPELINE_STATE_FAILED,
            ):
                job_state = MagicMock(spec=PipelineJob)
                job_state.state.name = state
                with patch.object(
                    pipeline_client,
                    "get",
                    return_value=job_state,
                ):
                    client = self.create_client()
                    result = client.wait_for_completion(10)
                    assert (
                        not result.is_success
                    ), "Pipeline should be determined as failed"
                    assert isinstance(
                        result.job_data, PipelineJob
                    ), "Field job_data should have value in finished pipelines"

    def test_wait_for_completion_timeout(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob

            pipeline_client.get.side_effect = lambda _: {
                "state": PipelineStatus.PIPELINE_STATE_SUCCEEDED,
                "hacky :)": sleep(60.0),
            }
            client = self.create_client()
            result = client.wait_for_completion(3)
            assert not result.is_success, "Pipeline should be determined as failed"
            assert (
                result.job_data is None
            ), "Timed-out pipelines will not have job details"
            assert (
                "max timeout" in result.state.lower()
            ), "Final state seems invalid"

    def test_wait_for_completion_intervals(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            pipeline_client = ai_client.PipelineJob

            job_state = MagicMock(spec=PipelineJob)
            job_state.state.name = PipelineStatus.PIPELINE_STATE_RUNNING
            pipeline_client.get.return_value = job_state
            timeout = 2
            interval = 0.1
            tolerance = 2
            client = self.create_client()
            client.wait_for_completion(2, 0.1)
            assert (
                (timeout / interval) - tolerance
                <= pipeline_client.get.call_count
                < (timeout / interval) + tolerance
            ), "Number of calls to the API within the specified interval is invalid"

    def test_wait_for_completion_api_errors(self):
        with patch("kedro_vertexai.client.aiplatform") as ai_client:
            with patch("kedro_vertexai.client.VertexAIPipelinesClient.log.error") as logger:

                pipeline_client = ai_client.PipelineJob
                pipeline_client.get.side_effect = Exception()

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
                    pipeline_client.get.call_count == 7
                ), "Invalid number of API calls"
