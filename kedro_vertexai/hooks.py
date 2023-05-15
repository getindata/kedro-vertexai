import os

from kedro.framework.hooks import hook_impl

from kedro_vertexai.constants import (
    KEDRO_CONFIG_JOB_NAME,
    KEDRO_CONFIG_RUN_ID,
    VERTEXAI_JOB_NAME_TAG,
    VERTEXAI_RUN_ID_TAG,
)
from kedro_vertexai.utils import is_mlflow_enabled


class MlflowTagsHook:
    """Adds `kubeflow_run_id` to MLFlow tags based on environment variables"""

    @hook_impl
    def before_node_run(self) -> None:
        if is_mlflow_enabled():
            import mlflow

            if run_id := os.getenv(KEDRO_CONFIG_RUN_ID, None):
                mlflow.set_tag(VERTEXAI_RUN_ID_TAG, run_id)

            if job_name := os.getenv(KEDRO_CONFIG_JOB_NAME, None):
                mlflow.set_tag(VERTEXAI_JOB_NAME_TAG, job_name)


mlflow_tags_hook = MlflowTagsHook()
