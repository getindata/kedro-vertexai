import os
import warnings
from typing import Iterable

from kedro.config import ConfigLoader
from kedro.framework.hooks import hook_impl

from kedro_vertexai.constants import (
    KEDRO_CONFIG_JOB_NAME,
    KEDRO_CONFIG_RUN_ID,
    VERTEXAI_JOB_NAME_TAG,
    VERTEXAI_RUN_ID_TAG,
)
from kedro_vertexai.context_helper import EnvTemplatedConfigLoader
from kedro_vertexai.runtime_config import CONFIG_HOOK_DISABLED
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


if not CONFIG_HOOK_DISABLED:

    class KedoVertexAIConfigLoaderHook:
        @hook_impl
        def register_config_loader(self, conf_paths: Iterable[str]) -> ConfigLoader:
            return EnvTemplatedConfigLoader(conf_paths)

else:

    class KedoVertexAIConfigLoaderHook:
        pass

    warnings.warn(
        "KEDRO_VERTEXAI_DISABLE_CONFIG_HOOK environment variable is set "
        "and EnvTemplatedConfigLoader will not be used which means formatted "
        "config values like ${run_id} will not be substituted at runtime"
    )

mlflow_tags_hook = MlflowTagsHook()
env_templated_config_loader_hook = KedoVertexAIConfigLoaderHook()
