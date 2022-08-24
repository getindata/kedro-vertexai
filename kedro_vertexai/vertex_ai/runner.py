import os
from typing import Any, Dict

from kedro.io import AbstractDataSet, DataCatalog
from kedro.pipeline import Pipeline
from kedro.runner import SequentialRunner
from pluggy import PluginManager

from kedro_vertexai.config import KedroVertexAIRunnerConfig
from kedro_vertexai.constants import (
    KEDRO_CONFIG_JOB_NAME,
    KEDRO_VERTEXAI_RUNNER_CONFIG,
)
from kedro_vertexai.vertex_ai.datasets import KedroVertexAIRunnerDataset


class VertexAIPipelinesRunner(SequentialRunner):
    @classmethod
    def runner_name(cls):
        return f"{cls.__module__}.{cls.__qualname__}"

    def __init__(self, is_async: bool = False):
        super().__init__(is_async)
        self.runner_config_raw = os.environ.get(KEDRO_VERTEXAI_RUNNER_CONFIG).strip("'")
        self.runner_config: KedroVertexAIRunnerConfig = (
            KedroVertexAIRunnerConfig.parse_raw(self.runner_config_raw)
        )

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        unsatisfied = pipeline.inputs() - set(catalog.list())
        for ds_name in unsatisfied:
            catalog = catalog.shallow_copy()
            catalog.add(ds_name, self.create_default_data_set(ds_name))

        return super().run(pipeline, catalog, hook_manager, session_id)

    def create_default_data_set(self, ds_name: str) -> AbstractDataSet:
        return KedroVertexAIRunnerDataset(
            self.runner_config.storage_root,
            ds_name,
            os.environ.get(KEDRO_CONFIG_JOB_NAME),
        )
