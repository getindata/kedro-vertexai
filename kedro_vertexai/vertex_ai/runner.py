import os
from typing import Any, Dict

from kedro.io import AbstractDataset, DataCatalog
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
            KedroVertexAIRunnerConfig.model_validate_json(self.runner_config_raw)
        )

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        run_id: str = None,
        **kwargs,
    ) -> Dict[str, Any]:

        unsatisfied = (pipeline.inputs() | pipeline.outputs()) - set(catalog.filter())
        for ds_name in unsatisfied:
            catalog[ds_name] = self.create_default_data_set(ds_name)
        return super().run(pipeline, catalog, hook_manager, run_id, **kwargs)

    def create_default_data_set(self, ds_name: str) -> AbstractDataset:
        return KedroVertexAIRunnerDataset(
            self.runner_config.storage_root,
            ds_name,
            os.environ.get(KEDRO_CONFIG_JOB_NAME),
        )
