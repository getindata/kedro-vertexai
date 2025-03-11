import bz2
import os
from functools import lru_cache
from sys import version_info
from typing import Any, Dict

import cloudpickle
import fsspec
from google.cloud import aiplatform as aip
from kedro.io import AbstractDataset, MemoryDataset

from kedro_vertexai.config import dynamic_load_class
from kedro_vertexai.constants import KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME


class KedroVertexAIRunnerDataset(AbstractDataset):
    def __init__(
        self,
        storage_root: str,
        dataset_name: str,
        unique_id: str,
    ):
        self.storage_root = storage_root
        self.unique_id = unique_id
        self.dataset_name = dataset_name
        self.pickle_protocol = None if version_info[:2] > (3, 8) else 4

    @lru_cache()
    def _get_target_path(self):
        return (
            f"gs://{self.storage_root.strip('/')}/"
            f"{KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME}/{self.unique_id}/{self.dataset_name}.bin"
        )

    @lru_cache()
    def _get_storage_options(self):
        return {}

    def _load(self):
        with fsspec.open(
            self._get_target_path(), "rb", **self._get_storage_options()
        ) as f:
            with bz2.open(f, "rb") as stream:
                return cloudpickle.load(stream)

    def _save(self, data: Any) -> None:
        with fsspec.open(
            self._get_target_path(), "wb", **self._get_storage_options()
        ) as f:
            with bz2.open(f, "wb") as stream:
                cloudpickle.dump(data, stream, protocol=self.pickle_protocol)

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Kedro Vertex AI Pipelines",
            "dataset_name": self.dataset_name,
            "path": self._get_target_path(),
        }

    def __getattribute__(self, __name: None) -> Any:
        if __name == "__class__":
            return MemoryDataset.__getattribute__(MemoryDataset(), __name)
        return super().__getattribute__(__name)


class KedroVertexAIMetadataDataset(AbstractDataset):
    def __init__(
        self,
        base_dataset: str,
        display_name: str,
        base_dataset_args: Dict[str, Any],
        metadata: Dict[str, Any],
        schema: str = "system.Dataset",
    ) -> None:
        base_dataset_class: AbstractDataset = dynamic_load_class(base_dataset)

        self._base_dataset: AbstractDataset = base_dataset_class(**base_dataset_args)
        self._display_name = display_name
        self._artifact_uri = (
            f"{self._base_dataset._protocol}://{self._base_dataset._get_save_path()}"
        )
        self._artifact_schema = schema

        try:
            project_id = os.environ["GCP_PROJECT_ID"]
            region = os.environ["GCP_REGION"]
        except KeyError as e:
            self._logger.error(
                """Did you set GCP_PROJECT_ID and GCP_REGION env variables?
                They must be set in order to create Vertex AI artifact."""
            )
            raise e

        aip.init(
            project=project_id,
            location=region,
        )

        self._run_id = os.environ.get("KEDRO_CONFIG_RUN_ID")
        self._job_name = os.environ.get("KEDRO_CONFIG_JOB_NAME")

        if self._run_id is None or self._job_name is None:
            self._logger.warning(
                """KEDRO_CONFIG_RUN_ID and PIPELINE_JOB_NAME_PLACEHOLDER env variables are not set.
                                 Set them to assign it as artifact metadata."""
            )

        self._metadata = metadata

        super().__init__()

    def _load(self) -> Any:
        return self._base_dataset._load()

    def _save(self, data: Any) -> None:
        self._base_dataset._save(data)

        self._logger.info(
            f"Creating {self._display_name} artifact with uri {self._artifact_uri}"
        )

        aip.Artifact.create(
            schema_title=self._artifact_schema,
            display_name=self._display_name,
            uri=self._artifact_uri,
            metadata={
                "pipeline run id": self._run_id,
                "pipeline job name": self._job_name,
                **self._metadata,
            },
        )

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Kedro Vertex AI Pipelines",
            "display_name": self._display_name,
            "artifact_uri": self._artifact_uri,
            "base_dataset": self._base_dataset.__class__.__name__,
        }
