import bz2
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
        self, base_dataset: str, display_name: str, base_dataset_args: Dict[str, Any]
    ) -> None:
        base_dataset_class: AbstractDataset = dynamic_load_class(base_dataset)
        self._base_dataset = base_dataset_class(**base_dataset_args)
        self._display_name: str = display_name

        aip.init(
            project="gid-ml-ops-sandbox",
            location="europe-west3",
        )

        super().__init__()

    def _load(self) -> Any:
        return self._base_dataset._load()

    def _save(self, data: Any) -> None:
        self._base_dataset._save(data)
        aip.Artifact.create(
            schema_title="system.Dataset", display_name=self._display_name
        )  # , uri=DATASET_URI)

    def _describe(self) -> Dict[str, Any]:
        return {
            "info": "for use only within Kedro Vertex AI Pipelines",
            "display_name": self._display_name,
        }
