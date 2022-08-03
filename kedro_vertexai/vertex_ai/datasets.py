import bz2
from functools import lru_cache
from sys import version_info
from typing import Any, Dict

import cloudpickle
import fsspec
from kedro.io import AbstractDataSet

from kedro_vertexai.constants import KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME


class KedroVertexAIRunnerDataset(AbstractDataSet):
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
