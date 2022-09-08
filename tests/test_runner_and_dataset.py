import os
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4

from kedro.io import DataCatalog, MemoryDataSet
from kedro.pipeline import node, pipeline

from kedro_vertexai.config import KedroVertexAIRunnerConfig
from kedro_vertexai.constants import (
    KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME,
    KEDRO_VERTEXAI_RUNNER_CONFIG,
)
from kedro_vertexai.vertex_ai.datasets import KedroVertexAIRunnerDataset
from kedro_vertexai.vertex_ai.runner import VertexAIPipelinesRunner


class TestVertexAIRunnerAndDataset(unittest.TestCase):
    def dummy_pipeline(self):
        identity = lambda x: x  # noqa
        return pipeline(
            [
                node(identity, inputs="input_data", outputs="i2", name="node1"),
                node(identity, inputs="i2", outputs="i3", name="node2"),
                node(identity, inputs="i3", outputs="output_data", name="node3"),
            ]
        )

    @contextmanager
    def patched_dataset(self) -> KedroVertexAIRunnerDataset:
        with TemporaryDirectory() as tmp_dir:
            target_path = Path(tmp_dir) / (uuid4().hex + ".bin")
        with patch.object(
            KedroVertexAIRunnerDataset,
            "_get_target_path",
            return_value=str(target_path.absolute()),
        ):
            yield KedroVertexAIRunnerDataset("", "unit_tests", uuid4().hex)

    @contextmanager
    def patched_runner(self) -> VertexAIPipelinesRunner:
        with patch.dict(
            os.environ,
            {
                KEDRO_VERTEXAI_RUNNER_CONFIG: KedroVertexAIRunnerConfig(
                    storage_root="unit_tests"
                ).json()
            },
            clear=False,
        ):
            with self.patched_dataset():
                yield VertexAIPipelinesRunner()

    def test_custom_runner_paths(self):
        run_id = uuid4().hex
        ds = KedroVertexAIRunnerDataset("storage_root", "unit_tests_dataset", run_id)
        target_path = ds._get_target_path()

        assert (
            target_path.startswith("gs://")
            and target_path.endswith(".bin")
            and all(
                part in target_path
                for part in (
                    "storage_root",
                    "unit_tests_dataset",
                    KEDRO_VERTEXAI_BLOB_TEMP_DIR_NAME,
                    run_id,
                )
            )
        ), "Invalid target path"

        assert (
            len(ds._get_storage_options()) == 0
        ), "Invalid storage config"  # as of 2022-08-01 it should be empty

    def test_custom_runner_can_save_python_objects_using_fsspec(self):
        class SomeClass:
            def __init__(self, data):
                self.data = data

        for obj, comparer in [
            (
                ["just", "a", "list"],
                lambda a, b: all(a[i] == b[i] for i in range(len(a))),
            ),
            (
                {"some": "dictionary"},
                lambda a, b: all(a[k] == b[k] for k in a.keys()),
            ),
            (set(["python", "set"]), lambda a, b: len(a - b) == 0),
            ("this is a string", lambda a, b: a == b),
            (1235, lambda a, b: a == b),
            (
                (1234, 5678),
                lambda a, b: all(a[i] == b[i] for i in range(len(a))),
            ),
            (
                SomeClass(["a", 123.0, 456, True]),
                lambda a, b: all(a.data[i] == b.data[i] for i in range(len(a.data))),
            ),
        ]:
            with self.subTest(object_to_save=obj, comparer=comparer):
                with self.patched_dataset() as ds:
                    ds.save(obj)
                    assert (
                        Path(ds._get_target_path()).stat().st_size > 0
                    ), "File does not seem to be saved"
                    assert comparer(
                        obj, ds.load()
                    ), "Objects are not the same after deserialization"

    def test_can_run_dummy_pipeline(self):
        with self.patched_runner() as runner:
            catalog = DataCatalog()
            input_data = ["yolo :)"]
            catalog.add("input_data", MemoryDataSet(data=input_data))
            results = runner.run(
                self.dummy_pipeline(),
                catalog,
            )
            assert results["output_data"] == input_data, "No output data found"

    def test_runner_fills_missing_datasets(self):
        with self.patched_runner() as runner:
            input_data = ["yolo :)"]
            catalog = DataCatalog()
            catalog.add("input_data", MemoryDataSet(data=input_data))
            for node_no in range(3):
                results = runner.run(
                    self.dummy_pipeline().filter(node_names=[f"node{node_no + 1}"]),
                    catalog,
                )
            assert results["output_data"] == input_data, "Invalid output data"
