import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from kedro_vertexai.vertex_ai.datasets import KedroVertexAIMetadataDataset


class TestKedroVertexAIMetadataDataset(unittest.TestCase):
    def test_dataset(self):
        with patch("kedro_vertexai.vertex_ai.datasets.aip.init"), patch(
            "kedro_vertexai.vertex_ai.datasets.aip.Artifact.create"
        ) as aip_artifact_create_mock, patch(
            "kedro_vertexai.vertex_ai.datasets.dynamic_load_class"
        ) as mock_dynamic_load_class:
            dataset_class_mock = mock_dynamic_load_class.return_value

            mock_dynamic_load_class.return_value.return_value._protocol = "gcs"
            mock_dynamic_load_class.return_value.return_value._get_save_path.return_value = Path(
                "save_path/file.csv"
            )
            mock_dynamic_load_class.return_value.return_value.__class__.__name__ == "some_package.SomeDataset"

            os.environ["GCP_PROJECT_ID"] = "project id"
            os.environ["GCP_REGION"] = "region"

            dataset = KedroVertexAIMetadataDataset(
                base_dataset="some_package.SomeDataset",
                display_name="dataset_name",
                base_dataset_args={"some_argument": "its_value"},
                metadata={"test_key": "Some additional info"},
            )

            mock_dynamic_load_class.assert_called_once()
            assert len(mock_dynamic_load_class.call_args.args)
            assert (
                mock_dynamic_load_class.call_args.args[0] == "some_package.SomeDataset"
            )

            dataset_class_mock.assert_called_once()
            assert "some_argument" in dataset_class_mock.call_args.kwargs
            assert dataset_class_mock.call_args.kwargs["some_argument"] == "its_value"

            assert dataset._artifact_uri == "gcs://save_path/file.csv"

            data_mock = MagicMock()
            dataset.save(data_mock)

            aip_artifact_create_mock.assert_called_once()
            assert (
                aip_artifact_create_mock.call_args.kwargs["schema_title"]
                == "system.Dataset"
            )
            assert (
                aip_artifact_create_mock.call_args.kwargs["display_name"]
                == "dataset_name"
            )
            assert (
                aip_artifact_create_mock.call_args.kwargs["uri"]
                == "gcs://save_path/file.csv"
            )
            assert (
                aip_artifact_create_mock.call_args.kwargs["metadata"]["test_key"]
                == "Some additional info"
            )

            dataset_info = dataset._describe()
            assert dataset_info["display_name"] == "dataset_name"
            assert dataset_info["artifact_uri"] == "gcs://save_path/file.csv"
