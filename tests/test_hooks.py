import unittest
from unittest.mock import MagicMock, call, patch

import mlflow

from kedro_vertexai.constants import VERTEXAI_JOB_NAME_TAG, VERTEXAI_RUN_ID_TAG
from kedro_vertexai.hooks import MlflowTagsHook

from .utils import environment


@patch.object(mlflow, "set_tag")
class TestMlflowTagsHook(unittest.TestCase):
    def test_should_set_mlflow_tags(self, mlflow_set_tag: MagicMock):
        with environment(
            {"KEDRO_CONFIG_RUN_ID": "KFP_123", "KEDRO_CONFIG_JOB_NAME": "asd"}
        ), patch("kedro_vertexai.hooks.is_mlflow_enabled", return_value=True):
            MlflowTagsHook().before_node_run()

        mlflow_set_tag.assert_has_calls(
            [call(VERTEXAI_RUN_ID_TAG, "KFP_123"), call(VERTEXAI_JOB_NAME_TAG, "asd")],
            any_order=True,
        )

    def test_should_not_set_mlflow_tags_when_kubeflow_run_id_env_is_not_set(
        self, mlflow_set_tag
    ):
        with environment({}, delete_keys=["KEDRO_CONFIG_RUN_ID"]):
            MlflowTagsHook().before_node_run()

        mlflow_set_tag.assert_not_called()

    def test_should_not_set_mlflow_tags_when_kubeflow_run_id_env_is_empty(
        self, mlflow_set_tag
    ):
        with environment({"KEDRO_CONFIG_RUN_ID": ""}):
            MlflowTagsHook().before_node_run()

        mlflow_set_tag.assert_not_called()

    def test_should_not_set_mlflow_tags_when_mlflow_is_not_enabled(
        self, mlflow_set_tag
    ):
        # given
        real_import = __builtins__["__import__"]

        def mlflow_import_disabled(name, *args, **kw):
            if name == "mlflow":
                raise ImportError
            return real_import(name, *args, **kw)

        __builtins__["__import__"] = mlflow_import_disabled

        # when
        with environment({"KEDRO_CONFIG_RUN_ID": "KFP_123"}):
            MlflowTagsHook().before_node_run()

        # then
        mlflow_set_tag.assert_not_called()

        # cleanup
        __builtins__["__import__"] = real_import
