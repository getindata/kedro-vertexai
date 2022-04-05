import os
from contextlib import contextmanager

from kedro_vertexai.config import PluginConfig

test_config = PluginConfig.parse_obj(
    {
        "project_id": "test-project-id",
        "region": "test",
        "run_config": {
            "image": "gcr.io/project-image/test",
            "image_pull_policy": "Always",
            "experiment_name": "Test Experiment",
            "run_name": "test run",
            "volume": {
                "storageclass": "default",
                "size": "3Gi",
                "access_modes": "[ReadWriteOnce]",
            },
        },
    }
)


@contextmanager
def environment(env, delete_keys=None):
    original_environ = os.environ.copy()
    os.environ.update(env)
    if delete_keys is None:
        delete_keys = []
    for key in delete_keys:
        os.environ.pop(key, None)

    yield
    os.environ = original_environ
