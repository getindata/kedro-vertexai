import unittest

import yaml
from pydantic import ValidationError

from kedro_vertexai.config import PluginConfig

CONFIG_FULL = """
project_id: test-project-id
region: some-region
run_config:
  image: "gcr.io/project-image/test"
  image_pull_policy: "Always"
  experiment_name: "Test Experiment"
  scheduled_run_name: "scheduled run"
  description: "My awesome pipeline"
  ttl: 300
  volume:
    storageclass: default
    size: 3Gi
    access_modes: [ReadWriteOnce]
    keep: True
"""

CONFIG_MINIMAL = """
project_id: some-project
region: some-region
run_config:
    image: test
    experiment_name: test
"""


class TestPluginConfig(unittest.TestCase):
    def test_plugin_config(self):
        obj = yaml.safe_load(CONFIG_FULL)
        cfg = PluginConfig.parse_obj(obj)
        assert cfg.run_config.image == "gcr.io/project-image/test"
        assert cfg.run_config.image_pull_policy == "Always"
        assert cfg.run_config.experiment_name == "Test Experiment"
        assert cfg.run_config.scheduled_run_name == "scheduled run"
        assert cfg.run_config.resources_for("node1") == {
            "cpu": "500m",
            "memory": "1024Mi",
        }
        assert cfg.run_config.ttl == 300

    def test_defaults(self):
        cfg = PluginConfig.parse_obj(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.image_pull_policy == "IfNotPresent"
        assert cfg.run_config.description is None
        assert cfg.run_config.ttl == 3600 * 24 * 7

    def test_missing_required_config(self):
        with self.assertRaises(ValidationError):
            PluginConfig.parse_obj({})

    def test_resources_default_only(self):
        cfg = PluginConfig.parse_obj(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.resources_for("node2") == {
            "cpu": "500m",
            "memory": "1024Mi",
        }
        assert cfg.run_config.resources_for("node3") == {
            "cpu": "500m",
            "memory": "1024Mi",
        }

    def test_resources_no_default(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {"resources": {"__default__": {"cpu": None, "memory": None}}}
        )
        cfg = PluginConfig.parse_obj(obj)
        assert cfg.run_config.resources_for("node2") == {
            "cpu": None,
            "memory": None,
        }

    def test_resources_default_and_node_specific(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {
                "resources": {
                    "__default__": {"cpu": "200m", "memory": "64Mi"},
                    "node2": {"cpu": "100m"},
                }
            }
        )
        cfg = PluginConfig.parse_obj(obj)
        assert cfg.run_config.resources_for("node2") == {
            "cpu": "100m",
            "memory": "64Mi",
        }
        assert cfg.run_config.resources_for("node3") == {
            "cpu": "200m",
            "memory": "64Mi",
        }

    def test_parse_network_config(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {
                "network": {
                    "vpc": "projects/some-project-id/global/networks/some-vpc-name",
                    "host_aliases": [
                        {"ip": "10.10.10.10", "hostnames": ["mlflow.internal"]}
                    ],
                }
            }
        )
        cfg = PluginConfig.parse_obj(obj)
        assert (
            cfg.run_config.network.vpc
            == "projects/some-project-id/global/networks/some-vpc-name"
        )
        assert str(cfg.run_config.network.host_aliases[0].ip) == "10.10.10.10"
        assert (
            "mlflow.internal"
            in cfg.run_config.network.host_aliases[0].hostnames
        )

    def test_accept_default_vertex_ai_networking_config(self):
        cfg = PluginConfig.parse_obj(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.network.vpc is None
        assert cfg.run_config.network.host_aliases == []

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_reuse_run_name_for_scheduled_run_name(self):
        cfg = PluginConfig.parse_obj(
            {
                "run_config": {
                    "scheduled_run_name": "some run",
                    "experiment_name": "test",
                }
            }
        )
        assert cfg.run_config.scheduled_run_name == "some run"
