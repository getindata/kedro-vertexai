import unittest
from unittest.mock import patch

import yaml
from pydantic import ValidationError

from kedro_vertexai.config import PluginConfig, dynamic_init_class
from kedro_vertexai.grouping import IdentityNodeGrouper, TagNodeGrouper

CONFIG_FULL = """
project_id: test-project-id
region: some-region
run_config:
  image: "gcr.io/project-image/test"
  experiment_name: "Test Experiment"
  experiment_description: "Test Experiment Description."
  scheduled_run_name: "scheduled run"
  description: "My awesome pipeline"
  service_account: test@pipelines.gserviceaccount.com
  grouping:
    cls: "kedro_vertexai.grouping.IdentityNodeGrouper"
    params:
        tag_prefix: "group."
  ttl: 300
  network:
    vpc: my-vpc
    host_aliases:
        - ip: 127.0.0.1
          hostnames:
            - myself.local
            - me.local
  volume:
    storageclass: default
    size: 3Gi
    access_modes: [ReadWriteOnce]
    keep: True
  mlflow:
    request_header_provider_params:
      service_account: test@example.com
      client_id: xyz
"""

CONFIG_MINIMAL = """
project_id: some-project
region: some-region
run_config:
    image: test
    experiment_name: test
"""


class TestPluginConfig(unittest.TestCase):
    def test_grouping_config(self):
        cfg = PluginConfig.model_validate(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.grouping is not None
        assert (
            cfg.run_config.grouping.cls == "kedro_vertexai.grouping.IdentityNodeGrouper"
        )
        c_obj = dynamic_init_class(cfg.run_config.grouping.cls, None)
        assert isinstance(c_obj, IdentityNodeGrouper)
        cfg_tag_group = """
project_id: some-project
region: some-region
run_config:
    image: test
    experiment_name: test
    grouping:
        cls: kedro_vertexai.grouping.TagNodeGrouper
        params:
            tag_prefix: "group."
"""
        cfg = PluginConfig.model_validate(yaml.safe_load(cfg_tag_group))
        assert cfg.run_config.grouping is not None
        c_obj = dynamic_init_class(
            cfg.run_config.grouping.cls, None, **cfg.run_config.grouping.params
        )
        assert isinstance(c_obj, TagNodeGrouper)
        assert c_obj.tag_prefix == "group."

    @patch("kedro_vertexai.config.logger.error")
    def test_grouping_config_error(self, log_error):
        cfg_tag_group = """
project_id: some-project
region: some-region
run_config:
    image: test
    experiment_name: test
    grouping:
        cls: "kedro_vertexai.grouping.TagNodeGrouper"
        params:
            foo: "bar:"
"""
        cfg = PluginConfig.model_validate(yaml.safe_load(cfg_tag_group))
        c = dynamic_init_class(
            cfg.run_config.grouping.cls, None, **cfg.run_config.grouping.params
        )
        assert c is None
        log_error.assert_called_once()

    def test_plugin_config(self):
        obj = yaml.safe_load(CONFIG_FULL)
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.image == "gcr.io/project-image/test"
        assert cfg.run_config.experiment_name == "Test Experiment"
        assert cfg.run_config.experiment_description == "Test Experiment Description."
        assert cfg.run_config.scheduled_run_name == "scheduled run"
        assert cfg.run_config.service_account == "test@pipelines.gserviceaccount.com"
        assert cfg.run_config.network.vpc == "my-vpc"
        assert str(cfg.run_config.network.host_aliases[0].ip) == "127.0.0.1"
        assert "myself.local" in cfg.run_config.network.host_aliases[0].hostnames
        assert "me.local" in cfg.run_config.network.host_aliases[0].hostnames
        assert cfg.run_config.resources_for("node1") == {
            "cpu": "500m",
            "gpu": None,
            "memory": "1024Mi",
        }
        assert cfg.run_config.ttl == 300

    def test_defaults(self):
        cfg = PluginConfig.model_validate(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.description is None
        assert cfg.run_config.ttl == 3600 * 24 * 7

    def test_missing_required_config(self):
        with self.assertRaises(ValidationError):
            PluginConfig.model_validate({})

    def test_resources_default_only(self):
        cfg = PluginConfig.model_validate(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.resources_for("node2") == {
            "cpu": "500m",
            "gpu": None,
            "memory": "1024Mi",
        }
        assert cfg.run_config.resources_for("node3") == {
            "cpu": "500m",
            "gpu": None,
            "memory": "1024Mi",
        }

    def test_node_selectors_default_only(self):
        cfg = PluginConfig.model_validate(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.node_selectors_for("node2") == {}
        assert cfg.run_config.node_selectors_for("node3") == {}

    def test_resources_no_default(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {"resources": {"__default__": {"cpu": None, "memory": None}}}
        )
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.resources_for("node2") == {
            "cpu": None,
            "gpu": None,
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
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.resources_for("node2") == {
            "cpu": "100m",
            "gpu": None,
            "memory": "64Mi",
        }
        assert cfg.run_config.resources_for("node3") == {
            "cpu": "200m",
            "gpu": None,
            "memory": "64Mi",
        }

    def test_resources_default_and_tag_specific(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {
                "resources": {
                    "__default__": {"cpu": "200m", "memory": "64Mi"},
                    "tag1": {"cpu": "100m", "gpu": "2"},
                }
            }
        )
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.resources_for("node2", {"tag1"}) == {
            "cpu": "100m",
            "gpu": "2",
            "memory": "64Mi",
        }
        assert cfg.run_config.resources_for("node3") == {
            "cpu": "200m",
            "gpu": None,
            "memory": "64Mi",
        }

    def test_resources_node_and_tag_specific(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {
                "resources": {
                    "__default__": {"cpu": "200m", "memory": "64Mi"},
                    "node2": {"cpu": "300m"},
                    "tag1": {"cpu": "100m", "gpu": "2"},
                }
            }
        )
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.resources_for("node2", {"tag1"}) == {
            "cpu": "300m",
            "gpu": "2",
            "memory": "64Mi",
        }

    def test_node_selectors_node_and_tag_specific(self):
        obj = yaml.safe_load(CONFIG_MINIMAL)
        obj["run_config"].update(
            {
                "node_selectors": {
                    "node2": {"cloud.google.com/gke-accelerator": "NVIDIA_TESLA_K80"},
                    "tag1": {"cloud.google.com/gke-accelerator": "NVIDIA_TESLA_T4"},
                }
            }
        )
        cfg = PluginConfig.model_validate(obj)
        assert cfg.run_config.node_selectors_for("node2", {"tag1"}) == {
            "cloud.google.com/gke-accelerator": "NVIDIA_TESLA_K80",
        }
        assert cfg.run_config.node_selectors_for("node3", {"tag1"}) == {
            "cloud.google.com/gke-accelerator": "NVIDIA_TESLA_T4",
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
        cfg = PluginConfig.model_validate(obj)
        assert (
            cfg.run_config.network.vpc
            == "projects/some-project-id/global/networks/some-vpc-name"
        )
        assert str(cfg.run_config.network.host_aliases[0].ip) == "10.10.10.10"
        assert "mlflow.internal" in cfg.run_config.network.host_aliases[0].hostnames

    def test_accept_default_vertex_ai_networking_config(self):
        cfg = PluginConfig.model_validate(yaml.safe_load(CONFIG_MINIMAL))
        assert cfg.run_config.network.vpc is None
        assert cfg.run_config.network.host_aliases == []

    @unittest.skip(
        "Scheduling feature is temporarily disabled https://github.com/getindata/kedro-vertexai/issues/4"
    )
    def test_reuse_run_name_for_scheduled_run_name(self):
        cfg = PluginConfig.model_validate(
            {
                "run_config": {
                    "scheduled_run_name": "some run",
                    "experiment_name": "test",
                }
            }
        )
        assert cfg.run_config.scheduled_run_name == "some run"
