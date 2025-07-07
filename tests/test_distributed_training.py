"""Test distributed training configuration and generation"""

import unittest
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import kfp
import yaml
from kedro.pipeline import Pipeline, node
from kfp.compiler import Compiler
from pydantic import ValidationError

from kedro_vertexai.config import (
    DistributedTrainingConfig,
    PluginConfig,
    WorkerPoolConfig,
)
from kedro_vertexai.generator import PipelineGenerator
from tests.utils import environment


def dummy_train_func(input_data: str) -> str:
    return input_data  # pragma: no cover


def dummy_preprocess_func(input_data: str) -> str:
    return input_data  # pragma: no cover


class TestDistributedTrainingConfig(unittest.TestCase):
    """Test distributed training configuration classes"""

    def test_worker_pool_config_defaults(self):
        """Test WorkerPoolConfig with default values"""
        config = WorkerPoolConfig()
        self.assertEqual(config.machine_type, "n1-standard-4")
        self.assertEqual(config.replica_count, 1)
        self.assertIsNone(config.accelerator_type)
        self.assertIsNone(config.accelerator_count)

    def test_worker_pool_config_custom_values(self):
        """Test WorkerPoolConfig with custom values"""
        config = WorkerPoolConfig(
            machine_type="n1-standard-8",
            replica_count=4,
            accelerator_type="NVIDIA_TESLA_T4",
            accelerator_count=2,
        )
        self.assertEqual(config.machine_type, "n1-standard-8")
        self.assertEqual(config.replica_count, 4)
        self.assertEqual(config.accelerator_type, "NVIDIA_TESLA_T4")
        self.assertEqual(config.accelerator_count, 2)

    def test_distributed_training_config_defaults(self):
        """Test DistributedTrainingConfig with default values"""
        config = DistributedTrainingConfig()
        self.assertEqual(config.enabled_for_node_names, [])
        self.assertEqual(config.enabled_for_tags, [])
        
        # Test primary pool defaults
        primary_pool = config.primary_pool
        self.assertIsNotNone(primary_pool)
        if primary_pool:
            self.assertEqual(primary_pool.replica_count, 1)
        
        # Test worker pool defaults
        worker_pool = config.worker_pool
        self.assertIsNotNone(worker_pool)
        if worker_pool:
            self.assertEqual(worker_pool.replica_count, 2)
            
        self.assertIsNone(config.base_output_directory)
        self.assertIsNone(config.service_account)

    def test_distributed_training_config_custom_values(self):
        """Test DistributedTrainingConfig with custom values"""
        config = DistributedTrainingConfig(
            enabled_for_node_names=["train_model", "train_embedding"],
            enabled_for_tags=["distributed", "gpu"],
            primary_pool=WorkerPoolConfig(
                machine_type="n1-standard-8", accelerator_type="NVIDIA_TESLA_T4"
            ),
            worker_pool=WorkerPoolConfig(
                machine_type="n1-standard-8", replica_count=4
            ),
            base_output_directory="gs://my-bucket/output/",
            service_account="distributed-training@my-project.iam.gserviceaccount.com",
        )
        self.assertEqual(config.enabled_for_node_names, ["train_model", "train_embedding"])
        self.assertEqual(config.enabled_for_tags, ["distributed", "gpu"])
        
        # Test primary pool configuration
        primary_pool = config.primary_pool
        if primary_pool:
            self.assertEqual(primary_pool.machine_type, "n1-standard-8")
        
        # Test worker pool configuration
        worker_pool = config.worker_pool
        if worker_pool:
            self.assertEqual(worker_pool.replica_count, 4)
            
        self.assertEqual(config.base_output_directory, "gs://my-bucket/output/")
        self.assertEqual(config.service_account, "distributed-training@my-project.iam.gserviceaccount.com")

    def test_should_use_distributed_training_node_names(self):
        """Test should_use_distributed_training with node names"""
        config_yaml = """
project_id: test-project
region: test-region
run_config:
  image: test-image
  experiment_name: test-experiment
  distributed_training:
    enabled_for_node_names:
      - train_model
      - train_embedding
"""
        config = PluginConfig.model_validate(yaml.safe_load(config_yaml))
        
        # Test node names that should use distributed training
        self.assertTrue(config.run_config.should_use_distributed_training("train_model"))
        self.assertTrue(config.run_config.should_use_distributed_training("train_embedding"))
        
        # Test node names that should not use distributed training
        self.assertFalse(config.run_config.should_use_distributed_training("preprocess"))
        self.assertFalse(config.run_config.should_use_distributed_training("evaluate"))

    def test_should_use_distributed_training_tags(self):
        """Test should_use_distributed_training with tags"""
        config_yaml = """
project_id: test-project
region: test-region
run_config:
  image: test-image
  experiment_name: test-experiment
  distributed_training:
    enabled_for_tags:
      - distributed
      - gpu-intensive
"""
        config = PluginConfig.model_validate(yaml.safe_load(config_yaml))
        
        # Test tags that should use distributed training
        self.assertTrue(config.run_config.should_use_distributed_training("any_node", {"distributed"}))
        self.assertTrue(config.run_config.should_use_distributed_training("any_node", {"gpu-intensive"}))
        self.assertTrue(config.run_config.should_use_distributed_training("any_node", {"distributed", "other"}))
        
        # Test tags that should not use distributed training
        self.assertFalse(config.run_config.should_use_distributed_training("any_node", {"standard"}))
        self.assertFalse(config.run_config.should_use_distributed_training("any_node", {"cpu-only"}))
        self.assertFalse(config.run_config.should_use_distributed_training("any_node", set()))

    def test_should_use_distributed_training_mixed(self):
        """Test should_use_distributed_training with both node names and tags"""
        config_yaml = """
project_id: test-project
region: test-region
run_config:
  image: test-image
  experiment_name: test-experiment
  distributed_training:
    enabled_for_node_names:
      - train_model
    enabled_for_tags:
      - distributed
"""
        config = PluginConfig.model_validate(yaml.safe_load(config_yaml))
        
        # Test node name match
        self.assertTrue(config.run_config.should_use_distributed_training("train_model"))
        
        # Test tag match
        self.assertTrue(config.run_config.should_use_distributed_training("other_node", {"distributed"}))
        
        # Test no match
        self.assertFalse(config.run_config.should_use_distributed_training("other_node", {"standard"}))

    def test_should_use_distributed_training_disabled(self):
        """Test should_use_distributed_training when distributed training is not configured"""
        config_yaml = """
project_id: test-project
region: test-region
run_config:
  image: test-image
  experiment_name: test-experiment
"""
        config = PluginConfig.model_validate(yaml.safe_load(config_yaml))
        
        # Should return False when distributed training is not configured
        self.assertFalse(config.run_config.should_use_distributed_training("any_node"))
        self.assertFalse(config.run_config.should_use_distributed_training("any_node", {"any_tag"}))

    def test_plugin_config_with_distributed_training(self):
        """Test PluginConfig with distributed training configuration"""
        config_yaml = """
project_id: test-project
region: test-region
run_config:
  image: test-image
  experiment_name: test-experiment
  distributed_training:
    enabled_for_node_names:
      - train_model
    enabled_for_tags:
      - distributed
    primary_pool:
      machine_type: n1-standard-8
      replica_count: 1
      accelerator_type: NVIDIA_TESLA_T4
      accelerator_count: 1
    worker_pool:
      machine_type: n1-standard-8
      replica_count: 4
      accelerator_type: NVIDIA_TESLA_T4
      accelerator_count: 1
    base_output_directory: gs://my-bucket/output/
"""
        config = PluginConfig.model_validate(yaml.safe_load(config_yaml))
        
        dt_config = config.run_config.distributed_training
        self.assertIsNotNone(dt_config)
        
        if dt_config:
            self.assertEqual(dt_config.enabled_for_node_names, ["train_model"])
            self.assertEqual(dt_config.enabled_for_tags, ["distributed"])
            
            primary_pool = dt_config.primary_pool
            if primary_pool:
                self.assertEqual(primary_pool.machine_type, "n1-standard-8")
                self.assertEqual(primary_pool.accelerator_type, "NVIDIA_TESLA_T4")
            
            worker_pool = dt_config.worker_pool
            if worker_pool:
                self.assertEqual(worker_pool.replica_count, 4)
                
            self.assertEqual(dt_config.base_output_directory, "gs://my-bucket/output/")


class TestDistributedTrainingGenerator(unittest.TestCase):
    """Test distributed training pipeline generation"""

    def create_pipeline(self):
        """Create a test pipeline with distributed and standard nodes"""
        return Pipeline(
            [
                node(
                    dummy_preprocess_func,
                    "raw_data",
                    "preprocessed_data",
                    name="preprocess",
                    tags=["preprocessing"],
                ),
                node(
                    dummy_train_func,
                    "preprocessed_data",
                    "model",
                    name="train_model",
                    tags=["training", "distributed"],
                ),
                node(
                    dummy_train_func,
                    "preprocessed_data",
                    "embeddings",
                    name="train_embedding",
                    tags=["training"],
                ),
            ]
        )

    def create_generator(self, config={}, params={}, catalog={}):
        """Create a PipelineGenerator for testing"""
        project_name = "test-distributed-training"
        config_loader = MagicMock()
        config_loader.get.return_value = catalog
        context = type(
            "obj",
            (object,),
            {
                "env": "unittests",
                "params": params,
                "config_loader": config_loader,
            },
        )

        self.pipelines_under_test = {"pipeline": self.create_pipeline()}

        config_with_defaults = {
            "image": "test-image",
            "root": "test-bucket/test-suffix",
            "experiment_name": "test-experiment",
            "run_name": "test-run",
        }
        config_with_defaults.update(config)
        
        self.generator_under_test = PipelineGenerator(
            PluginConfig.model_validate(
                {
                    "project_id": "test-project",
                    "region": "test-region",
                    "run_config": config_with_defaults,
                }
            ),
            project_name,
            context,
            "test-run-name",
        )

    def test_should_generate_standard_container_when_distributed_training_disabled(self):
        """Test that standard container components are generated when distributed training is disabled"""
        self.create_generator()
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            with NamedTemporaryFile(mode="rt", prefix="pipeline", suffix=".yaml") as spec_output:
                Compiler().compile(pipeline, spec_output.name)
                with open(spec_output.name) as f:
                    pipeline_spec = yaml.safe_load(f)
                
                # All nodes should be standard container components
                executors = pipeline_spec["deploymentSpec"]["executors"]
                self.assertIn("exec-preprocess", executors)
                self.assertIn("exec-train-model", executors)
                self.assertIn("exec-train-embedding", executors)
                
                # Check that they are container components (not CustomTrainingJobOp)
                for executor_name in ["exec-preprocess", "exec-train-model", "exec-train-embedding"]:
                    self.assertIn("container", executors[executor_name])
                    self.assertIn("args", executors[executor_name]["container"])

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_generate_custom_training_job_when_enabled_by_node_name(self, mock_custom_training_job):
        """Test that CustomTrainingJobOp is generated for nodes enabled by name"""
        config = {
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
                "primary_pool": {
                    "machine_type": "n1-standard-8",
                    "replica_count": 1,
                    "accelerator_type": "NVIDIA_TESLA_T4",
                    "accelerator_count": 1,
                },
                "worker_pool": {
                    "machine_type": "n1-standard-8",
                    "replica_count": 2,
                    "accelerator_type": "NVIDIA_TESLA_T4",
                    "accelerator_count": 1,
                },
                "base_output_directory": "gs://test-bucket/output/",
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            # CustomTrainingJobOp should be called for the distributed training node
            mock_custom_training_job.assert_called()
            
            # Check the call arguments
            call_args = mock_custom_training_job.call_args
            self.assertEqual(call_args[1]["display_name"], "distributed-train-model")
            self.assertEqual(call_args[1]["base_output_directory"], "gs://test-bucket/output/")
            
            # Check worker pool specs
            worker_pool_specs = call_args[1]["worker_pool_specs"]
            self.assertEqual(len(worker_pool_specs), 2)  # Primary + worker pool
            
            # Check primary pool
            primary_pool = worker_pool_specs[0]
            self.assertEqual(primary_pool["replica_count"], 1)
            self.assertEqual(primary_pool["machine_spec"]["machine_type"], "n1-standard-8")
            self.assertEqual(primary_pool["machine_spec"]["accelerator_type"], "NVIDIA_TESLA_T4")
            self.assertEqual(primary_pool["machine_spec"]["accelerator_count"], 1)
            
            # Check worker pool
            worker_pool = worker_pool_specs[1]
            self.assertEqual(worker_pool["replica_count"], 2)
            self.assertEqual(worker_pool["machine_spec"]["machine_type"], "n1-standard-8")
            self.assertEqual(worker_pool["machine_spec"]["accelerator_type"], "NVIDIA_TESLA_T4")
            self.assertEqual(worker_pool["machine_spec"]["accelerator_count"], 1)

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_generate_custom_training_job_when_enabled_by_tag(self, mock_custom_training_job):
        """Test that CustomTrainingJobOp is generated for nodes enabled by tag"""
        config = {
            "distributed_training": {
                "enabled_for_tags": ["distributed"],
                "primary_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 1,
                },
                "worker_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 3,
                },
                "base_output_directory": "gs://test-bucket/output/",
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            # CustomTrainingJobOp should be called for the node with "distributed" tag
            mock_custom_training_job.assert_called()
            
            # Check that the correct node is using distributed training
            call_args = mock_custom_training_job.call_args
            self.assertEqual(call_args[1]["display_name"], "distributed-train-model")

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_generate_mixed_components_when_partially_enabled(self, mock_custom_training_job):
        """Test that both standard and distributed components are generated when partially enabled"""
        config = {
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
                "primary_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 1,
                },
                "worker_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 2,
                },
                "base_output_directory": "gs://test-bucket/output/",
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            with NamedTemporaryFile(mode="rt", prefix="pipeline", suffix=".yaml") as spec_output:
                 Compiler().compile(pipeline, spec_output.name)
                 with open(spec_output.name) as f:
                     pipeline_spec = yaml.safe_load(f)
                 
                 # Check that some nodes are standard containers
                 executors = pipeline_spec["deploymentSpec"]["executors"]
                 self.assertIn("exec-preprocess", executors)
                 self.assertIn("exec-train-embedding", executors)
                 
                 # These should be standard container components
                 self.assertIn("container", executors["exec-preprocess"])
                 self.assertIn("container", executors["exec-train-embedding"])
            
            # CustomTrainingJobOp should be called once for train_model
            mock_custom_training_job.assert_called_once()

    def test_should_use_default_output_directory_when_not_specified(self):
        """Test that default output directory is used when not specified"""
        config = {
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro_vertexai.generator.CustomTrainingJobOp") as mock_custom_training_job:
            with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
                pipeline = self.generator_under_test.generate_pipeline(
                    "pipeline", "test-image", "test-token"
                )
                
                # Check that default output directory is used
                call_args = mock_custom_training_job.call_args
                expected_output_dir = "gs://test-bucket/test-suffix/distributed-training-output/"
                self.assertEqual(call_args[1]["base_output_directory"], expected_output_dir)

    def test_should_handle_worker_pool_with_zero_replicas(self):
        """Test that worker pool is omitted when replica count is 0"""
        config = {
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
                "primary_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 1,
                },
                "worker_pool": {
                    "machine_type": "n1-standard-4",
                    "replica_count": 0,  # No worker replicas
                },
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro_vertexai.generator.CustomTrainingJobOp") as mock_custom_training_job:
            with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
                pipeline = self.generator_under_test.generate_pipeline(
                    "pipeline", "test-image", "test-token"
                )
                
                # Check that only primary pool is included
                call_args = mock_custom_training_job.call_args
                worker_pool_specs = call_args[1]["worker_pool_specs"]
                self.assertEqual(len(worker_pool_specs), 1)  # Only primary pool
                self.assertEqual(worker_pool_specs[0]["replica_count"], 1)

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_use_service_account_from_distributed_training_config(self, mock_custom_training_job):
        """Test that service account is passed from distributed training config"""
        config = {
            "service_account": "global@my-project.iam.gserviceaccount.com",
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
                "service_account": "distributed@my-project.iam.gserviceaccount.com",
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            # Verify CustomTrainingJobOp was called with distributed training service account
            mock_custom_training_job.assert_called_once()
            call_args = mock_custom_training_job.call_args
            self.assertEqual(call_args[1]["service_account"], "distributed@my-project.iam.gserviceaccount.com")

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_fallback_to_global_service_account_when_not_specified(self, mock_custom_training_job):
        """Test that service account falls back to global when not specified in distributed training config"""
        config = {
            "service_account": "global@my-project.iam.gserviceaccount.com",
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            # Verify CustomTrainingJobOp was called with global service account
            mock_custom_training_job.assert_called_once()
            call_args = mock_custom_training_job.call_args
            self.assertEqual(call_args[1]["service_account"], "global@my-project.iam.gserviceaccount.com")

    @patch("kedro_vertexai.generator.CustomTrainingJobOp")
    def test_should_use_empty_string_when_no_service_account_specified(self, mock_custom_training_job):
        """Test that empty string is used when no service account is specified"""
        config = {
            "distributed_training": {
                "enabled_for_node_names": ["train_model"],
            }
        }
        
        self.create_generator(config=config)
        
        with patch("kedro.framework.project.pipelines", new=self.pipelines_under_test):
            pipeline = self.generator_under_test.generate_pipeline(
                "pipeline", "test-image", "test-token"
            )
            
            # Verify CustomTrainingJobOp was called with empty string
            mock_custom_training_job.assert_called_once()
            call_args = mock_custom_training_job.call_args
            self.assertEqual(call_args[1]["service_account"], "")


if __name__ == "__main__":
    unittest.main() 