import json
import logging
import re
import subprocess
import sys
from pathlib import Path

import yaml
from kedro.framework.project import settings

from kedro_vertexai.dynamic_config import DynamicConfigProvider

logger = logging.getLogger(__name__)


def strip_margin(text: str) -> str:
    return re.sub("\n[ \t]*\\|", "\n", text).strip()


def clean_name(name: str) -> str:
    return re.sub(r"[\W_]+", "-", name).strip("-")


def is_mlflow_enabled() -> bool:
    try:
        import kedro_mlflow  # NOQA
        import mlflow  # NOQA

        return True
    except ImportError:
        return False


def save_yaml(obj: object, target_path: Path):
    with target_path.open("w") as f:
        yaml.safe_dump(obj, f)


def store_parameters_in_yaml(params: str, output: str):
    if params:
        parameters = json.loads(params.strip("'"))
        output_path = Path(output)
        config_data = _load_yaml_or_empty_dict(output_path)

        if "run" not in config_data:
            config_data["run"] = {}
        config_data["run"]["params"] = parameters

        save_yaml(config_data, output_path)
    else:
        logger.debug("No params to serialize")


def materialize_dynamic_configuration(config, context_helper):
    for provider_config in config.run_config.dynamic_config_providers:
        provider = DynamicConfigProvider.build(config, provider_config)

        if provider is None:
            logger.warning(
                f"Provider {provider_config.cls} could not be initialized, see the error messages above"
            )
            continue

        _generate_and_save_dynamic_config(provider, context_helper)


def _load_yaml_or_empty_dict(output_path):
    if output_path.exists():
        with output_path.open("r") as f:
            dict_from_yaml = yaml.safe_load(f)
    else:
        dict_from_yaml = {}
    return dict_from_yaml


def _generate_and_save_dynamic_config(provider: DynamicConfigProvider, context_helper):
    dynamic_config = provider.generate_config()
    target_path = (
        context_helper.context.project_path
        / settings.CONF_SOURCE
        / provider.target_env
        / provider.target_config_file
    )
    existing_config = _load_yaml_or_empty_dict(target_path)
    provider.merge_with_existing(existing_config, dynamic_config)
    logger.info(f"Saving dynamic config {target_path} [{type(provider).__name__}]")
    save_yaml(dynamic_config, target_path)


def docker_build(path: str, image: str) -> int:
    rv = subprocess.run(
        [
            "docker",
            "build",
            path,
            "-t",
            image,
        ],
        stdout=sys.stdout,
        stderr=subprocess.STDOUT,
    ).returncode
    if rv:
        logger.error("Docker build has failed.")
    return rv


def docker_push(image: str) -> int:
    rv = subprocess.run(
        ["docker", "push", image], stdout=sys.stdout, stderr=subprocess.STDOUT
    ).returncode
    if rv:
        logger.error("Docker push has failed.")
    return rv
