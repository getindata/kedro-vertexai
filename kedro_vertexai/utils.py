import json
import logging
import re
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def strip_margin(text: str) -> str:
    return re.sub("\n[ \t]*\\|", "\n", text).strip()


def clean_name(name: str) -> str:
    return re.sub(r"[\W_]+", "-", name).strip("-")


def is_mlflow_enabled() -> bool:
    try:
        import mlflow  # NOQA
        from kedro_mlflow.framework.context import get_mlflow_config  # NOQA

        return True
    except ImportError:
        return False


def store_parameters_in_yaml(params: str, output: str):
    if params:
        parameters = json.loads(params.strip("'"))
        output_path = Path(output)
        config_data = _load_existing_config_or_default(output_path)

        if "run" not in config_data:
            config_data["run"] = {}
        config_data["run"]["params"] = parameters

        with output_path.open("w") as f:
            yaml.dump(config_data, f)
    else:
        logger.debug("No params to serialize")


def _load_existing_config_or_default(output_path):
    if output_path.exists():
        with output_path.open("r") as f:
            config_data = yaml.safe_load(f)
    else:
        config_data = {}
    return config_data
