import os
from distutils.util import strtobool

from kedro_vertexai.constants import KEDRO_VERTEXAI_DISABLE_CONFIG_HOOK

CONFIG_HOOK_DISABLED = strtobool(
    os.getenv(KEDRO_VERTEXAI_DISABLE_CONFIG_HOOK, "false")
)
