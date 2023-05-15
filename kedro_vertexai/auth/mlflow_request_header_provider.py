import logging
from abc import ABC
from typing import Any, Dict, Optional

from kedro.framework.context import KedroContext

logger = logging.getLogger(__name__)


def safe_import_mlflow():
    try:
        import mlflow
        from mlflow.tracking.request_header.abstract_request_header_provider import (
            RequestHeaderProvider,
        )
    except ImportError:
        mlflow = None
        RequestHeaderProvider = object
    return mlflow, RequestHeaderProvider


mlflow, RequestHeaderProvider = safe_import_mlflow()


class RequestHeaderProviderWithKedroContext(RequestHeaderProvider, ABC):
    def __init__(self, kedro_context: KedroContext, **kwargs):
        self.kedro_context = kedro_context
        self.params: Optional[Dict[str, Any]] = kwargs


class DynamicMLFlowRequestHeaderProvider(RequestHeaderProvider):
    __instance__ = None
    provider: RequestHeaderProvider = None

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "__instance__") or not isinstance(cls.__instance__, cls):
            cls.__instance__ = object.__new__(cls, *args, **kwargs)
        return cls.__instance__

    def configure(self, provider: RequestHeaderProvider):
        if (self.provider is None) or (type(self.provider) is not type(provider)):
            logger.info(f"Configured MLflow request header provider to use {provider}")
            self.provider = provider
        else:
            logger.info("MLflow request header provider was already initialized")

    def in_context(self):
        if self.provider is not None:
            ctx = self.provider.in_context()
        else:
            ctx = False
        return ctx

    def request_headers(self):
        if self.provider is not None:
            x = self.provider.request_headers()
            return x
        else:
            return {}
