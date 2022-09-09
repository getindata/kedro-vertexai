import logging

from kedro.framework.context import KedroContext
from kedro.framework.hooks import hook_impl

from kedro_vertexai.auth.mlflow_request_header_provider import (
    DynamicMLFlowRequestHeaderProvider,
    RequestHeaderProviderWithKedroContext,
)


class MLFlowRequestHeaderProviderHook:
    """
    Hook allowing to plug-in custom MLFlow request header provider
    Usage:
    (in settings.py of your project)
    HOOKS = (MLFlowRequestHeaderProviderHook(implementation class), )
    """

    def __init__(self, provider_class: type):
        assert issubclass(
            provider_class, RequestHeaderProviderWithKedroContext
        ), f"Provider class needs to be a subclass of {RequestHeaderProviderWithKedroContext.__qualname__}"
        self.logger = logging.getLogger(self.__class__.__name__)
        self.provider_class = provider_class

    @hook_impl
    def after_context_created(
        self,
        context: KedroContext,
    ):
        DynamicMLFlowRequestHeaderProvider().configure(self.provider_class(context))
