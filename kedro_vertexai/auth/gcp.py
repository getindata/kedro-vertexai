"""
GCP related authorization code
"""
import logging
import os
import re
from urllib.parse import urlsplit, urlunsplit

import requests
from cachetools import TTLCache, cached

from kedro_vertexai.auth.mlflow_request_header_provider import (
    RequestHeaderProviderWithKedroContext,
)
from kedro_vertexai.config import PluginConfig
from kedro_vertexai.dynamic_config import DynamicConfigProvider

IAP_CLIENT_ID = "IAP_CLIENT_ID"
DEX_USERNAME = "DEX_USERNAME"
DEX_PASSWORD = "DEX_PASSWORD"


class AuthHandler:
    """
    Utils for handling authorization
    """

    log = logging.getLogger(__name__)

    def obtain_id_token(self, client_id: str):
        """
        Obtain OAuth2.0 token to be used with HTTPs requests
        """
        # pylint: disable=import-outside-toplevel
        from google.auth.exceptions import DefaultCredentialsError
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token

        # pylint enable=import-outside-toplevel

        jwt_token = None

        if not client_id:
            self.log.debug(
                "No IAP_CLIENT_ID provided, skipping custom IAP authentication"
            )
            return jwt_token

        try:
            self.log.debug("Attempt to get IAP token for %s", client_id)
            jwt_token = id_token.fetch_id_token(Request(), client_id)
            self.log.info("Obtained JWT token for IAP proxy authentication.")
        except DefaultCredentialsError:
            self.log.warning(
                (
                    " Note that this authentication method does not work with default"
                    " credentials obtained via 'gcloud auth application-default login'"
                    " command. Refer to documentation on how to configure service account"
                    " locally"
                    " (https://cloud.google.com/docs/authentication/production#manually)"
                ),
                exc_info=True,
            )
        except Exception:  # pylint: disable=broad-except
            self.log.error("Failed to obtain IAP access token.", exc_info=True)

        return jwt_token

    def obtain_dex_authservice_session(self, kfp_api):
        """
        Obtain token for DEX-protected service
        """
        if DEX_USERNAME not in os.environ or DEX_PASSWORD not in os.environ:
            self.log.debug("Skipping DEX authentication due to missing env variables")
            return None

        session = requests.Session()
        response = session.get(kfp_api)
        form_relative_url = re.search(
            '/dex/auth/local\\?req=([^"]*)', response.text
        ).group(0)

        kfp_url_parts = urlsplit(kfp_api)
        form_absolute_url = urlunsplit(
            [
                kfp_url_parts.scheme,
                kfp_url_parts.netloc,
                form_relative_url,
                None,
                None,
            ]
        )

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "login": os.environ[DEX_USERNAME],
            "password": os.environ[DEX_PASSWORD],
        }

        session.post(form_absolute_url, headers=headers, data=data)
        return session.cookies.get_dict()["authservice_session"]

    def obtain_iam_token(self, service_account, client_id):
        from google.cloud import iam_credentials

        self.log.debug(f"Attempt to get IAM token for {service_account}")
        client = iam_credentials.IAMCredentialsClient()
        return client.generate_id_token(
            name=f"projects/-/serviceAccounts/{service_account}",
            audience=client_id,
            include_email=True,
        ).token


class MLFlowGoogleOAuthCredentialsProvider(DynamicConfigProvider):
    """
    Uses Google OAuth to generate MLFLOW_TRACKING_TOKEN
    """

    def __init__(self, config: PluginConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.client_id = kwargs["client_id"]

    @property
    def target_config_file(self) -> str:
        return "credentials.yml"

    def generate_config(self) -> dict:
        return {
            "gcp_credentials": {
                "MLFLOW_TRACKING_TOKEN": AuthHandler().obtain_id_token(self.client_id)
            }
        }


class MLFlowGoogleIAMCredentialsProvider(DynamicConfigProvider):
    """
    Uses Google IAM API to generate MLFLOW_TRACKING_TOKEN
    """

    def __init__(self, config: PluginConfig, *args, **kwargs):
        super().__init__(config, *args, **kwargs)
        self.client_id = kwargs["client_id"]
        self.service_account = kwargs["service_account"]

    @property
    def target_config_file(self) -> str:
        return "credentials.yml"

    def generate_config(self) -> dict:
        return {
            "gcp_credentials": {
                "MLFLOW_TRACKING_TOKEN": AuthHandler().obtain_iam_token(
                    self.service_account, self.client_id
                )
            }
        }


class MLFlowGoogleIAMRequestHeaderProvider(RequestHeaderProviderWithKedroContext):
    required_params = ("client_id", "service_account")
    get_token = AuthHandler().obtain_iam_token

    def in_context(self):
        return self.params and all(p in self.params for p in self.required_params)

    @cached(TTLCache(1, ttl=59 * 60))
    def request_headers(self):
        get_token_kwargs = {
            k: v for k, v in self.params.items() if k in self.required_params
        }
        token = self.get_token(**get_token_kwargs)
        return {"Authorization": f"Bearer {token}"}


class MLFlowGoogleOauthRequestHeaderProvider(MLFlowGoogleIAMRequestHeaderProvider):
    required_params = ("client_id",)
    get_token = AuthHandler().obtain_id_token
