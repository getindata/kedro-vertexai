"""
GCP related authorization code
"""
import logging
import os
import re
from urllib.parse import urlsplit, urlunsplit

import requests
from google.auth.exceptions import TransportError
from retry.api import retry_call

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
        HEADERS = {"Metadata-Flavor": "Google"}
        try:
            r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
                         params={
                             "audience": "887254626752-8l79mcpv0cfmtukh9klei9cgn7q9dmp7.apps.googleusercontent.com",
                             "format":"full"
                         },
                         headers=HEADERS)
            self.log.info(r.text)
        except Exception:
            self.log.error("Call failed", exc_info=True)

        try:
            r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/willa-vertex-pipelines@willapay-mlops-staging.iam.gserviceaccount.com/identity",
                         params={
                             "audience": "887254626752-8l79mcpv0cfmtukh9klei9cgn7q9dmp7.apps.googleusercontent.com",
                             "format": "full"
                         },
                         headers=HEADERS)
            self.log.info(r.text)
        except Exception:
            self.log.error("Call failed", exc_info=True)

        try:
            r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/willa-vertex-pipelines@willapay-mlops-staging.iam.gserviceaccount.com/identity",
                             params={
                                 "audience": "887254626752-8l79mcpv0cfmtukh9klei9cgn7q9dmp7.apps.googleusercontent.com",
                                 "format": "full",
                                 "recursive": True
                             },
                             headers=HEADERS)
            self.log.info(r.text)
        except Exception:
            self.log.error("Call failed", exc_info=True)


        try:
            r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts",
                             params={
                                 "audience": "887254626752-8l79mcpv0cfmtukh9klei9cgn7q9dmp7.apps.googleusercontent.com",
                                 "format": "full"
                             },
                             headers=HEADERS)
            self.log.info(r.text)
        except Exception:
            self.log.error("Call failed", exc_info=True)


        try:
            self.log.debug("Attempt to get IAP token for %s", client_id)
            requests.get("http:/")
            jwt_token = retry_call(id_token.fetch_id_token, fargs=[Request(), client_id], tries=5, delay=5, jitter=5,
                                   exceptions=TransportError)
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
            self.log.debug(
                "Skipping DEX authentication due to missing env variables"
            )
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
                "MLFLOW_TRACKING_TOKEN": AuthHandler().obtain_id_token(
                    self.client_id
                )
            }
        }
