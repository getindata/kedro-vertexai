import logging
import os
import re
from urllib.parse import urlsplit, urlunsplit

import requests

from kedro_vertexai.config import PluginConfig
from kedro_vertexai.dynamic_config import DynamicConfigProvider

IAP_CLIENT_ID = "IAP_CLIENT_ID"
DEX_USERNAME = "DEX_USERNAME"
DEX_PASSWORD = "DEX_PASSWORD"


class AuthHandler(object):

    log = logging.getLogger(__name__)

    @classmethod
    def obtain_id_token(self, client_id: str):
        from google.auth.exceptions import DefaultCredentialsError
        from google.auth.transport.requests import Request
        from google.oauth2 import id_token

        jwt_token = None

        if not client_id:
            self.log.debug(
                "No IAP_CLIENT_ID provided, skipping custom IAP authentication"
            )
            return jwt_token

        try:
            self.log.debug("Attempt to get IAP token for %s." + client_id)
            jwt_token = id_token.fetch_id_token(Request(), client_id)
            self.log.info("Obtained JWT token for IAP proxy authentication.")
        except DefaultCredentialsError as ex:
            self.log.warning(
                str(ex)
                + (
                    " Note that this authentication method does not work with default"
                    " credentials obtained via 'gcloud auth application-default login'"
                    " command. Refer to documentation on how to configure service account"
                    " locally"
                    " (https://cloud.google.com/docs/authentication/production#manually)"
                )
            )
        except Exception as e:
            self.log.error("Failed to obtain IAP access token. " + str(e))
        finally:
            return jwt_token

    def obtain_dex_authservice_session(self, kfp_api):
        if DEX_USERNAME not in os.environ or DEX_PASSWORD not in os.environ:
            self.log.debug(
                "Skipping DEX authentication due to missing env variables"
            )
            return None

        s = requests.Session()
        r = s.get(kfp_api)
        form_relative_url = re.search(
            '/dex/auth/local\\?req=([^"]*)', r.text
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

        s.post(form_absolute_url, headers=headers, data=data)
        return s.cookies.get_dict()["authservice_session"]


class MLFlowGCPCredentialsProvider(DynamicConfigProvider):
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
