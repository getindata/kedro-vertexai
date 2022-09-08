import os
import unittest
from io import StringIO
from unittest.mock import MagicMock, patch
from uuid import uuid4

import responses
import yaml
from google.auth.exceptions import DefaultCredentialsError
from kedro.framework.context import KedroContext

from kedro_vertexai.auth.gcp import (
    AuthHandler,
    MLFlowGoogleIAMRequestHeaderProvider,
)
from kedro_vertexai.auth.mlflow_request_header_provider import (
    DynamicMLFlowRequestHeaderProvider,
    RequestHeaderProviderWithKedroContext,
    safe_import_mlflow,
)
from kedro_vertexai.auth.mlflow_request_header_provider_hook import (
    MLFlowRequestHeaderProviderHook,
)
from tests.test_config import CONFIG_FULL


class TestAuthHandler(unittest.TestCase):
    @patch("google.oauth2.id_token.fetch_id_token")
    def test_should_error_on_invalid_creds(self, fetch_id_token_mock):
        # given
        fetch_id_token_mock.side_effect = Exception()

        with self.assertLogs("kedro_vertexai.auth", level="ERROR") as cm:
            # when

            token = AuthHandler().obtain_id_token("unittest-client-id")

            # then
            assert "Failed to obtain IAP access token" in cm.output[0]

        # then
        assert token is None

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_should_warn_if_trying_to_use_default_creds(self, fetch_id_token_mock):
        # given
        fetch_id_token_mock.side_effect = DefaultCredentialsError()

        with self.assertLogs("kedro_vertexai.auth", level="WARNING") as cm:
            # when
            token = AuthHandler().obtain_id_token("unittest-client-id")

            # then
            assert (
                "this authentication method does not work with default credentials"
                in cm.output[0]
            )
            assert token is None

    @patch("google.oauth2.id_token.fetch_id_token")
    def test_should_provide_valid_token(self, fetch_id_token_mock):
        # given
        fetch_id_token_mock.return_value = "TOKEN"

        # when
        token = AuthHandler().obtain_id_token("unittest-client-id")

        # then
        assert token == "TOKEN"

    def test_empty_client_id(self):
        # for better test coverage

        # when
        token = AuthHandler().obtain_id_token("")

        # then
        assert token is None

    def test_should_skip_dex_auth_if_env_is_not_set(self):
        # given
        # no env set

        # when
        session = AuthHandler().obtain_dex_authservice_session(None)

        # then
        assert session is None

    def test_should_skip_dex_auth_if_env_is_incomplete(self):
        # given
        os.environ["DEX_USERNAME"] = "user@example.com"
        # no password set

        # when
        session = AuthHandler().obtain_dex_authservice_session(None)

        # then
        assert session is None

    def tearDown(self):
        if "DEX_USERNAME" in os.environ:
            del os.environ["DEX_USERNAME"]
        if "DEX_PASSWORD" in os.environ:
            del os.environ["DEX_PASSWORD"]
        if "IAP_CLIENT_ID" in os.environ:
            del os.environ["IAP_CLIENT_ID"]

    @responses.activate
    def test_should_get_cookie_from_dex_secured_system(self):
        # given
        os.environ["DEX_USERNAME"] = "user@example.com"
        os.environ["DEX_PASSWORD"] = "pa$$"
        responses.add(
            responses.GET,
            "https://kubeflow.local/pipeline",
            body='<a href="/dex/auth/local?req=qjrrnpg3hngdu6odii3hcmfae" target="_self"',
        )
        responses.add(
            responses.POST,
            "https://kubeflow.local/dex/auth/local?req=qjrrnpg3hngdu6odii3hcmfae",
            headers={"Set-cookie": "authservice_session=sessionID"},
        )

        # when
        session = AuthHandler().obtain_dex_authservice_session(
            "https://kubeflow.local/pipeline"
        )

        # then
        assert session == "sessionID"
        assert (
            responses.calls[1].request.body
            == "login=user%40example.com&password=pa%24%24"
        )

    @patch("google.cloud.iam_credentials.IAMCredentialsClient")
    def test_can_obtain_iam_token(self, iam: MagicMock):
        mock_token = uuid4().hex
        return_token = MagicMock()
        return_token.token = mock_token
        iam.return_value.generate_id_token.return_value = return_token
        token = AuthHandler().obtain_iam_token("test@example.com", "client_id")
        iam.return_value.generate_id_token.assert_called_once()
        assert token == mock_token

    def test_mlflow_header_provider_is_singleton(self):
        provider = DynamicMLFlowRequestHeaderProvider()
        others = [DynamicMLFlowRequestHeaderProvider() for _ in range(100)]
        assert all(provider == o for o in others)

    def test_mlflow_header_provider_setup(self):
        for in_ctx in (True, False):
            with self.subTest(msg=f"Enabled={in_ctx}"):
                custom_header = {"Custom": "Header"}
                dummy_provider = MagicMock(spec=RequestHeaderProviderWithKedroContext)
                dummy_provider.in_context = MagicMock(return_value=in_ctx)
                dummy_provider.request_headers = MagicMock(return_value=custom_header)

                provider = DynamicMLFlowRequestHeaderProvider()

                # in_context() on uninitialized should always be false
                assert not provider.in_context()

                # request_headers() on uninitialized should be an empty dict
                self.assertDictEqual(provider.request_headers(), {})

                provider.configure(dummy_provider)
                assert provider.in_context() == in_ctx, "Value didn't passed through"
                dummy_provider.in_context.assert_called_once()
                if in_ctx:
                    self.assertDictEqual(provider.request_headers(), custom_header)

                # Testing re-configure for coverage
                provider.configure(dummy_provider)

                del DynamicMLFlowRequestHeaderProvider.__instance__

    def test_import_with_missing_mlflow(self):
        def mock_import(name, *args):
            if name == "mlflow":
                raise ImportError("no mlfow!")
            else:
                return __import__(name, *args)

        with patch("builtins.__import__", side_effect=mock_import):
            mlflow, rh_class = safe_import_mlflow()
            assert mlflow is None and rh_class is object

    @patch("kedro_vertexai.auth.gcp.AuthHandler.obtain_iam_token")
    @patch(
        "kedro_vertexai.auth.mlflow_request_header_provider.EnvTemplatedConfigLoader",
    )
    def test_mlflow_header_provider_methods(self, cfg_loader, obtain_iam_token):
        config = yaml.safe_load(StringIO(CONFIG_FULL))
        cfg_loader.return_value.get = MagicMock(return_value=config)
        token = uuid4().hex
        obtain_iam_token.return_value = token
        kedro_context = MagicMock(spec=KedroContext)
        provider = MLFlowGoogleIAMRequestHeaderProvider(kedro_context)
        provider.get_token = obtain_iam_token
        assert provider.in_context()
        headers = provider.request_headers()
        self.assertDictEqual(headers, {"Authorization": f"Bearer {token}"})

    def test_request_header_provider_hook(self):
        provider = MagicMock(spec=RequestHeaderProviderWithKedroContext)
        kedro_context = MagicMock(spec=KedroContext)
        with patch("builtins.issubclass", return_value=True):
            hook = MLFlowRequestHeaderProviderHook(provider)
        hook.after_context_created(kedro_context)
        provider.assert_called_once_with(kedro_context)
