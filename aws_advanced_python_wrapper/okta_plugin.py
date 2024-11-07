#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from datetime import datetime, timedelta
from html import unescape
from re import search
from typing import TYPE_CHECKING, Callable, Dict, Optional, Set

from aws_advanced_python_wrapper.credentials_provider_factory import (
    CredentialsProviderFactory, SamlCredentialsProviderFactory)
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils, TokenInfo
from aws_advanced_python_wrapper.utils.region_utils import RegionUtils
from aws_advanced_python_wrapper.utils.saml_utils import SamlUtils

if TYPE_CHECKING:
    from boto3 import Session
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.plugin_service import PluginService

import requests

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


class OktaAuthPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}

    _rds_utils: RdsUtils = RdsUtils()
    _token_cache: Dict[str, TokenInfo] = {}

    def __init__(self, plugin_service: PluginService, credentials_provider_factory: CredentialsProviderFactory, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._credentials_provider_factory = credentials_provider_factory
        self._session = session

        self._region_utils = RegionUtils()
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = telemetry_factory.create_counter("okta.fetch_token.count")
        self._cache_size_gauge = telemetry_factory.create_gauge("okta.token_cache.size", lambda: len(OktaAuthPlugin._token_cache))

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(host_info, props, connect_func)

    def _connect(self, host_info: HostInfo, props: Properties, connect_func: Callable) -> Connection:
        SamlUtils.check_idp_credentials_with_fallback(props)

        host = IamAuthUtils.get_iam_host(props, host_info)
        port = IamAuthUtils.get_port(props, host_info, self._plugin_service.database_dialect.default_port)
        region = self._region_utils.get_region(props, WrapperProperties.IAM_REGION.name, host, self._session)
        if not region:
            error_message = "RdsUtils.UnsupportedHostname"
            logger.debug(error_message, host)
            raise AwsWrapperError(Messages.get_formatted(error_message, host))

        user = WrapperProperties.DB_USER.get(props)
        cache_key: str = IamAuthUtils.get_cache_key(
            user,
            host,
            port,
            region
        )

        token_info = OktaAuthPlugin._token_cache.get(cache_key)

        if token_info is not None and not token_info.is_expired():
            logger.debug("OktaAuthPlugin.UseCachedToken", token_info.token)
            self._plugin_service.driver_dialect.set_password(props, token_info.token)
        else:
            self._update_authentication_token(host_info, props, user, region, cache_key)

        WrapperProperties.USER.set(props, WrapperProperties.DB_USER.get(props))

        try:
            return connect_func()
        except Exception:
            self._update_authentication_token(host_info, props, user, region, cache_key)

            try:
                return connect_func()
            except Exception as e:
                error_message = "OktaAuthPlugin.UnhandledException"
                logger.debug(error_message, e)
                raise AwsWrapperError(Messages.get_formatted(error_message, e)) from e

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(host_info, props, force_connect_func)

    def _update_authentication_token(self,
                                     host_info: HostInfo,
                                     props: Properties,
                                     user: Optional[str],
                                     region: str,
                                     cache_key: str) -> None:
        token_expiration_sec: int = WrapperProperties.IAM_TOKEN_EXPIRATION.get_int(props)
        token_expiry: datetime = datetime.now() + timedelta(seconds=token_expiration_sec)
        port: int = IamAuthUtils.get_port(props, host_info, self._plugin_service.database_dialect.default_port)
        credentials: Optional[Dict[str, str]] = self._credentials_provider_factory.get_aws_credentials(region, props)

        token: str = IamAuthUtils.generate_authentication_token(
            self._plugin_service,
            user,
            host_info.host,
            port,
            region,
            credentials,
            self._session
        )
        WrapperProperties.PASSWORD.set(props, token)
        OktaAuthPlugin._token_cache[cache_key] = TokenInfo(token, token_expiry)


class OktaCredentialsProviderFactory(SamlCredentialsProviderFactory):
    _SAML_RESPONSE_PATTERN = r"\"SAMLResponse\" .* value=\"(?P<saml>[^\"]+)\""
    _SAML_RESPONSE_PATTERN_GROUP = "saml"
    _HTTPS_URL_PATTERN = r"^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']"
    _OKTA_AWS_APP_NAME = "amazon_aws"
    _ONE_TIME_TOKEN = "onetimetoken"
    _SESSION_TOKEN = "sessionToken"

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props

    def _get_session_token(self, props: Properties) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_user = WrapperProperties.IDP_USERNAME.get(props)
        idp_password = WrapperProperties.IDP_PASSWORD.get(props)

        session_token_endpoint = f"https://{idp_endpoint}/api/v1/authn"

        request_body = {
            "username": idp_user,
            "password": idp_password,
        }
        try:
            r = requests.post(session_token_endpoint,
                              headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
                              json=request_body,
                              verify=WrapperProperties.SSL_SECURE.get_bool(props),
                              timeout=WrapperProperties.HTTP_REQUEST_TIMEOUT.get_int(props))
            if r.status_code / 100 != 2:
                error_message = "SamlUtils.RequestFailed"
                logger.debug(error_message, r.status_code, r.reason, r.text)
                raise AwsWrapperError(Messages.get_formatted(error_message, r.status_code, r.reason, r.text))
            return r.json().get(OktaCredentialsProviderFactory._SESSION_TOKEN)
        except IOError as e:
            error_message = "OktaAuthPlugin.UnhandledException"
            logger.debug(error_message, e)
            raise AwsWrapperError(Messages.get_formatted(error_message, e))

    def _get_saml_url(self, props: Properties) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        app_id = WrapperProperties.APP_ID.get(props)
        return f"https://{idp_endpoint}/app/{OktaCredentialsProviderFactory._OKTA_AWS_APP_NAME}/{app_id}/sso/saml"

    def get_saml_assertion(self, props: Properties):
        try:
            one_time_token = self._get_session_token(props)
            uri = self._get_saml_url(props)
            SamlUtils.validate_url(uri)

            logger.debug("OktaCredentialsProviderFactory.SamlAssertionUrl", uri)
            r = requests.get(uri,
                             params={OktaCredentialsProviderFactory._ONE_TIME_TOKEN: one_time_token},
                             verify=WrapperProperties.SSL_SECURE.get_bool(props),
                             timeout=WrapperProperties.HTTP_REQUEST_TIMEOUT.get_int(props))

            SamlUtils.validate_response(r)
            content = r.text
            match = search(OktaCredentialsProviderFactory._SAML_RESPONSE_PATTERN, content)
            if not match:
                error_message = "AdfsCredentialsProviderFactory.FailedLogin"
                logger.debug(error_message, content)
                raise AwsWrapperError(Messages.get_formatted(error_message, content))

            # return SAML Response value
            return unescape(match.group(self._SAML_RESPONSE_PATTERN_GROUP))

        except IOError as e:
            error_message = "OktaAuthPlugin.UnhandledException"
            logger.debug(error_message, e)
            raise AwsWrapperError(Messages.get_formatted(error_message, e))


class OktaAuthPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return OktaAuthPlugin(plugin_service, self.get_credentials_provider_factory(plugin_service, props))

    def get_credentials_provider_factory(self, plugin_service: PluginService, props: Properties) -> OktaCredentialsProviderFactory:
        return OktaCredentialsProviderFactory(plugin_service, props)
