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

from html import unescape
from re import DOTALL, findall, search
from typing import TYPE_CHECKING, List
from urllib.parse import urlencode

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

from datetime import datetime, timedelta
from typing import Callable, Dict, Optional, Set

import requests

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

logger = Logger(__name__)


class FederatedAuthPlugin(Plugin):
    _HTTPS_URL_PATTERN = r"^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']"
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}

    _rds_utils: RdsUtils = RdsUtils()
    _token_cache: Dict[str, TokenInfo] = {}

    def __init__(self, plugin_service: PluginService, credentials_provider_factory: CredentialsProviderFactory, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._credentials_provider_factory = credentials_provider_factory
        self._session = session

        self._region_utils = RegionUtils()
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        self._fetch_token_counter = telemetry_factory.create_counter("federated.fetch_token.count")
        self._cache_size_gauge = telemetry_factory.create_gauge("federated.token_cache.size", lambda: len(FederatedAuthPlugin._token_cache))

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

        token_info = FederatedAuthPlugin._token_cache.get(cache_key)

        if token_info is not None and not token_info.is_expired():
            logger.debug("FederatedAuthPlugin.UseCachedToken", token_info.token)
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
                error_message = "FederatedAuthPlugin.UnhandledException"
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

        self._fetch_token_counter.inc()
        token: str = IamAuthUtils.generate_authentication_token(
            self._plugin_service,
            user,
            host_info.host,
            port,
            region,
            credentials,
            self._session)
        WrapperProperties.PASSWORD.set(props, token)
        FederatedAuthPlugin._token_cache[cache_key] = TokenInfo(token, token_expiry)


class FederatedAuthPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return FederatedAuthPlugin(plugin_service, self.get_credentials_provider_factory(plugin_service, props))

    def get_credentials_provider_factory(self, plugin_service: PluginService, props: Properties) -> AdfsCredentialsProviderFactory:
        idp_name = WrapperProperties.IDP_NAME.get(props)
        if idp_name is None or idp_name == "" or idp_name == "adfs":
            return AdfsCredentialsProviderFactory(plugin_service, props)
        raise AwsWrapperError(Messages.get_formatted("FederatedAuthPluginFactory.UnsupportedIdp", idp_name))


class AdfsCredentialsProviderFactory(SamlCredentialsProviderFactory):
    _INPUT_TAG_PATTERN = r"<input(.+?)/>"
    _FORM_ACTION_PATTERN = r"<form.*?action=\"([^\"]+)\""
    _SAML_RESPONSE_PATTERN = r"\"SAMLResponse\" value=\"(?P<saml>[^\"]+)\""
    _SAML_RESPONSE_PATTERN_GROUP = "saml"
    _HTTPS_URL_PATTERN = r"^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']"

    def __init__(self, plugin_service: PluginService, props: Properties):
        self._plugin_service = plugin_service
        self._properties = props

    def get_saml_assertion(self, props: Properties):
        try:
            uri = self._get_sign_in_page_url(props)

            sign_in_page_body: str = self._get_sign_in_page_body(uri, props)
            action: str = self._get_form_action_from_html_body(sign_in_page_body)

            if action != "" and action.startswith("/"):
                uri = self._get_form_action_url(props, action)

            params: Dict[str, str] = self._get_parameters_from_html_body(sign_in_page_body, props)
            content: str = self._post_form_action_body(uri, params, props)

            match = search(self._SAML_RESPONSE_PATTERN, content)
            if not match:
                error_message = "AdfsCredentialsProviderFactory.FailedLogin"
                logger.debug(error_message, content)
                raise AwsWrapperError(Messages.get_formatted(error_message, content))

            # return SAML Response value
            return match.group(self._SAML_RESPONSE_PATTERN_GROUP)

        except IOError as e:
            error_message = "FederatedAuthPlugin.UnhandledException"
            logger.debug(error_message, e)
            raise AwsWrapperError(Messages.get_formatted(error_message, e))

    def _get_sign_in_page_body(self, url: str, props: Properties) -> str:
        logger.debug("AdfsCredentialsProviderFactory.SignOnPageUrl", url)
        SamlUtils.validate_url(url)
        r = requests.get(url,
                         verify=WrapperProperties.SSL_SECURE.get_bool(props),
                         timeout=WrapperProperties.HTTP_REQUEST_TIMEOUT.get_int(props))

        SamlUtils.validate_response(r)
        return r.text

    def _post_form_action_body(self, uri: str, parameters: Dict[str, str], props: Properties) -> str:
        logger.debug("AdfsCredentialsProviderFactory.SignOnPagePostActionUrl", uri)
        SamlUtils.validate_url(uri)
        r = requests.post(uri, data=urlencode(parameters),
                          verify=WrapperProperties.SSL_SECURE.get_bool(props),
                          timeout=WrapperProperties.HTTP_REQUEST_TIMEOUT.get_int(props))
        # Check HTTP Status Code is 2xx Success
        SamlUtils.validate_response(r)
        return r.text

    def _get_sign_in_page_url(self, props) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props)
        idp_port = WrapperProperties.IDP_PORT.get_int(props)
        relaying_party_id = WrapperProperties.RELAYING_PARTY_ID.get(props)
        url = f"https://{idp_endpoint}:{idp_port}/adfs/ls/IdpInitiatedSignOn.aspx?loginToRp={relaying_party_id}"
        if idp_endpoint is None or relaying_party_id is None:
            error_message = "SamlUtils.InvalidHttpsUrl"
            logger.debug(error_message, url)
            raise AwsWrapperError(Messages.get_formatted(error_message, url))

        return url

    def _get_form_action_url(self, props: Properties, action: str) -> str:
        idp_endpoint = WrapperProperties.IDP_ENDPOINT.get(props) if not None else ""
        idp_port = WrapperProperties.IDP_PORT.get(props)
        url = f"https://{idp_endpoint}:{idp_port}{action}"
        if idp_endpoint is None:
            error_message = "SamlUtils.InvalidHttpsUrl"
            logger.debug(error_message, url)
            raise AwsWrapperError(
                Messages.get_formatted(error_message, url))
        return url

    def _get_input_tags_from_html(self, body: str) -> List[str]:
        distinct_input_tags: List[str] = []
        input_tags = findall(self._INPUT_TAG_PATTERN, body, DOTALL)
        for input_tag in input_tags:
            tag_name: str = self._get_value_by_key(input_tag, "name")

            if tag_name != "" and tag_name not in distinct_input_tags:
                distinct_input_tags.append(tag_name)
        return input_tags

    def _get_value_by_key(self, input: str, key: str) -> str:

        key_value_pattern = r"(" + key + ")\\s*=\\s*\"(.*?)\""
        match = search(key_value_pattern, input)
        if match:
            return unescape(match.group(2))

        return ""

    def _get_parameters_from_html_body(self, body: str, props: Properties) -> Dict[str, str]:
        parameters: Dict[str, str] = {}
        for input_tag in self._get_input_tags_from_html(body):
            name: str = self._get_value_by_key(input_tag, "name")
            name_lower: str = name.lower()
            value: str = self._get_value_by_key(input_tag, "value")

            if "username" in name_lower:
                idp_user = WrapperProperties.IDP_USERNAME.get(props)
                if idp_user is not None:
                    parameters[name] = idp_user
            elif "authmethod" in name_lower:
                if value != "":
                    parameters[name] = value
            elif "password" in name_lower:
                idp_password = WrapperProperties.IDP_PASSWORD.get(props)
                if idp_password is not None:
                    parameters[name] = idp_password
            elif name != "":
                parameters[name] = value

        return parameters

    def _get_form_action_from_html_body(self, body: str) -> str:
        match = search(self._FORM_ACTION_PATTERN, body)
        if match:
            return unescape(match.group(1))

        return ""
