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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from boto3 import Session
    from aws_wrapper.generic_target_driver_dialect import TargetDriverDialect
    from aws_wrapper.hostinfo import HostInfo
    from aws_wrapper.pep249 import Connection
    from aws_wrapper.plugin_service import PluginService

from json import loads
from aws_wrapper.utils.log import Logger
from re import search
from types import SimpleNamespace
from typing import Callable, Dict, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.plugin import Plugin, PluginFactory
from aws_wrapper.utils.messages import Messages
from aws_wrapper.utils.properties import Properties, WrapperProperties

logger = Logger(__name__)


class AwsSecretsManagerPlugin(Plugin):
    _SUBSCRIBED_METHODS: Set[str] = {"connect", "force_connect"}

    _SECRETS_ARN_PATTERN = r"^arn:aws:secretsmanager:(?P<region>[^:\n]*):[^:\n]*:([^:/\n]*[:/])?(.*)$"

    _secrets_cache: Dict[Tuple, SimpleNamespace] = {}
    _secret_key: Tuple = ()

    @property
    def subscribed_methods(self) -> Set[str]:
        return self._SUBSCRIBED_METHODS

    def __init__(self, plugin_service: PluginService, props: Properties, session: Optional[Session] = None):
        self._plugin_service = plugin_service
        self._session = session

        secret_id = WrapperProperties.SECRETS_MANAGER_SECRET_ID.get(props)
        if not secret_id:
            raise AwsWrapperError(
                Messages.get_formatted("AwsSecretsManagerPlugin.MissingRequiredConfigParameter",
                                       WrapperProperties.SECRETS_MANAGER_SECRET_ID.name))

        region: str = self._get_rds_region(secret_id, props)

        self._secret_key: Tuple = (secret_id, region)

    def connect(
            self,
            target_driver_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        return self._connect(props, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            target_driver_dialect: TargetDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        return self._connect(props, force_connect_func)

    def _connect(self, props: Properties, connect_func: Callable) -> Connection:
        secret_fetched: bool = self._update_secret()

        try:
            self._apply_secret_to_properties(props)
            return connect_func()

        except Exception as e:
            if not self._plugin_service.is_login_exception(error=e) or secret_fetched:
                raise AwsWrapperError(
                    Messages.get_formatted("AwsSecretsManagerPlugin.ConnectException", e)) from e

            secret_fetched = self._update_secret(True)

            if secret_fetched:
                try:
                    self._apply_secret_to_properties(props)
                    return connect_func()
                except Exception as unhandled_error:
                    raise AwsWrapperError(
                        Messages.get_formatted("AwsSecretsManagerPlugin.UnhandledException",
                                               unhandled_error)) from unhandled_error
            raise AwsWrapperError(Messages.get_formatted("AwsSecretsManagerPlugin.FailedLogin", e)) from e

    def _update_secret(self, force_refetch: bool = False) -> bool:
        fetched: bool = False

        self._secret: Optional[SimpleNamespace] = AwsSecretsManagerPlugin._secrets_cache.get(self._secret_key)

        if not self._secret or force_refetch:
            try:
                self._secret = self._fetch_latest_credentials()
                if self._secret:
                    AwsSecretsManagerPlugin._secrets_cache[self._secret_key] = self._secret
                    fetched = True
            except (ClientError, AttributeError) as e:
                logger.debug("AwsSecretsManagerPlugin.FailedToFetchDbCredentials", e)
                raise AwsWrapperError(Messages.get_formatted("AwsSecretsManagerPlugin.FailedToFetchDbCredentials", e)) from e

        return fetched

    def _fetch_latest_credentials(self):
        session = self._session if self._session else boto3.Session()
        client = session.client(
            'secretsmanager',
            region_name=self._secret_key[1],
        )

        secret = client.get_secret_value(
            SecretId=self._secret_key[0],
        )

        client.close()

        return loads(secret.get("SecretString"), object_hook=lambda d: SimpleNamespace(**d))

    def _apply_secret_to_properties(self, properties: Properties):
        if self._secret:
            WrapperProperties.USER.set(properties, self._secret.username)
            WrapperProperties.PASSWORD.set(properties, self._secret.password)

    def _get_rds_region(self, secret_id: str, props: Properties) -> str:
        region: Optional[str] = props.get(WrapperProperties.SECRETS_MANAGER_REGION.name)
        if not region:
            match = search(self._SECRETS_ARN_PATTERN, secret_id)
            if match:
                region = match.group("region")
            else:
                raise AwsWrapperError(
                    Messages.get_formatted("AwsSecretsManagerPlugin.MissingRequiredConfigParameter",
                                           WrapperProperties.SECRETS_MANAGER_REGION.name))

        session = self._session if self._session else boto3.Session()
        if region not in session.get_available_regions("rds"):
            exception_message = "AwsSdk.UnsupportedRegion"
            logger.debug(exception_message, region)
            raise AwsWrapperError(Messages.get_formatted(exception_message, region))

        return region


class AwsSecretsManagerPluginFactory(PluginFactory):
    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return AwsSecretsManagerPlugin(plugin_service, props)
