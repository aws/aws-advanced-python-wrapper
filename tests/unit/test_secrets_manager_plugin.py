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

from aws_advanced_python_wrapper.aws_secrets_manager_plugin import \
    AwsSecretsManagerPlugin

if TYPE_CHECKING:
    from boto3 import Session, client
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.plugin_service import PluginService

from types import SimpleNamespace
from typing import Callable, Dict, Tuple
from unittest import TestCase
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from parameterized import param, parameterized

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties


class TestAwsSecretsManagerPlugin(TestCase):
    _TEST_REGION = "us-east-2"
    _TEST_SECRET_ID = "secretId"
    _TEST_ENDPOINT = None
    _TEST_USERNAME = "testUser"
    _TEST_PASSWORD = "testPassword"
    _TEST_USERNAME_KEY = "testUserKey"
    _TEST_PASSWORD_KEY = "testPasswordKey"
    _TEST_PORT = 5432
    _VALID_SECRET_STRING = {'SecretString': f'{{"username":"{_TEST_USERNAME}","password":"{_TEST_PASSWORD}"}}'}
    _INVALID_SECRET_STRING = {'SecretString': {"username": "invalid", "password": "invalid"}}
    _TEST_HOST = "test-domain"
    _SECRET_CACHE_KEY = (_TEST_SECRET_ID, _TEST_REGION, _TEST_ENDPOINT)
    _TEST_HOST_INFO = HostInfo(_TEST_HOST, _TEST_PORT)
    _TEST_SECRET = SimpleNamespace(username="testUser", password="testPassword")

    _MYSQL_HOST_INFO = HostInfo("mysql.testdb.us-east-2.rds.amazonaws.com")
    _PG_HOST_INFO = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com")
    _PG_HOST_INFO_WITH_PORT = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com", port=1234)
    _PG_HOST_INFO_WITH_REGION = HostInfo("pg.testdb.us-west-1.rds.amazonaws.com")

    _GENERIC_CLIENT_ERROR = ClientError({
        'Error': {
            'Code': 'SomeServiceException',
            'Message': 'Details/context around the exception or error'
        },
        'ResponseMetadata': {
            'HTTPStatusCode': 400,
        }
    }, "some_operation")

    _secrets_cache: Dict[Tuple, SimpleNamespace] = {}

    _mock_func: Callable
    _mock_plugin_service: PluginService
    _mock_dialect: DatabaseDialect
    _mock_session: Session
    _mock_client: client
    _mock_connection: Connection
    _pg_properties: Properties

    def setUp(self):
        self._mock_func = MagicMock()
        self._mock_plugin_service = MagicMock()
        self._mock_dialect = MagicMock()
        self._mock_session = MagicMock()
        self._mock_client = MagicMock()
        self._mock_connection = MagicMock()

        self._secrets_cache.clear()
        self._mock_session.client.return_value = self._mock_client
        self._mock_client.get_secret_value.return_value = self._VALID_SECRET_STRING
        self._mock_session.get_available_regions.return_value = ["us-east-1", "us-east-2", "us-west-1", "us-west-2",
                                                                 "us-iso-east-1"]
        self._mock_func.return_value = self._mock_connection
        self._properties = Properties({
            "secrets_manager_region": self._TEST_REGION,
            "secrets_manager_secret_id": self._TEST_SECRET_ID,
        })

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_connect_with_cached_secrets(self):
        self._secrets_cache[self._SECRET_CACHE_KEY] = self._TEST_SECRET
        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(self._mock_plugin_service,
                                                                         self._properties,
                                                                         self._mock_session)

        target_plugin.connect(
            MagicMock(), MagicMock(), self._TEST_HOST_INFO, self._properties, True, self._mock_func)
        assert 1 == len(self._secrets_cache)
        self._mock_client.get_secret_value.assert_not_called()
        self._mock_func.assert_called_once()
        assert self._TEST_USERNAME == self._properties.get("user")
        assert self._TEST_PASSWORD == self._properties.get("password")

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_connect_with_new_secrets(self):
        assert 0 == len(self._secrets_cache)

        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(self._mock_plugin_service,
                                                                         self._properties,
                                                                         self._mock_session)

        target_plugin.connect(
            MagicMock(), MagicMock(), self._TEST_HOST_INFO, self._properties, True, self._mock_func)

        assert 1 == len(self._secrets_cache)
        self._mock_client.get_secret_value.assert_called_once()
        self._mock_func.assert_called_once()
        assert self._TEST_USERNAME == self._properties.get("user")
        assert self._TEST_PASSWORD == self._properties.get("password")

    @parameterized.expand([
        param(Properties({"secrets_manager_region": "us-east-2"})),
        param(Properties({"secrets_manager_secret_id": "foo"}))
    ])
    def test_missing_required_params(self, test_value: Properties):
        with self.assertRaises(AwsWrapperError) as e:
            AwsSecretsManagerPlugin(self._mock_plugin_service,
                                    test_value,
                                    self._mock_session)
            self.assertTrue(Messages.get("AwsSecretsManagerPlugin.FailedToFetchDbCredentials") in str(e))

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_failed_initial_connection_with_unhandled_error(self):
        ...

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_connect_with_new_secrets_after_trying_with_cached_secrets(self):
        ...

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_failed_to_read_secrets(self):
        self._mock_client.get_secret_value.return_value = "foo"

        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
            self._mock_plugin_service,
            self._properties,
            self._mock_session)

        self.assertRaises(AwsWrapperError,
                          target_plugin.connect,
                          MagicMock(),
                          MagicMock(),
                          self._TEST_HOST_INFO,
                          self._properties,
                          True,
                          self._mock_func)

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_failed_to_get_secrets(self):
        self._mock_client.get_secret_value.side_effect = self._GENERIC_CLIENT_ERROR
        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
            self._mock_plugin_service,
            self._properties,
            self._mock_session)

        self.assertRaises(AwsWrapperError,
                          target_plugin.connect,
                          MagicMock(),
                          MagicMock(),
                          self._TEST_HOST_INFO,
                          self._properties,
                          True,
                          self._mock_func)
        self._mock_client.get_secret_value.assert_called_once()
        self._mock_func.assert_not_called()

    @parameterized.expand([
        param("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", "us-east-2"),
        param("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", "us-west-1"),
        param("arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo", "us-east-2")
    ])
    def test_connect_via_arn(self, arn: str, region: str):
        props: Properties = Properties({"secrets_manager_secret_id": arn})

        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
            self._mock_plugin_service,
            props,
            self._mock_session)

        target_plugin.connect(
            MagicMock(), MagicMock(), self._TEST_HOST_INFO, props, True, self._mock_func)

        self._mock_session.client.assert_called_with('secretsmanager', region_name=region, endpoint_url=None)
        self._mock_client.get_secret_value.assert_called_with(SecretId=arn)

    @parameterized.expand([
        param("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", "us-east-2"),
        param("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", "us-west-1"),
        param("arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo", "us-east-2"),
    ])
    def test_connection_with_region_parameter_and_arn(self, arn: str, parsed_region: str):
        expected_region: str = "us-iso-east-1"

        props: Properties = Properties(
            {"secrets_manager_secret_id": arn,
             "secrets_manager_region": expected_region})

        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
            self._mock_plugin_service,
            props,
            self._mock_session)

        target_plugin.connect(
            MagicMock(), MagicMock(), self._TEST_HOST_INFO, props, True, self._mock_func)

        # The region specified in `secrets_manager_region` should override the region parsed from ARN.
        self._mock_session.client.assert_called_with('secretsmanager', region_name=expected_region, endpoint_url=None)
        self._mock_client.get_secret_value.assert_called_with(SecretId=arn)

    @patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
    def test_connect_with_different_secret_keys(self):
        self._properties["secrets_manager_secret_username_key"] = self._TEST_USERNAME_KEY
        self._properties["secrets_manager_secret_password_key"] = self._TEST_PASSWORD_KEY
        self._mock_client.get_secret_value.return_value = {
            'SecretString': f'{{"{self._TEST_USERNAME_KEY}":"{self._TEST_USERNAME}","{self._TEST_PASSWORD_KEY}":"{self._TEST_PASSWORD}"}}'
        }

        target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(self._mock_plugin_service,
                                                                         self._properties,
                                                                         self._mock_session)
        target_plugin.connect(
            MagicMock(), MagicMock(), self._TEST_HOST_INFO, self._properties, True, self._mock_func)

        assert 1 == len(self._secrets_cache)
        self._mock_client.get_secret_value.assert_called_once()
        self._mock_func.assert_called_once()
        assert self._TEST_USERNAME == self._properties.get("user")
        assert self._TEST_PASSWORD == self._properties.get("password")
