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

import urllib.request
from datetime import datetime, timedelta
from typing import Callable, Dict
from unittest import TestCase
from unittest.mock import MagicMock, patch

from boto3 import Session, client

from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.pep249 import Connection
from aws_wrapper.plugins import (IamAuthConnectionPlugin, PluginService,
                                 TokenInfo)
from aws_wrapper.utils.dialect import Dialect
from aws_wrapper.utils.properties import Properties, WrapperProperties


class TestIamConnectionPlugin(TestCase):
    _GENERATED_TOKEN = "generated_token"
    _TEST_TOKEN = "test_token"
    _DEFAULT_PG_PORT = 5432
    _DEFAULT_MYSQL_PORT = 3306
    _PG_CACHE_KEY = f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{_DEFAULT_PG_PORT}:postgresqlUser"
    _MYSQL_CACHE_KEY = f"us-east-2:mysql.testdb.us-east-2.rds.amazonaws.com:{_DEFAULT_MYSQL_PORT}:mysqlUser"

    _MYSQL_HOST_INFO = HostInfo("mysql.testdb.us-east-2.rds.amazonaws.com")
    _PG_HOST_INFO = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com")
    _PG_HOST_INFO_WITH_PORT = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com", port=1234)
    _PG_HOST_INFO_WITH_REGION = HostInfo("pg.testdb.us-west-1.rds.amazonaws.com")

    _token_cache: Dict[str, TokenInfo] = {}

    _mock_func: Callable
    _mock_plugin_service: PluginService
    _mock_dialect: Dialect
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

        self._token_cache.clear()
        self._mock_session.client.return_value = self._mock_client
        self._mock_client.generate_db_auth_token.return_value = self._GENERATED_TOKEN
        self._mock_session.get_available_regions.return_value = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
        self._mock_func.return_value = self._mock_connection
        self._pg_properties = Properties({
            "user": "postgresqlUser",
        })

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_pg_connect_valid_token_in_cache(self):
        initial_token = TokenInfo(self._TEST_TOKEN, datetime.now() + timedelta(minutes=5))
        self._token_cache[self._PG_CACHE_KEY] = initial_token

        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service,
                                                                         self._mock_session)
        target_plugin.connect(host_info=self._PG_HOST_INFO, props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_not_called()

        actual_token = self._token_cache.get(self._PG_CACHE_KEY)
        self.assertNotEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertEqual(self._TEST_TOKEN, actual_token.token)
        self.assertFalse(actual_token.is_expired())

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_pg_connect_with_invalid_port_fall_backs_to_host_port(self):
        invalid_port = "0"
        self._pg_properties[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service,
                                                                         self._mock_session)
        target_plugin.connect(host_info=self._PG_HOST_INFO_WITH_PORT, props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
            Port=1234,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get("us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:postgresqlUser")
        self.assertEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(self._GENERATED_TOKEN, self._pg_properties.get("password"))

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_pg_connect_with_invalid_port_and_no_host_port_fall_backs_to_host_port(self):
        expected_default_pg_port = 5432
        invalid_port = "0"
        self._pg_properties[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service,
                                                                         self._mock_session)
        target_plugin.connect(host_info=self._PG_HOST_INFO, props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
            Port=expected_default_pg_port,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get(
            f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{expected_default_pg_port}:postgresqlUser")
        self.assertEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(self._GENERATED_TOKEN, self._pg_properties.get("password"))

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_expired_token_in_cache(self):
        initial_token = TokenInfo(self._TEST_TOKEN, datetime.now() - timedelta(minutes=5))
        self._token_cache[self._PG_CACHE_KEY] = initial_token

        self._mock_func.side_effect = Exception("generic exception")
        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        self.assertRaises(
            Exception,
            target_plugin.connect,
            host_info=self._PG_HOST_INFO, props=self._pg_properties, initial=False,
            connect_func=self._mock_func
        )
        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
            Port=5432,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get(self._PG_CACHE_KEY)
        self.assertNotEqual(initial_token, actual_token)
        self.assertEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertFalse(actual_token.is_expired())

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_empty_cache(self):
        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        actual_connection = target_plugin.connect(host_info=self._PG_HOST_INFO, props=self._pg_properties,
                                                  initial=False,
                                                  connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
            Port=5432,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get(self._PG_CACHE_KEY)
        self.assertEqual(self._mock_connection, actual_connection)
        self.assertEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertFalse(actual_token.is_expired())

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_with_specified_port(self):
        cache_key_with_new_port: str = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:postgresqlUser"
        initial_token = TokenInfo(f"{self._TEST_TOKEN}:1234", datetime.now() + timedelta(minutes=5))
        self._token_cache[cache_key_with_new_port] = initial_token

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        target_plugin.connect(host_info=self._PG_HOST_INFO_WITH_PORT, props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_not_called()

        actual_token = self._token_cache.get(cache_key_with_new_port)
        self.assertIsNone(self._token_cache.get(self._PG_CACHE_KEY))
        self.assertNotEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertEqual(f"{self._TEST_TOKEN}:1234", actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(f"{self._TEST_TOKEN}:1234", self._pg_properties.get("password"))

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_with_specified_iam_default_port(self):
        iam_default_port: str = "9999"
        self._pg_properties[WrapperProperties.IAM_DEFAULT_PORT.name] = iam_default_port
        cache_key_with_new_port = f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{iam_default_port}:postgresqlUser"
        initial_token = TokenInfo(f"{self._TEST_TOKEN}:{iam_default_port}", datetime.now() + timedelta(minutes=5))
        self._token_cache[cache_key_with_new_port] = initial_token

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        target_plugin.connect(host_info=self._PG_HOST_INFO_WITH_PORT, props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_not_called()

        actual_token = self._token_cache.get(cache_key_with_new_port)
        self.assertIsNone(self._token_cache.get(self._PG_CACHE_KEY))
        self.assertNotEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertEqual(f"{self._TEST_TOKEN}:{iam_default_port}", actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(f"{self._TEST_TOKEN}:{iam_default_port}", self._pg_properties.get("password"))

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_with_specified_region(self):
        iam_region: str = "us-east-1"

        # Cache a token with a different region
        cache_key_with_region = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:5432:postgresqlUser"
        initial_token = TokenInfo("us-east-2", datetime.now() + timedelta(minutes=5))
        self._token_cache[cache_key_with_region] = initial_token

        self._pg_properties[WrapperProperties.IAM_REGION.name] = iam_region

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        self._mock_client.generate_db_auth_token.return_value = f"{self._TEST_TOKEN}:{iam_region}"
        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        target_plugin.connect(host_info=HostInfo("pg.testdb.us-east-2.rds.amazonaws.com"), props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_session.client.assert_called_with(
            "rds",
            region_name=iam_region
        )
        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
            Port=5432,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get("us-east-1:pg.testdb.us-east-2.rds.amazonaws.com:5432:postgresqlUser")
        self.assertEqual(f"{self._TEST_TOKEN}:{iam_region}", actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(f"{self._TEST_TOKEN}:{iam_region}", self._pg_properties.get("password"))

    @patch("aws_wrapper.plugins.IamAuthConnectionPlugin._token_cache", _token_cache)
    def test_connect_with_specified_host(self):
        iam_host: str = "foo.testdb.us-east-2.rds.amazonaws.com"

        self._pg_properties[WrapperProperties.IAM_HOST.name] = iam_host

        # Assert no password has been set
        self.assertIsNone(self._pg_properties.get("password"))

        self._mock_client.generate_db_auth_token.return_value = f"{self._TEST_TOKEN}:{iam_host}"
        target_plugin: IamAuthConnectionPlugin = IamAuthConnectionPlugin(self._mock_plugin_service, self._mock_session)
        target_plugin.connect(host_info=HostInfo("pg.testdb.us-east-2.rds.amazonaws.com"), props=self._pg_properties,
                              initial=False,
                              connect_func=self._mock_func)

        self._mock_client.generate_db_auth_token.assert_called_with(
            DBHostname=iam_host,
            Port=5432,
            DBUsername="postgresqlUser"
        )

        actual_token = self._token_cache.get("us-east-2:foo.testdb.us-east-2.rds.amazonaws.com:5432:postgresqlUser")
        self.assertNotEqual(self._GENERATED_TOKEN, actual_token.token)
        self.assertEqual(f"{self._TEST_TOKEN}:foo.testdb.us-east-2.rds.amazonaws.com", actual_token.token)
        self.assertFalse(actual_token.is_expired())

        # Assert password has been updated to the value in token cache
        self.assertEqual(f"{self._TEST_TOKEN}:foo.testdb.us-east-2.rds.amazonaws.com",
                         self._pg_properties.get("password"))

    def test_aws_supported_regions_url_exists(self):
        url = "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html"
        self.assertEqual(200, urllib.request.urlopen(url).getcode())
