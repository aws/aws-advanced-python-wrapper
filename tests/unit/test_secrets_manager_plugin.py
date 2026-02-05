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

from types import SimpleNamespace
from typing import Tuple
from unittest.mock import patch

import pytest
from boto3 import Session
from botocore.exceptions import ClientError

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.aws_secrets_manager_plugin import \
    AwsSecretsManagerPlugin
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from aws_advanced_python_wrapper.utils.properties import Properties

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
_INVALID_TEST_SECRET = SimpleNamespace(username="oldUser", password="oldPassword")
_ONE_YEAR_IN_NANOSECONDS = 60 * 60 * 24 * 365 * 1000

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
        'RequestId': 'test-request-id',
        'HostId': 'test-host-id',
        'HTTPHeaders': {},
        'RetryAttempts': 0
    }
}, "some_operation")

_secrets_cache: CacheMap[Tuple, SimpleNamespace] = CacheMap()


@pytest.fixture(autouse=True)
def clear_caches():
    _secrets_cache.clear()
    AwsCredentialsManager.release_resources()


@pytest.fixture
def mock_client(mocker):
    client = mocker.MagicMock()
    return client


@pytest.fixture
def mock_session(mocker):
    session = mocker.MagicMock(spec=Session)
    return session


@pytest.fixture
def mock_connection(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_func(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def mock_default_behavior(mock_session, mock_client, mock_func, mock_connection, mocker):
    mock_session.client.return_value = mock_client
    mock_client.get_secret_value.return_value = _VALID_SECRET_STRING
    mock_session.get_available_regions.return_value = [
        "us-east-1", "us-east-2", "us-west-1", "us-west-2", "us-iso-east-1"
    ]
    mock_func.return_value = mock_connection

    mocker.patch('aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsCredentialsManager.get_session', return_value=mock_session)
    yield


@pytest.fixture
def test_properties():
    return Properties({
        "secrets_manager_region": _TEST_REGION,
        "secrets_manager_secret_id": _TEST_SECRET_ID,
    })


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_connect_with_cached_secrets(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    _secrets_cache.put(_SECRET_CACHE_KEY, _TEST_SECRET, _ONE_YEAR_IN_NANOSECONDS)
    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, test_properties, mock_session)

    target_plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)
    assert 1 == len(_secrets_cache)
    mock_client.get_secret_value.assert_not_called()
    mock_func.assert_called_once()
    assert _TEST_USERNAME == test_properties.get("user")
    assert _TEST_PASSWORD == test_properties.get("password")


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_connect_with_new_secrets(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    assert 0 == len(_secrets_cache)

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, test_properties, mock_session)

    target_plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)

    assert 1 == len(_secrets_cache)
    mock_client.get_secret_value.assert_called_once()
    mock_func.assert_called_once()
    assert _TEST_USERNAME == test_properties.get("user")
    assert _TEST_PASSWORD == test_properties.get("password")


@pytest.mark.parametrize("key", [
    pytest.param("secrets_manager_region"),
    pytest.param("secrets_manager_secret_id")
])
def test_missing_required_params(key: str, mock_plugin_service, mock_session):
    test_props = Properties({"secrets_manager_region": "us-east-2", "secrets_manager_secret_id": "foo"})
    test_props.pop(key)
    with pytest.raises(AwsWrapperError) as exc_info:
        AwsSecretsManagerPlugin(mock_plugin_service, test_props, mock_session)
    # The error message should mention the missing parameter
    assert "required" in str(exc_info.value).lower()


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_failed_initial_connection_with_unhandled_error(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    exception_msg = "Unhandled error during connection"

    # Simulate an unhandled exception (neither a login exception nor a network exception)
    unhandled_exception = RuntimeError(exception_msg)
    mock_func.side_effect = unhandled_exception
    mock_plugin_service.is_login_exception.return_value = False

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(mock_plugin_service, test_properties, mock_session)

    with pytest.raises(AwsWrapperError):
        target_plugin.connect(
            mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)

    assert 1 == len(_secrets_cache)
    mock_client.get_secret_value.assert_called_once()
    mock_func.assert_called_once()
    assert _TEST_USERNAME == test_properties.get("user")
    assert _TEST_PASSWORD == test_properties.get("password")


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_connect_with_new_secrets_after_trying_with_cached_secrets(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    _secrets_cache.put(_SECRET_CACHE_KEY, _INVALID_TEST_SECRET, _ONE_YEAR_IN_NANOSECONDS)

    login_exception = Exception("Login failed with cached credentials")
    mock_func.side_effect = [login_exception, mocker.MagicMock()]
    mock_plugin_service.is_login_exception.return_value = True

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(mock_plugin_service, test_properties, mock_session)

    target_plugin.connect(mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)

    assert 1 == len(_secrets_cache)
    mock_client.get_secret_value.assert_called_once()
    assert 2 == mock_func.call_count
    assert _TEST_USERNAME == test_properties.get("user")
    assert _TEST_PASSWORD == test_properties.get("password")


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_failed_to_read_secrets(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    mock_client.get_secret_value.return_value = "foo"

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, test_properties, mock_session)

    with pytest.raises(AwsWrapperError):
        target_plugin.connect(
            mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_failed_to_get_secrets(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    mock_client.get_secret_value.side_effect = _GENERIC_CLIENT_ERROR
    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, test_properties, mock_session)

    with pytest.raises(AwsWrapperError):
        target_plugin.connect(
            mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)
    mock_client.get_secret_value.assert_called_once()
    mock_func.assert_not_called()


@pytest.mark.parametrize("arn,region", [
    pytest.param("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", "us-east-2"),
    pytest.param("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", "us-west-1"),
    pytest.param("arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo", "us-east-2")
])
def test_connect_via_arn(
        arn: str, region: str, mocker, mock_plugin_service, mock_session, mock_func, mock_client):
    props: Properties = Properties({"secrets_manager_secret_id": arn})

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, props, mock_session)

    target_plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, props, True, mock_func)

    mock_session.client.assert_called_with(service_name="secretsmanager")
    mock_client.get_secret_value.assert_called_with(SecretId=arn)


@pytest.mark.parametrize("arn,parsed_region", [
    pytest.param("arn:aws:secretsmanager:us-east-2:123456789012:secret:foo", "us-east-2"),
    pytest.param("arn:aws:secretsmanager:us-west-1:123456789012:secret:boo", "us-west-1"),
    pytest.param("arn:aws:secretsmanager:us-east-2:123456789012:secret:rds!cluster-bar-foo", "us-east-2"),
])
def test_connection_with_region_parameter_and_arn(
        arn: str, parsed_region: str, mocker, mock_plugin_service, mock_session, mock_func, mock_client):
    expected_region: str = "us-iso-east-1"

    props: Properties = Properties({
        "secrets_manager_secret_id": arn,
        "secrets_manager_region": expected_region
    })

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, props, mock_session)

    target_plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, props, True, mock_func)

    # The region specified in `secrets_manager_region` should override the region parsed from ARN.
    mock_session.client.assert_called_with(service_name="secretsmanager")
    mock_client.get_secret_value.assert_called_with(SecretId=arn)


@patch("aws_advanced_python_wrapper.aws_secrets_manager_plugin.AwsSecretsManagerPlugin._secrets_cache", _secrets_cache)
def test_connect_with_different_secret_keys(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, test_properties):
    test_properties["secrets_manager_secret_username_key"] = _TEST_USERNAME_KEY
    test_properties["secrets_manager_secret_password_key"] = _TEST_PASSWORD_KEY
    secret_string = (
        f'{{"{_TEST_USERNAME_KEY}":"{_TEST_USERNAME}",'
        f'"{_TEST_PASSWORD_KEY}":"{_TEST_PASSWORD}"}}'
    )
    mock_client.get_secret_value.return_value = {'SecretString': secret_string}

    target_plugin: AwsSecretsManagerPlugin = AwsSecretsManagerPlugin(
        mock_plugin_service, test_properties, mock_session)
    target_plugin.connect(
        mocker.MagicMock(), mocker.MagicMock(), _TEST_HOST_INFO, test_properties, True, mock_func)

    assert 1 == len(_secrets_cache)
    mock_client.get_secret_value.assert_called_once()
    mock_func.assert_called_once()
    assert _TEST_USERNAME == test_properties.get("user")
    assert _TEST_PASSWORD == test_properties.get("password")
