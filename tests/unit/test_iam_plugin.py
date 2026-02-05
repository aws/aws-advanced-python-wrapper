#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable    law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import urllib.request
from datetime import datetime, timedelta
from typing import Dict
from unittest.mock import patch

import pytest
from boto3 import Session

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.iam_plugin import IamAuthPlugin, TokenInfo
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

_GENERATED_TOKEN = "generated_token"
_TEST_TOKEN = "test_token"
_DEFAULT_PG_PORT = 5432
_DEFAULT_MYSQL_PORT = 3306
_PG_CACHE_KEY = f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{_DEFAULT_PG_PORT}:postgresqlUser"

_MYSQL_HOST_INFO = HostInfo("mysql.testdb.us-east-2.rds.amazonaws.com")
_PG_HOST_INFO = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com")
_PG_HOST_INFO_WITH_PORT = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com", port=1234)
_PG_HOST_INFO_WITH_REGION = HostInfo("pg.testdb.us-west-1.rds.amazonaws.com")

_token_cache: Dict[str, TokenInfo] = {}


@pytest.fixture(autouse=True)
def clear_caches():
    _token_cache.clear()
    AwsCredentialsManager.release_resources()


@pytest.fixture
def mock_client(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_session(mocker, mock_client):
    return mocker.MagicMock(spec=Session)


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
def mock_default_behavior(mock_session, mock_client, mock_func, mock_connection, mock_plugin_service, mock_dialect, mocker):
    mock_session.client.return_value = mock_client
    mock_client.generate_db_auth_token.return_value = _GENERATED_TOKEN
    mock_session.get_available_regions.return_value = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
    mock_func.return_value = mock_connection
    mock_plugin_service.driver_dialect = mock_dialect
    mock_plugin_service.database_dialect = mock_dialect
    mock_dialect.default_port = _DEFAULT_PG_PORT

    mocker.patch('aws_advanced_python_wrapper.iam_plugin.AwsCredentialsManager.get_session', return_value=mock_session)
    yield


@pytest.fixture
def pg_properties():
    return Properties({"user": "postgresqlUser"})


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_valid_token_in_cache(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    initial_token = TokenInfo(_TEST_TOKEN, datetime.now() + timedelta(minutes=5))
    _token_cache[_PG_CACHE_KEY] = initial_token

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_not_called()

    actual_token = _token_cache.get(_PG_CACHE_KEY)
    assert _GENERATED_TOKEN != actual_token.token
    assert _TEST_TOKEN == actual_token.token
    assert actual_token.is_expired() is False


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_with_invalid_port_fall_backs_to_host_port(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    invalid_port = "0"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
        Port=1234,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get("us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:postgresqlUser")
    assert _GENERATED_TOKEN == actual_token.token
    assert actual_token.is_expired() is False

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "postgresqlUser", "iam_default_port": "0"}
    mock_dialect.set_password.assert_called_with(expected_props, _GENERATED_TOKEN)


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_with_invalid_port_and_no_host_port_fall_backs_to_host_port(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    expected_default_pg_port = 5432
    invalid_port = "0"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
        Port=expected_default_pg_port,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get(
        f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{expected_default_pg_port}:postgresqlUser")
    assert _GENERATED_TOKEN == actual_token.token
    assert actual_token.is_expired() is False

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "postgresqlUser", "iam_default_port": "0"}
    mock_dialect.set_password.assert_called_with(expected_props, _GENERATED_TOKEN)


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_expired_token_in_cache(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    initial_token = TokenInfo(_TEST_TOKEN, datetime.now() - timedelta(minutes=5))
    _token_cache[_PG_CACHE_KEY] = initial_token

    mock_func.side_effect = Exception("generic exception")
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    with pytest.raises(Exception):
        target_plugin.connect(
            target_driver_func=mocker.MagicMock(),
            driver_dialect=mock_dialect,
            host_info=_PG_HOST_INFO,
            props=test_props,
            is_initial_connection=False,
            connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
        Port=5432,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get(_PG_CACHE_KEY)
    assert initial_token != actual_token
    assert _GENERATED_TOKEN == actual_token.token
    assert actual_token.is_expired() is False


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_empty_cache(mocker, mock_plugin_service, mock_connection, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    actual_connection = target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
        Port=5432,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get(_PG_CACHE_KEY)
    assert mock_connection == actual_connection
    assert _GENERATED_TOKEN == actual_token.token
    assert actual_token.is_expired() is False


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_port(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    cache_key_with_new_port: str = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:1234:postgresqlUser"
    initial_token = TokenInfo(f"{_TEST_TOKEN}:1234", datetime.now() + timedelta(minutes=5))
    _token_cache[cache_key_with_new_port] = initial_token

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_not_called()

    actual_token = _token_cache.get(cache_key_with_new_port)
    assert _token_cache.get(_PG_CACHE_KEY) is None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:1234" == actual_token.token
    assert actual_token.is_expired() is False

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "postgresqlUser"}
    mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:1234")


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_iam_default_port(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    iam_default_port: str = "9999"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = iam_default_port
    cache_key_with_new_port = f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{iam_default_port}:postgresqlUser"
    initial_token = TokenInfo(f"{_TEST_TOKEN}:{iam_default_port}", datetime.now() + timedelta(minutes=5))
    _token_cache[cache_key_with_new_port] = initial_token

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_not_called()

    actual_token = _token_cache.get(cache_key_with_new_port)
    assert _token_cache.get(_PG_CACHE_KEY) is None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:{iam_default_port}" == actual_token.token
    assert actual_token.is_expired() is False

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "postgresqlUser", "iam_default_port": "9999"}
    mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:{iam_default_port}")


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_region(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    iam_region: str = "us-east-1"

    # Cache a token with a different region
    cache_key_with_region = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:5432:postgresqlUser"
    initial_token = TokenInfo("us-east-2", datetime.now() + timedelta(minutes=5))
    _token_cache[cache_key_with_region] = initial_token

    test_props[WrapperProperties.IAM_REGION.name] = iam_region

    # Assert no password has been set
    assert test_props.get("password") is None

    mock_client.generate_db_auth_token.return_value = f"{_TEST_TOKEN}:{iam_region}"
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=HostInfo("pg.testdb.us-east-2.rds.amazonaws.com"),
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_session.client.assert_called_with(service_name="rds")
    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname="pg.testdb.us-east-2.rds.amazonaws.com",
        Port=5432,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get("us-east-1:pg.testdb.us-east-2.rds.amazonaws.com:5432:postgresqlUser")
    assert f"{_TEST_TOKEN}:{iam_region}" == actual_token.token
    assert actual_token.is_expired() is False

    # Assert password has been updated to the value in token cache
    expected_props = {"iam_region": "us-east-1", "user": "postgresqlUser"}
    mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:{iam_region}")


@pytest.mark.parametrize("iam_host", [
    pytest.param("foo.testdb.us-east-2.rds.amazonaws.com"),
    pytest.param("test.cluster-123456789012.us-east-2.rds.amazonaws.com"),
    pytest.param("test-.cluster-ro-123456789012.us-east-2.rds.amazonaws.com"),
    pytest.param("test.cluster-custom-123456789012.us-east-2.rds.amazonaws.com"),
    pytest.param("test-.proxy-123456789012.us-east-2.rds.amazonaws.com.cn"),
    pytest.param("test-.proxy-123456789012.us-east-2.rds.amazonaws.com"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_host(iam_host: str, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})

    test_props[WrapperProperties.IAM_HOST.name] = iam_host

    # Assert no password has been set
    assert test_props.get("password") is None

    mock_client.generate_db_auth_token.return_value = f"{_TEST_TOKEN}:{iam_host}"
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=HostInfo("bar.foo.com"),
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_called_with(
        DBHostname=iam_host,
        Port=5432,
        DBUsername="postgresqlUser"
    )

    actual_token = _token_cache.get(f"us-east-2:{iam_host}:5432:postgresqlUser")
    assert actual_token is not None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:{iam_host}" == actual_token.token
    assert actual_token.is_expired() is False


def test_aws_supported_regions_url_exists():
    url = "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html"
    assert 200 == urllib.request.urlopen(url).getcode()


@pytest.mark.parametrize("host", [
    pytest.param("<>"),
    pytest.param("#"),
    pytest.param("'"),
    pytest.param("\""),
    pytest.param("%"),
    pytest.param("^"),
    pytest.param("https://foo.com/abc.html"),
    pytest.param("foo.boo//"),
    pytest.param("8.8.8.8"),
    pytest.param("a.b"),
])
def test_invalid_iam_host(host, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "postgresqlUser"})
    with pytest.raises(AwsWrapperError):
        target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service)
        target_plugin.connect(
            target_driver_func=mocker.MagicMock(),
            driver_dialect=mock_dialect,
            host_info=HostInfo(host),
            props=test_props,
            is_initial_connection=False,
            connect_func=mock_func)
