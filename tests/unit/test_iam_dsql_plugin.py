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

from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.iam_plugin import IamAuthPlugin, TokenInfo
from aws_advanced_python_wrapper.utils.dsql_token_utils import DSQLTokenUtils
from aws_advanced_python_wrapper.utils.iam_utils import IamAuthUtils
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

_GENERATED_TOKEN = "generated_token admin"
_GENERATED_TOKEN_NON_ADMIN = "generated_token non-admin"
_TEST_TOKEN = "test_token"
_DEFAULT_PG_PORT = 5432

_PG_HOST_INFO = HostInfo("dsqltestclusternamefoobar1.dsql.us-east-2.on.aws")
_PG_HOST_INFO_WITH_PORT = HostInfo(_PG_HOST_INFO.url, port=1234)
_PG_REGION = "us-east-2"

_PG_CACHE_KEY = f"{_PG_REGION}:{_PG_HOST_INFO.url}:{_DEFAULT_PG_PORT}:admin"


_token_cache: Dict[str, TokenInfo] = {}


@pytest.fixture(autouse=True)
def clear_caches():
    _token_cache.clear()


@pytest.fixture
def mock_session(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_client(mocker):
    return mocker.MagicMock()


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
def mock_default_behavior(mock_session, mock_client, mock_func, mock_connection, mock_plugin_service, mock_dialect):
    mock_session.client.return_value = mock_client
    mock_client.generate_db_connect_admin_auth_token.return_value = _GENERATED_TOKEN
    mock_client.generate_db_connect_auth_token.return_value = _GENERATED_TOKEN_NON_ADMIN
    mock_session.get_available_regions.return_value = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
    mock_func.return_value = mock_connection
    mock_plugin_service.driver_dialect = mock_dialect
    mock_plugin_service.database_dialect = mock_dialect
    mock_dialect.default_port = _DEFAULT_PG_PORT


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def set_token_cache(user, host, port, region, expired=False):
    if not expired:
        initial_token = TokenInfo(_TEST_TOKEN, datetime.now() + timedelta(minutes=5))
    else:
        initial_token = TokenInfo(_TEST_TOKEN, datetime.now() - timedelta(minutes=5))
    cache_key: str = IamAuthUtils.get_cache_key(
            user,
            host,
            port,
            region
        )
    _token_cache[cache_key] = initial_token

    return cache_key, initial_token


@pytest.mark.parametrize("user", [
    pytest.param("admin"),
    pytest.param("non-admin"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_valid_token_in_cache(user, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": user})
    cache_key, _ = set_token_cache(user, _PG_HOST_INFO.url, _DEFAULT_PG_PORT, _PG_REGION)

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    actual_token = _token_cache.get(cache_key)
    if user == "admin":
        mock_client.generate_db_connect_admin_auth_token.assert_not_called()
        assert _GENERATED_TOKEN != actual_token.token
    else:
        mock_client.generate_db_connect_auth_token.assert_not_called()
        assert _GENERATED_TOKEN_NON_ADMIN != actual_token.token

    assert _TEST_TOKEN == actual_token.token
    assert not actual_token.is_expired()


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_with_invalid_port_fall_backs_to_host_port(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "admin"})
    invalid_port = "0"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_connect_admin_auth_token.assert_called_with(
        _PG_HOST_INFO.url, _PG_REGION
    )

    actual_token = _token_cache.get(f"{_PG_REGION}:{_PG_HOST_INFO.url}:1234:admin")
    assert _GENERATED_TOKEN == actual_token.token
    assert not actual_token.is_expired()

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "admin", "iam_default_port": "0"}
    mock_dialect.set_password.assert_called_with(expected_props, _GENERATED_TOKEN)


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_pg_connect_with_invalid_port_and_no_host_port_fall_backs_to_host_port(
        mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "admin"})
    expected_default_pg_port = 5432
    invalid_port = "0"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = invalid_port

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_connect_admin_auth_token.assert_called_with(
        _PG_HOST_INFO.url, _PG_REGION
    )

    actual_token = _token_cache.get(
        f"{_PG_REGION}:{_PG_HOST_INFO.url}:{expected_default_pg_port}:admin")
    assert _GENERATED_TOKEN == actual_token.token
    assert not actual_token.is_expired()

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "admin", "iam_default_port": "0"}
    mock_dialect.set_password.assert_called_with(expected_props, _GENERATED_TOKEN)


@pytest.mark.parametrize("user", [
    pytest.param("admin"),
    pytest.param("non-admin"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_expired_token_in_cache(user, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": user})
    cache_key, initial_token = set_token_cache(user, _PG_HOST_INFO.url, _DEFAULT_PG_PORT, _PG_REGION, True)

    mock_func.side_effect = Exception("generic exception")
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    with pytest.raises(Exception):
        target_plugin.connect(
            target_driver_func=mocker.MagicMock(),
            driver_dialect=mock_dialect,
            host_info=_PG_HOST_INFO,
            props=test_props,
            is_initial_connection=False,
            connect_func=mock_func)

    actual_token = _token_cache.get(cache_key)
    assert initial_token != actual_token
    assert not actual_token.is_expired()

    if user == "admin":
        mock_client.generate_db_connect_admin_auth_token.assert_called_with(
            _PG_HOST_INFO.url, _PG_REGION)
        assert _GENERATED_TOKEN == actual_token.token
    else:
        mock_client.generate_db_connect_auth_token.assert_called_with(
            _PG_HOST_INFO.url, _PG_REGION)
        assert _GENERATED_TOKEN_NON_ADMIN == actual_token.token


@pytest.mark.parametrize("user", [
    pytest.param("admin"),
    pytest.param("non-admin"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_empty_cache(user, mocker, mock_plugin_service, mock_connection, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": user})
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    actual_connection = target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    cache_key: str = IamAuthUtils.get_cache_key(
            user, _PG_HOST_INFO.url, _DEFAULT_PG_PORT, _PG_REGION
        )
    actual_token = _token_cache.get(cache_key)

    if user == "admin":
        mock_client.generate_db_connect_admin_auth_token.assert_called_with(
            _PG_HOST_INFO.url, _PG_REGION
        )
        assert _GENERATED_TOKEN == actual_token.token
    else:
        mock_client.generate_db_connect_auth_token.assert_called_with(
            _PG_HOST_INFO.url, _PG_REGION)
        assert _GENERATED_TOKEN_NON_ADMIN == actual_token.token

    assert mock_connection == actual_connection
    assert not actual_token.is_expired()


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_port(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "admin"})
    cache_key_with_new_port: str = f"{_PG_REGION}:{_PG_HOST_INFO.url}:1234:admin"
    initial_token = TokenInfo(f"{_TEST_TOKEN}:1234", datetime.now() + timedelta(minutes=5))
    _token_cache[cache_key_with_new_port] = initial_token

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_connect_admin_auth_token.assert_not_called()

    actual_token = _token_cache.get(cache_key_with_new_port)
    assert _token_cache.get(_PG_CACHE_KEY) is None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:1234" == actual_token.token
    assert not actual_token.is_expired()

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "admin"}
    mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:1234")


@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_iam_default_port(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "admin"})
    iam_default_port: str = "9999"
    test_props[WrapperProperties.IAM_DEFAULT_PORT.name] = iam_default_port
    cache_key_with_new_port = f"{_PG_REGION}:{_PG_HOST_INFO.url}:{iam_default_port}:admin"
    initial_token = TokenInfo(f"{_TEST_TOKEN}:{iam_default_port}", datetime.now() + timedelta(minutes=5))
    _token_cache[cache_key_with_new_port] = initial_token

    # Assert no password has been set
    assert test_props.get("password") is None

    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO_WITH_PORT,
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_connect_admin_auth_token.assert_not_called()

    actual_token = _token_cache.get(cache_key_with_new_port)
    assert _token_cache.get(_PG_CACHE_KEY) is None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:{iam_default_port}" == actual_token.token
    assert not actual_token.is_expired()

    # Assert password has been updated to the value in token cache
    expected_props = {"user": "admin", "iam_default_port": "9999"}
    mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:{iam_default_port}")


@pytest.mark.parametrize("user", [
    pytest.param("admin"),
    pytest.param("non-admin"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_region(user, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": user})
    iam_region: str = "us-east-1"

    # Cache a token with a different region
    set_token_cache(user, _PG_HOST_INFO.url, _DEFAULT_PG_PORT, _PG_REGION)
    test_props[WrapperProperties.IAM_REGION.name] = iam_region

    # Assert no password has been set
    assert test_props.get("password") is None

    mock_client.generate_db_connect_admin_auth_token.return_value = f"{_TEST_TOKEN}:{iam_region}"
    mock_client.generate_db_connect_auth_token.return_value = f"{_GENERATED_TOKEN_NON_ADMIN}:{iam_region}"
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=HostInfo(_PG_HOST_INFO.url),
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_session.client.assert_called_with(
        "dsql",
        region_name=iam_region
    )

    expected_props = {"iam_region": "us-east-1", "user": user}
    actual_token = _token_cache.get(IamAuthUtils.get_cache_key(user, _PG_HOST_INFO.url, _DEFAULT_PG_PORT, iam_region))
    assert not actual_token.is_expired()

    if user == "admin":
        mock_client.generate_db_connect_admin_auth_token.assert_called_with(
            _PG_HOST_INFO.url, iam_region
        )
        assert f"{_TEST_TOKEN}:{iam_region}" == actual_token.token
        mock_dialect.set_password.assert_called_with(expected_props, f"{_TEST_TOKEN}:{iam_region}")
    else:
        mock_client.generate_db_connect_auth_token.assert_called_with(
            _PG_HOST_INFO.url, iam_region)
        assert f"{_GENERATED_TOKEN_NON_ADMIN}:{iam_region}" == actual_token.token
        mock_dialect.set_password.assert_called_with(expected_props, f"{_GENERATED_TOKEN_NON_ADMIN}:{iam_region}")


@pytest.mark.parametrize("iam_host", [
    pytest.param("dsqltestclusternamefoobar1.dsql.us-east-2.on.aws"),
    pytest.param("dsqltestclusternamefoobar2.dsql.us-east-2.on.aws"),
])
@patch("aws_advanced_python_wrapper.iam_plugin.IamAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_host(iam_host: str, mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    test_props: Properties = Properties({"user": "admin"})

    test_props[WrapperProperties.IAM_HOST.name] = iam_host

    # Assert no password has been set
    assert test_props.get("password") is None

    mock_client.generate_db_connect_admin_auth_token.return_value = f"{_TEST_TOKEN}:{iam_host}"
    target_plugin: IamAuthPlugin = IamAuthPlugin(mock_plugin_service,
                                                 DSQLTokenUtils(),
                                                 mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=HostInfo("bar.foo.com"),
        props=test_props,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_connect_admin_auth_token.assert_called_with(
        iam_host, _PG_REGION
    )

    actual_token = _token_cache.get(f"{_PG_REGION}:{iam_host}:5432:admin")
    assert actual_token is not None
    assert _GENERATED_TOKEN != actual_token.token
    assert f"{_TEST_TOKEN}:{iam_host}" == actual_token.token
    assert not actual_token.is_expired()


def test_aws_supported_regions_url_exists():
    url = "https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html"
    assert 200 == urllib.request.urlopen(url).getcode()
