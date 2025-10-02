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

from datetime import datetime, timedelta
from typing import Dict
from unittest.mock import patch

import pytest

from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.iam_plugin import TokenInfo
from aws_advanced_python_wrapper.okta_plugin import OktaAuthPlugin
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_token_utils import RDSTokenUtils

_GENERATED_TOKEN = "generated_token"
_TEST_TOKEN = "test_token"
_DEFAULT_PG_PORT = 5432
_PG_CACHE_KEY = f"us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:{_DEFAULT_PG_PORT}:postgresqlUser"
_DB_USER = "postgresqlUser"

_PG_HOST_INFO = HostInfo("pg.testdb.us-east-2.rds.amazonaws.com")

_token_cache: Dict[str, TokenInfo] = {}


@pytest.fixture(autouse=True)
def clear_cache():
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


@pytest.fixture
def mock_credentials_provider_factory(mocker):
    return mocker.MagicMock()


@pytest.fixture(autouse=True)
def mock_default_behavior(mock_session, mock_client, mock_func, mock_connection, mock_plugin_service, mock_dialect,
                          mock_credentials_provider_factory):
    mock_session.client.return_value = mock_client
    mock_client.generate_db_auth_token.return_value = _TEST_TOKEN
    mock_session.get_available_regions.return_value = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
    mock_func.return_value = mock_connection
    mock_plugin_service.driver_dialect = mock_dialect
    mock_plugin_service.database_dialect = mock_dialect
    mock_dialect.default_port = _DEFAULT_PG_PORT
    mock_credentials_provider_factory.get_aws_credentials.return_value = {"AccessKeyId": "test-access-key",
                                                                          "SecretAccessKey": "test-secret-access",
                                                                          "SessionToken": "test-session-token"}


@patch("aws_advanced_python_wrapper.okta_plugin.OktaAuthPlugin._token_cache", _token_cache)
def test_pg_connect_valid_token_in_cache(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect):
    properties: Properties = Properties()
    WrapperProperties.PLUGINS.set(properties, "okta")
    WrapperProperties.DB_USER.set(properties, _DB_USER)
    initial_token = TokenInfo(_TEST_TOKEN, datetime.now() + timedelta(minutes=5))
    _token_cache[_PG_CACHE_KEY] = initial_token

    target_plugin: OktaAuthPlugin = OktaAuthPlugin(mock_plugin_service, RDSTokenUtils(), mock_session)
    key = "us-east-2:pg.testdb.us-east-2.rds.amazonaws.com:" + str(_DEFAULT_PG_PORT) + ":postgesqlUser"
    _token_cache[key] = initial_token

    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=_PG_HOST_INFO,
        props=properties,
        is_initial_connection=False,
        connect_func=mock_func)

    mock_client.generate_db_auth_token.assert_not_called()

    actual_token = _token_cache.get(_PG_CACHE_KEY)
    assert _GENERATED_TOKEN != actual_token.token
    assert _TEST_TOKEN == actual_token.token
    assert actual_token.is_expired() is False


@patch("aws_advanced_python_wrapper.okta_plugin.OktaAuthPlugin._token_cache", _token_cache)
def test_expired_cached_token(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect, mock_credentials_provider_factory):
    test_props: Properties = Properties({"plugins": "okta", "user": "postgresqlUser", "idp_username": "user", "idp_password": "password"})
    WrapperProperties.DB_USER.set(test_props, _DB_USER)
    initial_token = TokenInfo(_TEST_TOKEN, datetime.now() - timedelta(minutes=5))
    _token_cache[_PG_CACHE_KEY] = initial_token

    target_plugin: OktaAuthPlugin = OktaAuthPlugin(mock_plugin_service,
                                                   mock_credentials_provider_factory,
                                                   RDSTokenUtils(),
                                                   mock_session)

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
    assert WrapperProperties.USER.get(test_props) == _DB_USER
    assert WrapperProperties.PASSWORD.get(test_props) == _TEST_TOKEN


@patch("aws_advanced_python_wrapper.okta_plugin.OktaAuthPlugin._token_cache", _token_cache)
def test_no_cached_token(mocker, mock_plugin_service, mock_session, mock_func, mock_client, mock_dialect, mock_credentials_provider_factory):
    test_props: Properties = Properties({"plugins": "okta", "user": "postgresqlUser", "idp_username": "user", "idp_password": "password"})
    WrapperProperties.DB_USER.set(test_props, _DB_USER)

    target_plugin: OktaAuthPlugin = OktaAuthPlugin(mock_plugin_service,
                                                   mock_credentials_provider_factory,
                                                   RDSTokenUtils(),
                                                   mock_session)

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
    assert WrapperProperties.USER.get(test_props) == _DB_USER
    assert WrapperProperties.PASSWORD.get(test_props) == _TEST_TOKEN


@patch("aws_advanced_python_wrapper.okta_plugin.OktaAuthPlugin._token_cache", _token_cache)
def test_no_cached_token_raises_exception(mocker, mock_plugin_service, mock_session, mock_func, mock_client,
                                          mock_dialect, mock_credentials_provider_factory):
    test_props: Properties = Properties({"plugins": "okta", "user": "postgresqlUser", "idp_username": "user", "idp_password": "password"})
    WrapperProperties.DB_USER.set(test_props, _DB_USER)

    exception_message = "generic exception"
    mock_func.side_effect = Exception(exception_message)

    target_plugin: OktaAuthPlugin = OktaAuthPlugin(mock_plugin_service,
                                                   mock_credentials_provider_factory,
                                                   RDSTokenUtils(),
                                                   mock_session)

    with pytest.raises(Exception) as e_info:
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

    assert e_info.type == Exception
    assert str(e_info.value) == exception_message


@patch("aws_advanced_python_wrapper.okta_plugin.OktaAuthPlugin._token_cache", _token_cache)
def test_connect_with_specified_iam_host_port_region(mocker,
                                                     mock_plugin_service,
                                                     mock_session,
                                                     mock_func,
                                                     mock_client,
                                                     mock_dialect,
                                                     mock_credentials_provider_factory):
    properties: Properties = Properties()
    WrapperProperties.PLUGINS.set(properties, "okta")
    WrapperProperties.DB_USER.set(properties, "specifiedUser")

    expected_host = "pg.testdb.us-west-2.rds.amazonaws.com"
    expected_port: str = "5555"
    expected_region = "us-west-2"
    WrapperProperties.IAM_HOST.set(properties, expected_host)
    WrapperProperties.IAM_DEFAULT_PORT.set(properties, expected_port)
    WrapperProperties.IAM_REGION.set(properties, expected_region)
    test_token_info = TokenInfo(_TEST_TOKEN, datetime.now() + timedelta(minutes=5))

    key = "us-west-2:pg.testdb.us-west-2.rds.amazonaws.com:" + str(expected_port) + ":specifiedUser"
    _token_cache[key] = test_token_info

    mock_client.generate_db_auth_token.return_value = f"{_TEST_TOKEN}:{expected_region}"

    target_plugin: OktaAuthPlugin = OktaAuthPlugin(mock_plugin_service,
                                                   mock_credentials_provider_factory,
                                                   RDSTokenUtils(),
                                                   mock_session)
    target_plugin.connect(
        target_driver_func=mocker.MagicMock(),
        driver_dialect=mock_dialect,
        host_info=HostInfo(expected_host),
        props=properties,
        is_initial_connection=False,
        connect_func=mock_func)

    assert WrapperProperties.USER.get(properties) == "specifiedUser"
    mock_dialect.set_password.assert_called_with(properties, _TEST_TOKEN)
