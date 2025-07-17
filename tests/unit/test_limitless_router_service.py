#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import psycopg
import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.limitless_plugin import (
    LimitlessContext, LimitlessPlugin, LimitlessRouterService)
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

CLUSTER_ID: str = "some_cluster_id"
EXPIRATION_NANO_SECONDS: int = 60 * 60 * 1_000_000_000


@pytest.fixture
def writer_host():
    return HostInfo("instance-0", 5432, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host1() -> HostInfo:
    return HostInfo("instance-1", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host2():
    return HostInfo("instance-2", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host3():
    return HostInfo("instance-3", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def default_hosts(writer_host, reader_host2, reader_host3, reader_host1):
    return [writer_host, reader_host1, reader_host2, reader_host3]


@pytest.fixture
def limitless_router1():
    return HostInfo("limitless-router-1", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_router2():
    return HostInfo("limitless-router-2", 5432, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_router3():
    return HostInfo("limitless-router-3", 5432, HostRole.READER, HostAvailability.UNAVAILABLE)


@pytest.fixture
def limitless_router4():
    return HostInfo("limitless-router-4", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_routers(limitless_router1, limitless_router2, limitless_router3, limitless_router4):
    return [limitless_router1, limitless_router2, limitless_router3, limitless_router4]


@pytest.fixture
def mock_driver_dialect(mocker):
    driver_dialect_mock = mocker.MagicMock()
    driver_dialect_mock.is_closed.return_value = False
    return driver_dialect_mock


@pytest.fixture
def mock_plugin_service(mocker, mock_driver_dialect, mock_conn, host_info, default_hosts):
    service_mock = mocker.MagicMock()
    service_mock.current_connection = mock_conn
    service_mock.current_host_info = host_info
    service_mock.hosts = default_hosts
    service_mock.host_list_provider = mocker.MagicMock()
    service_mock.host_list_provider.get_cluster_id.return_value = CLUSTER_ID

    type(service_mock).driver_dialect = mocker.PropertyMock(return_value=mock_driver_dialect)
    return service_mock


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_limitless_router_service(mocker):
    limitless_router_service_mock = mocker.MagicMock()
    return limitless_router_service_mock


@pytest.fixture
def mock_limitless_query_helper(mocker):
    limitless_query_helper_mock = mocker.MagicMock()
    return limitless_query_helper_mock


@pytest.fixture
def host_info():
    return HostInfo(host="host-info", role=HostRole.READER)


@pytest.fixture
def props():
    return Properties()


@pytest.fixture
def plugin(mock_plugin_service, props, mock_limitless_router_service):
    plugin = LimitlessPlugin(mock_plugin_service, props)
    plugin._limitless_router_service = mock_limitless_router_service
    return plugin


@pytest.fixture(autouse=True)
def run_before_and_after_tests(mock_limitless_router_service):
    # Before

    yield

    # After

    LimitlessRouterService._limitless_router_cache.clear()


def test_establish_connection_empty_routers_list_then_wait_for_router_info_then_raises_exception(mocker,
                                                                                                 mock_conn,
                                                                                                 mock_limitless_query_helper,
                                                                                                 host_info,
                                                                                                 props,
                                                                                                 mock_plugin_service):
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        mock_plugin_service
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)

    with pytest.raises(Exception) as e_info:
        limitless_router_service.establish_connection(input_context)

    assert e_info.type == AwsWrapperError
    assert str(e_info.value) == Messages.get("LimitlessRouterService.NoRoutersAvailable")


def test_establish_connection_empty_routers_list_do_not_wait_for_router_info_then_call_connection_function(mocker,
                                                                                                           mock_conn,
                                                                                                           mock_limitless_query_helper,
                                                                                                           host_info,
                                                                                                           props,
                                                                                                           mock_plugin_service):
    WrapperProperties.WAIT_FOR_ROUTER_INFO.set(props, False)
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        mock_plugin_service
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    mock_connect_func.assert_called_once()


def test_establish_connection_host_info_in_router_cache_then_call_connection_function(mocker,
                                                                                      mock_conn,
                                                                                      mock_limitless_query_helper,
                                                                                      limitless_router1,
                                                                                      props,
                                                                                      mock_plugin_service,
                                                                                      limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    input_context: LimitlessContext = LimitlessContext(
        limitless_router1,
        props,
        None,
        mock_connect_func,
        [],
        mock_plugin_service
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    mock_connect_func.assert_called_once()


def test_establish_connection_fetch_router_list_and_host_info_in_router_list_then_call_connection_function(mocker,
                                                                                                           mock_conn,
                                                                                                           mock_limitless_query_helper,
                                                                                                           host_info,
                                                                                                           props,
                                                                                                           mock_plugin_service,
                                                                                                           limitless_router1,
                                                                                                           limitless_routers):
    mock_limitless_query_helper.query_for_limitless_routers.return_value = limitless_routers
    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = mock_conn

    input_context: LimitlessContext = LimitlessContext(
        limitless_router1,
        props,
        None,
        mock_connect_func,
        [],
        mock_plugin_service
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    mock_limitless_query_helper.query_for_limitless_routers.assert_called_once()
    mock_connect_func.assert_called_once()


def test_establish_connection_router_cache_then_select_host(mocker,
                                                            mock_conn,
                                                            mock_limitless_query_helper,
                                                            host_info,
                                                            props,
                                                            mock_plugin_service,
                                                            plugin,
                                                            limitless_router1,
                                                            limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect.return_value = mock_conn

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = None

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    mock_plugin_service.get_host_info_by_strategy.assert_called_once()
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "weighted_random",
                                                                     limitless_routers)
    mock_plugin_service.connect.assert_called_once()
    mock_plugin_service.connect.assert_called_with(limitless_router1, props, plugin)
    mock_connect_func.assert_not_called()


def test_establish_connection_fetch_router_list_then_select_host(mocker,
                                                                 mock_conn,
                                                                 mock_limitless_query_helper,
                                                                 host_info,
                                                                 props,
                                                                 mock_plugin_service,
                                                                 plugin,
                                                                 limitless_router1,
                                                                 limitless_routers):
    mock_limitless_query_helper.query_for_limitless_routers.return_value = limitless_routers
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect.return_value = mock_conn

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.return_value = None

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    mock_limitless_query_helper.query_for_limitless_routers.assert_called_once()
    mock_plugin_service.get_host_info_by_strategy.assert_called_once()
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "weighted_random",
                                                                     limitless_routers)
    mock_plugin_service.connect.assert_called_once()
    mock_plugin_service.connect.assert_called_with(limitless_router1, props, plugin)
    mock_connect_func.assert_called_once()


def test_establish_connection_host_info_in_router_cache_can_call_connection_function_then_raises_exception_and_retries(
        mocker,
        mock_conn,
        mock_limitless_query_helper,
        props,
        mock_plugin_service,
        plugin,
        limitless_router1,
        limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect.return_value = mock_conn

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.side_effect = Exception()

    input_context: LimitlessContext = LimitlessContext(
        limitless_router1,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    mock_plugin_service.get_host_info_by_strategy.assert_called_once()
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.connect.assert_called_once()
    mock_plugin_service.connect.assert_called_with(limitless_router1, props, plugin)
    mock_connect_func.assert_called_once()


def test_establish_connection_selected_host_raises_exception_and_retries(mocker,
                                                                         mock_conn,
                                                                         mock_limitless_query_helper,
                                                                         host_info,
                                                                         props,
                                                                         mock_plugin_service,
                                                                         plugin,
                                                                         limitless_router1,
                                                                         limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [
        Exception(),
        limitless_router1
    ]
    mock_plugin_service.connect.return_value = mock_conn

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.side_effect = Exception()

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.connect.assert_called_once()
    mock_plugin_service.connect.assert_called_with(limitless_router1, props, plugin)


def test_establish_connection_selected_host_none_then_retry(mocker,
                                                            mock_conn,
                                                            mock_limitless_query_helper,
                                                            host_info,
                                                            props,
                                                            mock_plugin_service,
                                                            plugin,
                                                            limitless_router1,
                                                            limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [
        None,
        limitless_router1
    ]
    mock_plugin_service.connect.return_value = mock_conn

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.side_effect = Exception()

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.connect.assert_called_once()
    mock_plugin_service.connect.assert_called_with(limitless_router1, props, plugin)


def test_establish_connection_plugin_service_connect_raises_exception_then_retry(mocker,
                                                                                 mock_conn,
                                                                                 mock_limitless_query_helper,
                                                                                 host_info,
                                                                                 props,
                                                                                 mock_plugin_service,
                                                                                 plugin,
                                                                                 limitless_router1,
                                                                                 limitless_router2,
                                                                                 limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [
        limitless_router1,
        limitless_router2
    ]
    mock_plugin_service.connect.side_effect = [
        Exception(),
        mock_conn
    ]

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.side_effect = Exception()

    input_context: LimitlessContext = LimitlessContext(
        host_info,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    limitless_router_service.establish_connection(input_context)

    assert mock_conn == input_context.get_connection()
    assert limitless_routers == LimitlessRouterService._limitless_router_cache.get(CLUSTER_ID)
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(HostRole.WRITER, "highest_weight",
                                                                     limitless_routers)
    assert mock_plugin_service.connect.call_count == 2
    mock_plugin_service.connect.assert_called_with(limitless_router2, props, plugin)


def test_establish_connection_retry_and_max_retries_exceeded_then_raise_exception(mocker,
                                                                                  mock_conn,
                                                                                  mock_limitless_query_helper,
                                                                                  host_info,
                                                                                  props,
                                                                                  mock_plugin_service,
                                                                                  plugin,
                                                                                  limitless_router1,
                                                                                  limitless_routers):
    LimitlessRouterService._limitless_router_cache.compute_if_absent(CLUSTER_ID, lambda _: limitless_routers,
                                                                     EXPIRATION_NANO_SECONDS)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect.side_effect = Exception()

    mock_connect_func = mocker.MagicMock()
    mock_connect_func.side_effect = Exception()

    input_context: LimitlessContext = LimitlessContext(
        limitless_router1,
        props,
        None,
        mock_connect_func,
        [],
        plugin
    )

    limitless_router_service: LimitlessRouterService = LimitlessRouterService(mock_plugin_service,
                                                                              mock_limitless_query_helper)
    with pytest.raises(Exception) as e_info:
        limitless_router_service.establish_connection(input_context)

    assert e_info.type == AwsWrapperError
    assert str(e_info.value) == Messages.get("LimitlessRouterService.MaxRetriesExceeded")
    assert mock_plugin_service.connect.call_count == WrapperProperties.MAX_RETRIES_MS.get(props)
    assert mock_plugin_service.get_host_info_by_strategy.call_count == WrapperProperties.MAX_RETRIES_MS.get(props)
