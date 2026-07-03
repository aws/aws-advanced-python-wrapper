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

"""Unit tests for the async Limitless connect state machine.

The establish_connection cases mirror sync
``tests/unit/test_limitless_router_service.py`` one-for-one (wait-for-router-info
branches, cache/select paths, retry with least-loaded fallback, availability
marking, MaxRetriesExceeded). The plugin-level cases mirror sync
``tests/unit/test_limitless_plugin.py`` (dialect gate + FailedToConnectToHost).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.limitless_plugin import (
    AsyncLimitlessContext, AsyncLimitlessPlugin, AsyncLimitlessRouterCache,
    AsyncLimitlessRouterService)
from aws_advanced_python_wrapper.database_dialect import (AuroraPgDialect,
                                                          MysqlDatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

CLUSTER_ID: str = "some_cluster_id"


@pytest.fixture(autouse=True)
def _reset_limitless_singletons():
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()
    yield
    try:
        asyncio.run(AsyncLimitlessRouterService.stop_all())
    except RuntimeError:
        pass
    AsyncLimitlessRouterCache.clear()
    AsyncLimitlessRouterService._reset_for_tests()


# ----- fixtures ----------------------------------------------------


@pytest.fixture
def limitless_router1() -> HostInfo:
    return HostInfo("limitless-router-1", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_router2() -> HostInfo:
    return HostInfo("limitless-router-2", 5432, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_router3() -> HostInfo:
    return HostInfo("limitless-router-3", 5432, HostRole.READER, HostAvailability.UNAVAILABLE)


@pytest.fixture
def limitless_router4() -> HostInfo:
    return HostInfo("limitless-router-4", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def limitless_routers(limitless_router1, limitless_router2, limitless_router3, limitless_router4):
    return [limitless_router1, limitless_router2, limitless_router3, limitless_router4]


@pytest.fixture
def host_info() -> HostInfo:
    return HostInfo(host="host-info", role=HostRole.READER)


@pytest.fixture
def props() -> Properties:
    return Properties()


@pytest.fixture
def mock_conn() -> MagicMock:
    return MagicMock(name="mock_conn")


@pytest.fixture
def mock_plugin_service() -> MagicMock:
    svc = MagicMock(name="plugin_service")
    svc.host_list_provider = MagicMock()
    svc.host_list_provider.get_cluster_id.return_value = CLUSTER_ID
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.is_closed = AsyncMock(return_value=False)
    svc.driver_dialect.abort_connection = AsyncMock()
    svc.connect = AsyncMock()
    svc.get_host_info_by_strategy = MagicMock()
    svc.is_login_exception = MagicMock(return_value=False)
    svc.database_dialect = MagicMock()
    return svc


@pytest.fixture
def mock_query_helper() -> MagicMock:
    helper = MagicMock(name="query_helper")
    helper.query_for_limitless_routers = AsyncMock(return_value=[])
    return helper


@pytest.fixture
def connection_plugin() -> MagicMock:
    return MagicMock(name="connection_plugin")


def _context(host_info, props, connect_func, connection_plugin, plugin_service):
    return AsyncLimitlessContext(
        host_info, props, None, connect_func, [], connection_plugin, plugin_service)


# ----- establish_connection: wait-for-router-info branch -----------


def test_establish_connection_empty_routers_wait_then_raises(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin):
    mock_query_helper.query_for_limitless_routers = AsyncMock(return_value=[])
    connect_func = AsyncMock(return_value=mock_conn)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        with pytest.raises(AwsWrapperError) as exc:
            await service.establish_connection(context)
        assert str(exc.value) == Messages.get("LimitlessRouterService.NoRoutersAvailable")

    asyncio.run(_body())


def test_establish_connection_empty_routers_do_not_wait_calls_connect_func(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin):
    WrapperProperties.WAIT_FOR_ROUTER_INFO.set(props, False)
    connect_func = AsyncMock(return_value=mock_conn)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    connect_func.assert_awaited_once()


# ----- establish_connection: host already a router -----------------


def test_establish_connection_host_in_cache_calls_connect_func(
        mock_conn, mock_query_helper, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    connect_func = AsyncMock(return_value=mock_conn)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(limitless_router1, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    connect_func.assert_awaited_once()


def test_establish_connection_fetch_router_list_host_in_list_calls_connect_func(
        mock_conn, mock_query_helper, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    mock_query_helper.query_for_limitless_routers = AsyncMock(return_value=limitless_routers)
    connect_func = AsyncMock(return_value=mock_conn)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(limitless_router1, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert AsyncLimitlessRouterCache.get(CLUSTER_ID) == limitless_routers
    mock_query_helper.query_for_limitless_routers.assert_awaited_once()
    connect_func.assert_awaited_once()


# ----- establish_connection: select a router -----------------------


def test_establish_connection_cache_then_select_host(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect = AsyncMock(return_value=mock_conn)
    connect_func = AsyncMock(return_value=None)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert AsyncLimitlessRouterCache.get(CLUSTER_ID) == limitless_routers
    mock_plugin_service.get_host_info_by_strategy.assert_called_once_with(
        HostRole.WRITER, "weighted_random", limitless_routers)
    mock_plugin_service.connect.assert_awaited_once_with(
        limitless_router1, props, plugin_to_skip=connection_plugin)
    connect_func.assert_not_called()


def test_establish_connection_fetch_then_select_host(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    mock_query_helper.query_for_limitless_routers = AsyncMock(return_value=limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect = AsyncMock(return_value=mock_conn)
    connect_func = AsyncMock(return_value=None)
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert AsyncLimitlessRouterCache.get(CLUSTER_ID) == limitless_routers
    mock_query_helper.query_for_limitless_routers.assert_awaited_once()
    mock_plugin_service.get_host_info_by_strategy.assert_called_once_with(
        HostRole.WRITER, "weighted_random", limitless_routers)
    mock_plugin_service.connect.assert_awaited_once_with(
        limitless_router1, props, plugin_to_skip=connection_plugin)
    connect_func.assert_awaited_once()


# ----- establish_connection: retry with least-loaded fallback ------


def test_establish_connection_host_in_cache_connect_func_raises_then_retries(
        mock_conn, mock_query_helper, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect = AsyncMock(return_value=mock_conn)
    connect_func = AsyncMock(side_effect=Exception())
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(limitless_router1, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    mock_plugin_service.get_host_info_by_strategy.assert_called_once_with(
        HostRole.WRITER, "highest_weight", limitless_routers)
    mock_plugin_service.connect.assert_awaited_once_with(
        limitless_router1, props, plugin_to_skip=connection_plugin)
    connect_func.assert_awaited_once()


def test_establish_connection_selected_host_raises_then_retries(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [Exception(), limitless_router1]
    mock_plugin_service.connect = AsyncMock(return_value=mock_conn)
    connect_func = AsyncMock(side_effect=Exception())
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(
        HostRole.WRITER, "highest_weight", limitless_routers)
    mock_plugin_service.connect.assert_awaited_once_with(
        limitless_router1, props, plugin_to_skip=connection_plugin)


def test_establish_connection_selected_host_none_then_retries(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [None, limitless_router1]
    mock_plugin_service.connect = AsyncMock(return_value=mock_conn)
    connect_func = AsyncMock(side_effect=Exception())
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    mock_plugin_service.get_host_info_by_strategy.assert_called_with(
        HostRole.WRITER, "highest_weight", limitless_routers)
    mock_plugin_service.connect.assert_awaited_once_with(
        limitless_router1, props, plugin_to_skip=connection_plugin)


def test_establish_connection_service_connect_raises_then_retries(
        mock_conn, mock_query_helper, host_info, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_router2, limitless_routers):
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.side_effect = [limitless_router1, limitless_router2]
    mock_plugin_service.connect = AsyncMock(side_effect=[Exception(), mock_conn])
    connect_func = AsyncMock(side_effect=Exception())
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(host_info, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        await service.establish_connection(context)

    asyncio.run(_body())

    assert context.get_connection() is mock_conn
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 2
    assert mock_plugin_service.connect.await_count == 2
    # The first (weighted_random) pick failed to connect and was marked down.
    assert limitless_router1.get_availability() == HostAvailability.UNAVAILABLE
    mock_plugin_service.connect.assert_awaited_with(
        limitless_router2, props, plugin_to_skip=connection_plugin)


def test_establish_connection_max_retries_exceeded_raises(
        mock_conn, mock_query_helper, props, mock_plugin_service, connection_plugin,
        limitless_router1, limitless_routers):
    WrapperProperties.MAX_RETRIES_MS.set(props, 3)
    AsyncLimitlessRouterCache.put(CLUSTER_ID, limitless_routers)
    mock_plugin_service.get_host_info_by_strategy.return_value = limitless_router1
    mock_plugin_service.connect = AsyncMock(side_effect=Exception())
    connect_func = AsyncMock(side_effect=Exception())
    service = AsyncLimitlessRouterService(mock_plugin_service, mock_query_helper)
    context = _context(limitless_router1, props, connect_func, connection_plugin, mock_plugin_service)

    async def _body():
        with pytest.raises(AwsWrapperError) as exc:
            await service.establish_connection(context)
        assert str(exc.value) == Messages.get("LimitlessRouterService.MaxRetriesExceeded")

    asyncio.run(_body())

    assert mock_plugin_service.connect.await_count == 3
    assert mock_plugin_service.get_host_info_by_strategy.call_count == 3


# ----- plugin connect gate + FailedToConnectToHost -----------------


def _mock_service_for_plugin_gate():
    svc = MagicMock(name="plugin_service")
    svc.driver_dialect = MagicMock()
    svc.driver_dialect.is_closed = AsyncMock(return_value=False)
    return svc


def _mock_router_service():
    router_service = MagicMock(name="router_service")
    router_service.dispose_stale_monitors = AsyncMock()
    router_service.start_monitoring = MagicMock()
    return router_service


def test_plugin_connect_returns_established_connection(host_info, props, mock_conn):
    svc = _mock_service_for_plugin_gate()
    svc.database_dialect = AuroraPgDialect()
    plugin = AsyncLimitlessPlugin(svc, props)
    router_service = _mock_router_service()

    def _set_conn(context):
        context._connection = mock_conn
        return None

    router_service.establish_connection = AsyncMock(side_effect=_set_conn)
    plugin._limitless_router_service = router_service
    connect_func = AsyncMock(return_value=None)

    async def _body():
        return await plugin.connect(
            MagicMock(), MagicMock(), host_info, props, True, connect_func)

    result = asyncio.run(_body())

    assert result is mock_conn
    connect_func.assert_not_called()
    router_service.start_monitoring.assert_called_once_with(host_info, props)
    router_service.establish_connection.assert_awaited_once()


def test_plugin_connect_none_connection_raises(host_info, props, mock_conn):
    svc = _mock_service_for_plugin_gate()
    svc.database_dialect = AuroraPgDialect()
    plugin = AsyncLimitlessPlugin(svc, props)
    router_service = _mock_router_service()

    def _set_none(context):
        context._connection = None
        return None

    router_service.establish_connection = AsyncMock(side_effect=_set_none)
    plugin._limitless_router_service = router_service
    connect_func = AsyncMock(return_value=mock_conn)

    async def _body():
        with pytest.raises(AwsWrapperError) as exc:
            await plugin.connect(
                MagicMock(), MagicMock(), host_info, props, True, connect_func)
        assert str(exc.value) == Messages.get_formatted(
            "LimitlessPlugin.FailedToConnectToHost", host_info.host)

    asyncio.run(_body())

    router_service.start_monitoring.assert_called_once_with(host_info, props)
    router_service.establish_connection.assert_awaited_once()


def test_plugin_connect_unsupported_dialect_raises(host_info, props, mock_conn):
    svc = _mock_service_for_plugin_gate()
    unsupported = MysqlDatabaseDialect()
    svc.database_dialect = unsupported
    plugin = AsyncLimitlessPlugin(svc, props)
    plugin._limitless_router_service = _mock_router_service()
    connect_func = AsyncMock(return_value=mock_conn)

    async def _body():
        with pytest.raises(UnsupportedOperationError) as exc:
            await plugin.connect(
                MagicMock(), MagicMock(), host_info, props, True, connect_func)
        assert str(exc.value) == Messages.get_formatted(
            "LimitlessPlugin.UnsupportedDialectOrDatabase", type(unsupported).__name__)

    asyncio.run(_body())


def test_plugin_connect_supported_dialect_after_refresh(host_info, props, mock_conn):
    # First dialect read is unsupported, the re-read (refresh) is supported --
    # mirrors sync connect's refresh-then-recheck (limitless_plugin.py:83-89).
    class _RefreshingService:
        def __init__(self, dialects, driver_dialect):
            self._dialects = iter(dialects)
            self.driver_dialect = driver_dialect

        @property
        def database_dialect(self):
            return next(self._dialects)

    driver_dialect = MagicMock()
    driver_dialect.is_closed = AsyncMock(return_value=False)
    svc = _RefreshingService([MysqlDatabaseDialect(), AuroraPgDialect()], driver_dialect)

    plugin = AsyncLimitlessPlugin(svc, props)
    router_service = _mock_router_service()

    def _set_conn(context):
        context._connection = mock_conn
        return None

    router_service.establish_connection = AsyncMock(side_effect=_set_conn)
    plugin._limitless_router_service = router_service
    connect_func = AsyncMock(return_value=None)

    async def _body():
        return await plugin.connect(
            MagicMock(), MagicMock(), host_info, props, True, connect_func)

    result = asyncio.run(_body())

    assert result is mock_conn
    router_service.start_monitoring.assert_called_once_with(host_info, props)
    router_service.establish_connection.assert_awaited_once()
