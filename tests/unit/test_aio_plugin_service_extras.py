from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.default_plugin import AsyncDefaultPlugin
from aws_advanced_python_wrapper.aio.driver_dialect.base import \
    AsyncDriverDialect
from aws_advanced_python_wrapper.aio.plugin_manager import AsyncPluginManager
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import Properties


def _make_service(database_dialect: Optional[DatabaseDialect] = None) -> AsyncPluginServiceImpl:
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    if database_dialect is not None:
        svc.database_dialect = database_dialect
    return svc


def test_database_dialect_defaults_to_none():
    svc = _make_service()
    assert svc.database_dialect is None


def test_database_dialect_is_settable():
    dialect = MagicMock(spec=DatabaseDialect)
    svc = _make_service(database_dialect=dialect)
    assert svc.database_dialect is dialect


def test_is_network_exception_returns_false_when_no_dialect():
    svc = _make_service()
    assert svc.is_network_exception(error=Exception("boom")) is False


def test_is_network_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_network_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("connection reset")
    assert svc.is_network_exception(error=err) is True
    handler.is_network_exception.assert_called_once_with(error=err, sql_state=None)


def test_is_login_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_login_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("auth failed")
    assert svc.is_login_exception(error=err, sql_state="28000") is True
    handler.is_login_exception.assert_called_once_with(error=err, sql_state="28000")


def test_get_availability_returns_none_when_unset():
    svc = _make_service()
    assert svc.get_availability("host-1.cluster.example") is None


def test_set_then_get_availability():
    svc = _make_service()
    svc.set_availability(frozenset({"host-1.cluster.example"}), HostAvailability.UNAVAILABLE)
    assert svc.get_availability("host-1.cluster.example") == HostAvailability.UNAVAILABLE


def test_set_availability_covers_all_aliases():
    svc = _make_service()
    aliases = frozenset({"h1.example", "h1-alias.example"})
    svc.set_availability(aliases, HostAvailability.UNAVAILABLE)
    for alias in aliases:
        assert svc.get_availability(alias) == HostAvailability.UNAVAILABLE


def test_default_plugin_accepts_random_strategy():
    plugin = AsyncDefaultPlugin()
    assert plugin.accepts_strategy(HostRole.READER, "random") is True


def test_default_plugin_rejects_unknown_strategy():
    plugin = AsyncDefaultPlugin()
    assert plugin.accepts_strategy(HostRole.READER, "bogus_strategy") is False


def test_default_plugin_get_host_info_by_strategy_returns_matching_role():
    plugin = AsyncDefaultPlugin()
    reader = HostInfo(host="reader-1", port=5432, role=HostRole.READER)
    writer = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    chosen = plugin.get_host_info_by_strategy(
        HostRole.READER, "random", [reader, writer]
    )
    assert chosen is reader


def test_plugin_service_delegates_strategy_through_manager():
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    # NOTE: 3-arg signature (svc, props, plugins). AsyncPluginManager auto-appends AsyncDefaultPlugin.
    manager = AsyncPluginManager(svc, Properties(), [])
    svc.plugin_manager = manager
    assert svc.accepts_strategy(HostRole.READER, "random") is True
    reader = HostInfo(host="reader-1", port=5432, role=HostRole.READER)
    got = svc.get_host_info_by_strategy(HostRole.READER, "random", [reader])
    assert got is reader


def test_accepts_strategy_returns_false_when_plugin_manager_unbound():
    svc = _make_service()
    assert svc.accepts_strategy(HostRole.READER, "random") is False


def test_get_host_info_by_strategy_returns_none_when_plugin_manager_unbound():
    svc = _make_service()
    assert svc.get_host_info_by_strategy(HostRole.READER, "random", None) is None


class _FakeHostListProvider:
    """Minimal AsyncHostListProvider stand-in for delegation tests."""

    def __init__(self):
        self.refresh_calls = 0
        self.force_refresh_calls = 0
        self._topology = (HostInfo(host="writer-1", port=5432, role=HostRole.WRITER),)

    async def refresh(self, connection):
        self.refresh_calls += 1
        return self._topology

    async def force_refresh(self, connection):
        self.force_refresh_calls += 1
        return self._topology


def test_host_list_provider_defaults_to_none():
    svc = _make_service()
    assert svc.host_list_provider is None


def test_host_list_provider_is_settable():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp
    assert svc.host_list_provider is hlp


def test_refresh_host_list_delegates_to_provider():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp
    asyncio.run(svc.refresh_host_list())
    assert svc.all_hosts == hlp._topology
    assert hlp.refresh_calls == 1


def test_force_refresh_host_list_delegates_to_provider():
    svc = _make_service()
    hlp = _FakeHostListProvider()
    svc.host_list_provider = hlp
    asyncio.run(svc.force_refresh_host_list())
    assert svc.all_hosts == hlp._topology
    assert hlp.force_refresh_calls == 1


def test_refresh_raises_when_no_provider():
    svc = _make_service()
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.refresh_host_list())


def test_force_refresh_raises_when_no_provider():
    svc = _make_service()
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.force_refresh_host_list())


def test_all_hosts_defaults_to_empty_tuple():
    svc = _make_service()
    assert svc.all_hosts == ()


def test_refresh_does_not_mutate_all_hosts_on_error():
    svc = _make_service()
    # No provider set; refresh raises. all_hosts must remain ().
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.refresh_host_list())
    assert svc.all_hosts == ()


def test_initial_connection_host_info_defaults_to_none():
    svc = _make_service()
    assert svc.initial_connection_host_info is None


def test_initial_connection_host_info_is_settable():
    svc = _make_service()
    host = HostInfo(host="writer-1", port=5432, role=HostRole.WRITER)
    svc.initial_connection_host_info = host
    assert svc.initial_connection_host_info is host


class _ReleasableHostListProvider(_FakeHostListProvider):
    """Extends the fake with an async release_resources hook."""

    def __init__(self):
        super().__init__()
        self.released = False

    async def release_resources(self):
        self.released = True


def test_release_resources_closes_connection_and_provider():
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()

    async def _abort(conn):
        conn.closed = True

    # AsyncMock so the await path inside release_resources actually
    # exercises the awaitable side_effect. MagicMock would call _abort
    # synchronously and return the raw coroutine; AsyncMock makes the
    # whole await happen end-to-end, matching the real driver shape.
    driver_dialect.abort_connection = AsyncMock(side_effect=_abort)
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    conn = MagicMock()
    conn.closed = False
    svc._current_connection = conn
    hlp = _ReleasableHostListProvider()
    svc.host_list_provider = hlp
    asyncio.run(svc.release_resources())
    assert hlp.released is True


def test_release_resources_survives_errors():
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    # AsyncMock so the error surfaces from the awaited coroutine
    # rather than synchronously at call time -- exercises the same
    # try/except path a real async driver would trigger.
    driver_dialect.abort_connection = AsyncMock(
        side_effect=RuntimeError("boom"))
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    conn = MagicMock()
    conn.closed = False
    svc._current_connection = conn
    # Must not raise
    asyncio.run(svc.release_resources())


def test_release_resources_is_noop_when_no_connection_no_provider():
    svc = _make_service()
    # Should not raise with neither connection nor provider
    asyncio.run(svc.release_resources())


def test_release_resources_skips_provider_without_hook():
    """A provider without async release_resources is silently skipped."""
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    driver_dialect.abort_connection = AsyncMock()  # Async noop
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    svc._current_connection = MagicMock()
    svc.host_list_provider = _FakeHostListProvider()  # No release_resources method
    # Must not raise; must not attempt to call release on a provider lacking the method
    asyncio.run(svc.release_resources())


def test_is_read_only_connection_exception_returns_false_when_no_dialect():
    svc = _make_service()
    assert svc.is_read_only_connection_exception(error=Exception("boom")) is False


def test_is_read_only_connection_exception_delegates_to_dialect_handler():
    dialect = MagicMock(spec=DatabaseDialect)
    handler = MagicMock()
    handler.is_read_only_connection_exception.return_value = True
    dialect.exception_handler = handler
    svc = _make_service(database_dialect=dialect)
    err = Exception("read-only connection")
    assert svc.is_read_only_connection_exception(error=err, sql_state="25006") is True
    handler.is_read_only_connection_exception.assert_called_once_with(
        error=err, sql_state="25006")


def test_plugin_service_get_host_role_delegates_to_dialect_utils():
    svc = _make_service()
    dialect = MagicMock(spec=DatabaseDialect)
    dialect.is_reader_query = "SELECT pg_is_in_recovery()"
    svc.database_dialect = dialect

    # Build a conn with an async cursor that returns (True,)
    cursor = MagicMock()
    cursor.execute = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(True,))
    cursor.close = MagicMock()
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cursor)
    svc._current_connection = conn

    role = asyncio.run(svc.get_host_role())
    assert role == HostRole.READER


def test_plugin_service_get_host_role_raises_without_dialect():
    svc = _make_service()
    svc._current_connection = MagicMock()
    # No database_dialect set
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.get_host_role())


def test_plugin_service_get_host_role_raises_without_connection():
    svc = _make_service()
    dialect = MagicMock(spec=DatabaseDialect)
    dialect.is_reader_query = "SELECT 1"
    svc.database_dialect = dialect

    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.get_host_role())


def test_get_telemetry_factory_returns_no_op_by_default():
    svc = _make_service()
    factory = svc.get_telemetry_factory()
    # Should be callable and return valid counter/gauge objects
    assert factory is not None
    counter = factory.create_counter("test.counter")
    # Counter may be None for strict no-op impl, or an object we can .inc()
    if counter is not None:
        counter.inc()


def test_get_telemetry_factory_is_memoized():
    svc = _make_service()
    f1 = svc.get_telemetry_factory()
    f2 = svc.get_telemetry_factory()
    assert f1 is f2


def test_set_telemetry_factory_overrides_default():
    from unittest.mock import MagicMock
    svc = _make_service()
    fake = MagicMock()
    svc.set_telemetry_factory(fake)
    assert svc.get_telemetry_factory() is fake


def test_plugin_service_connect_requires_plugin_manager():
    svc = _make_service()
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.connect(
            HostInfo(host="h", port=5432), Properties()))


def test_plugin_service_connect_requires_target_driver_func():
    svc = _make_service()
    svc.plugin_manager = MagicMock()
    with pytest.raises(AwsWrapperError):
        asyncio.run(svc.connect(
            HostInfo(host="h", port=5432), Properties()))


def test_plugin_service_connect_delegates_to_plugin_manager():
    svc = _make_service()
    manager = MagicMock()
    manager.connect = AsyncMock(return_value=MagicMock(name="conn"))
    svc.plugin_manager = manager
    svc.set_target_driver_func(lambda: None)

    host = HostInfo(host="h", port=5432)
    props = Properties()
    asyncio.run(svc.connect(host, props))

    manager.connect.assert_awaited_once()
    call = manager.connect.await_args
    assert call.kwargs["host_info"] is host
    assert call.kwargs["props"] is props
    assert call.kwargs["is_initial_connection"] is False


def test_set_and_get_status_round_trip():
    svc = _make_service()

    class _Fake:
        pass

    obj = _Fake()
    svc.set_status(_Fake, "k", obj)
    assert svc.get_status(_Fake, "k") is obj


def test_get_status_returns_none_for_missing_key():
    svc = _make_service()

    class _Fake:
        pass

    assert svc.get_status(_Fake, "nope") is None


def test_get_status_returns_none_on_type_mismatch():
    svc = _make_service()

    class _A:
        pass

    class _B:
        pass

    svc.set_status(_A, "k", _A())
    # Looking up under _B should return None, not raise
    assert svc.get_status(_B, "k") is None


def test_remove_status_drops_entry():
    svc = _make_service()

    class _Fake:
        pass

    svc.set_status(_Fake, "k", _Fake())
    svc.remove_status(_Fake, "k")
    assert svc.get_status(_Fake, "k") is None


def test_remove_status_is_noop_for_missing_key():
    svc = _make_service()

    class _Fake:
        pass

    # Must not raise
    svc.remove_status(_Fake, "nope")
