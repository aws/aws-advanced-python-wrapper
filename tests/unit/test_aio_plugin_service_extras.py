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


def test_set_availability_marks_only_the_named_host():
    """The mark is keyed by alias, so marking one host must leave the other
    hosts in the topology selectable."""
    svc = _make_service()
    writer = HostInfo(host="instance-1.abc.us-east-2.rds.amazonaws.com",
                      port=3306, role=HostRole.WRITER)
    reader = HostInfo(host="instance-2.abc.us-east-2.rds.amazonaws.com",
                      port=3306, role=HostRole.READER)
    svc._all_hosts = (writer, reader)

    svc.set_availability(writer.all_aliases, HostAvailability.UNAVAILABLE)

    for alias in writer.all_aliases:
        assert svc.get_availability(alias) == HostAvailability.UNAVAILABLE
    for alias in reader.all_aliases:
        assert svc.get_availability(alias) is None


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
    svc.set_status(_Fake, obj, "k")
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

    svc.set_status(_A, _A(), "k")
    # Looking up under _B should return None, not raise
    assert svc.get_status(_B, "k") is None


def test_remove_status_drops_entry():
    svc = _make_service()

    class _Fake:
        pass

    svc.set_status(_Fake, _Fake(), "k")
    svc.remove_status(_Fake, "k")
    assert svc.get_status(_Fake, "k") is None


def test_remove_status_is_noop_for_missing_key():
    svc = _make_service()

    class _Fake:
        pass

    # Must not raise
    svc.remove_status(_Fake, "nope")


# ---- current_host_info fallback chain (sync parity) -------------------


def test_current_host_info_returns_set_value():
    svc = _make_service()
    h = HostInfo("h", 5432)
    svc._current_host_info = h
    assert svc.current_host_info is h


def test_current_host_info_falls_back_to_initial_connection_host():
    svc = _make_service()
    h = HostInfo("init", 5432)
    svc.initial_connection_host_info = h
    assert svc.current_host_info is h


def test_current_host_info_none_when_no_hosts():
    svc = _make_service()
    assert svc.current_host_info is None


def test_current_host_info_picks_writer_from_topology():
    svc = _make_service()
    writer = HostInfo("w", 5432, HostRole.WRITER)
    reader = HostInfo("r", 5432, HostRole.READER)
    svc._all_hosts = (reader, writer)
    assert svc.current_host_info is writer


def test_current_host_info_falls_back_to_first_allowed_when_no_writer():
    svc = _make_service()
    reader = HostInfo("r", 5432, HostRole.READER)
    svc._all_hosts = (reader,)
    assert svc.current_host_info is reader


def test_current_host_info_raises_when_writer_not_allowed():
    from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
        AllowedAndBlockedHosts
    svc = _make_service()
    writer = HostInfo("w-writer", 5432, HostRole.WRITER, host_id="w-writer")
    svc._all_hosts = (writer,)
    # Restrict allowed hosts to exclude the writer's instance id.
    svc.allowed_and_blocked_hosts = AllowedAndBlockedHosts({"other-instance"}, None)
    with pytest.raises(AwsWrapperError, match="not in the list of allowed"):
        _ = svc.current_host_info


# ---- fill_aliases (sync parity) --------------------------------------


def _alias_cursor(rows):
    cur = MagicMock()
    cur.__aenter__ = AsyncMock(return_value=cur)
    cur.__aexit__ = AsyncMock(return_value=None)
    cur.execute = AsyncMock()
    cur.fetchall = AsyncMock(return_value=rows)
    return cur


def test_fill_aliases_adds_self_query_and_identify_aliases():
    async def _body():
        svc = _make_service()
        dialect = MagicMock(spec=DatabaseDialect)
        dialect.host_alias_query = "SELECT CONCAT(...)"
        svc.database_dialect = dialect

        conn = MagicMock()
        conn.cursor = MagicMock(return_value=_alias_cursor([("db-alias",)]))

        identified = HostInfo("h", 5432)
        identified.add_alias("id-alias")
        svc.identify_connection = AsyncMock(return_value=identified)

        host = HostInfo("h", 5432)
        await svc.fill_aliases(conn, host)

        assert "h:5432" in host.aliases       # host:port self-alias
        assert "db-alias" in host.aliases     # from host_alias_query
        assert "id-alias" in host.aliases     # from identify_connection

    asyncio.run(_body())


def test_fill_aliases_noop_when_already_has_aliases():
    async def _body():
        svc = _make_service()
        host = HostInfo("h", 5432)
        host.add_alias("existing")
        conn = MagicMock()
        await svc.fill_aliases(conn, host)
        conn.cursor.assert_not_called()

    asyncio.run(_body())


def test_fill_aliases_noop_when_no_connection():
    async def _body():
        svc = _make_service()
        host = HostInfo("h", 5432)
        await svc.fill_aliases(None, host)
        assert len(host.aliases) == 0

    asyncio.run(_body())


# ---- get_host_role default timeout (sync parity) ---------------------


def test_get_host_role_defaults_timeout_to_auxiliary_query_timeout(monkeypatch):
    from aws_advanced_python_wrapper.aio.dialect_utils import AsyncDialectUtils
    captured = {}

    async def _fake(conn, driver_dialect, query, timeout_sec):
        captured["timeout"] = timeout_sec
        return HostRole.WRITER

    monkeypatch.setattr(AsyncDialectUtils, "get_host_role", _fake)

    async def _body():
        driver_dialect = MagicMock(spec=AsyncDriverDialect)
        driver_dialect.network_bound_methods = set()
        svc = AsyncPluginServiceImpl(
            Properties({"auxiliary_query_timeout_sec": "12"}), driver_dialect)
        dialect = MagicMock(spec=DatabaseDialect)
        dialect.is_reader_query = "SELECT is_reader"
        svc.database_dialect = dialect
        svc._current_connection = MagicMock()

        role = await svc.get_host_role()
        assert role == HostRole.WRITER
        assert captured["timeout"] == 12.0

        # An explicit timeout still wins.
        await svc.get_host_role(timeout_sec=3.5)
        assert captured["timeout"] == 3.5

    asyncio.run(_body())


# ---- default plugin post-connect bookkeeping (sync default_plugin.py:80-82)


def _bookkeeping_service_and_provider(connect_result=None, connect_error=None):
    svc = MagicMock()
    provider = MagicMock()
    if connect_error is not None:
        provider.connect = AsyncMock(side_effect=connect_error)
    else:
        provider.connect = AsyncMock(
            return_value=connect_result if connect_result is not None else MagicMock(name="conn"))
    manager = svc.get_connection_provider_manager.return_value
    manager.get_connection_provider.return_value = provider
    manager.default_provider = provider
    # The terminal hook now awaits the database-dialect upgrade (sync parity
    # with default_plugin.py:82); a bare MagicMock is not awaitable.
    svc.update_database_dialect = AsyncMock()
    return svc, provider


def test_default_plugin_connect_marks_host_available_and_updates_driver_dialect():
    conn = MagicMock(name="conn")
    svc, provider = _bookkeeping_service_and_provider(connect_result=conn)
    plugin = AsyncDefaultPlugin(svc)
    host = HostInfo("writer-1.cluster.rds", 5432)

    async def _body():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=host,
            props=Properties(),
            is_initial_connection=True,
            connect_func=AsyncMock(),
        )

    result = asyncio.run(_body())
    assert result is conn
    svc.set_availability.assert_called_once_with(
        host.as_aliases(), HostAvailability.AVAILABLE)
    svc.update_driver_dialect.assert_called_once_with(provider)


def test_default_plugin_force_connect_marks_host_available():
    svc, provider = _bookkeeping_service_and_provider()
    plugin = AsyncDefaultPlugin(svc)
    host = HostInfo("writer-1.cluster.rds", 5432)

    async def _body():
        return await plugin.force_connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=host,
            props=Properties(),
            is_initial_connection=False,
            force_connect_func=AsyncMock(),
        )

    asyncio.run(_body())
    svc.set_availability.assert_called_once_with(
        host.as_aliases(), HostAvailability.AVAILABLE)
    svc.update_driver_dialect.assert_called_once_with(provider)


def test_default_plugin_connect_raises_without_plugin_service():
    """Review feedback on #1257: a missing service means broken wiring --
    the terminal hook must fail fast, not silently bypass the provider
    manager via a direct driver-dialect connect."""
    plugin = AsyncDefaultPlugin()

    async def _body():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=HostInfo("writer-1.cluster.rds", 5432),
            props=Properties(),
            is_initial_connection=True,
            connect_func=AsyncMock(),
        )

    with pytest.raises(AwsWrapperError, match="no AsyncPluginService"):
        asyncio.run(_body())


def test_default_plugin_connect_raises_without_database_dialect():
    """The wrapper seeds an initial dialect guess before the pipeline runs;
    None here means broken wiring and must raise, not fall back."""
    svc, _provider = _bookkeeping_service_and_provider()
    svc.database_dialect = None
    plugin = AsyncDefaultPlugin(svc)

    async def _body():
        return await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=HostInfo("writer-1.cluster.rds", 5432),
            props=Properties(),
            is_initial_connection=True,
            connect_func=AsyncMock(),
        )

    with pytest.raises(AwsWrapperError, match="database dialect"):
        asyncio.run(_body())


def test_default_plugin_force_connect_raises_without_plugin_service():
    plugin = AsyncDefaultPlugin()

    async def _body():
        return await plugin.force_connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=HostInfo("writer-1.cluster.rds", 5432),
            props=Properties(),
            is_initial_connection=False,
            force_connect_func=AsyncMock(),
        )

    with pytest.raises(AwsWrapperError, match="no AsyncPluginService"):
        asyncio.run(_body())


def test_default_plugin_connect_failure_skips_bookkeeping():
    svc, _provider = _bookkeeping_service_and_provider(
        connect_error=RuntimeError("connect blew up"))
    plugin = AsyncDefaultPlugin(svc)

    async def _body():
        await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=HostInfo("writer-1.cluster.rds", 5432),
            props=Properties(),
            is_initial_connection=True,
            connect_func=AsyncMock(),
        )

    with pytest.raises(RuntimeError, match="connect blew up"):
        asyncio.run(_body())
    svc.set_availability.assert_not_called()
    svc.update_driver_dialect.assert_not_called()


# ---- default plugin strategy uses the FILTERED hosts view --------------
# (sync default_plugin.py:136 reads plugin_service.hosts, not all_hosts)


def _service_with_filtered_topology():
    from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
        AllowedAndBlockedHosts
    driver_dialect = MagicMock(spec=AsyncDriverDialect)
    driver_dialect.network_bound_methods = set()
    svc = AsyncPluginServiceImpl(Properties(), driver_dialect)
    allowed_reader = HostInfo(
        "r1.cluster.rds", 5432, HostRole.READER, host_id="r1")
    blocked_reader = HostInfo(
        "r2.cluster.rds", 5432, HostRole.READER, host_id="r2")
    svc._all_hosts = (allowed_reader, blocked_reader)
    svc.allowed_and_blocked_hosts = AllowedAndBlockedHosts({"r1"}, None)
    return svc, allowed_reader, blocked_reader


def test_default_plugin_strategy_uses_filtered_hosts():
    svc, allowed_reader, _blocked = _service_with_filtered_topology()
    plugin = AsyncDefaultPlugin(svc)
    # random over the filtered view can only ever return the allowed reader.
    for _ in range(20):
        assert plugin.get_host_info_by_strategy(
            HostRole.READER, "random") is allowed_reader


def test_default_plugin_strategy_explicit_host_list_overrides_filter():
    svc, _allowed, blocked_reader = _service_with_filtered_topology()
    plugin = AsyncDefaultPlugin(svc)
    # An explicit host_list wins over the service's filtered view (parity
    # with the async signature's host_list override).
    chosen = plugin.get_host_info_by_strategy(
        HostRole.READER, "random", [blocked_reader])
    assert chosen is blocked_reader


def test_default_plugin_strategy_raises_when_filter_empties_topology():
    from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
        AllowedAndBlockedHosts
    svc, _allowed, _blocked = _service_with_filtered_topology()
    svc.allowed_and_blocked_hosts = AllowedAndBlockedHosts({"none-match"}, None)
    plugin = AsyncDefaultPlugin(svc)
    with pytest.raises(AwsWrapperError):
        plugin.get_host_info_by_strategy(HostRole.READER, "random")


# ---- force_monitoring_refresh_host_list (sync v2 parity) -----------------


def test_force_monitoring_refresh_host_list_adopts_monitor_topology():
    """Monitor-driven refresh adopts the published topology into all_hosts and
    returns True (sync PluginService.force_monitoring_refresh_host_list)."""
    async def _body() -> None:
        svc = _make_service()
        writer = HostInfo(host="w-new", port=5432, role=HostRole.WRITER)
        reader = HostInfo(host="r1", port=5432, role=HostRole.READER)
        provider = MagicMock()
        provider.force_monitoring_refresh = AsyncMock(
            return_value=(writer, reader))
        svc.host_list_provider = provider

        ok = await svc.force_monitoring_refresh_host_list(True, 5.0)
        assert ok is True
        provider.force_monitoring_refresh.assert_awaited_once_with(True, 5.0)
        assert svc.all_hosts == (writer, reader)

    asyncio.run(_body())


def test_force_monitoring_refresh_host_list_returns_false_on_timeout():
    """An empty monitor result (deadline expired with no publication) reports
    False so failover can react -- mirrors sync failover_v2_plugin.py:333-335."""
    async def _body() -> None:
        svc = _make_service()
        provider = MagicMock()
        provider.force_monitoring_refresh = AsyncMock(return_value=())
        svc.host_list_provider = provider

        ok = await svc.force_monitoring_refresh_host_list(True, 0.1)
        assert ok is False

    asyncio.run(_body())


def test_force_monitoring_refresh_host_list_falls_back_without_monitor():
    """Providers without force_monitoring_refresh (static dialects / test
    doubles) fall back to a plain forced refresh."""
    async def _body() -> None:
        svc = _make_service()
        writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
        provider = MagicMock(spec=["refresh", "force_refresh"])
        provider.force_refresh = AsyncMock(return_value=(writer,))
        svc.host_list_provider = provider

        ok = await svc.force_monitoring_refresh_host_list(True, 5.0)
        assert ok is True
        assert svc.all_hosts == (writer,)

    asyncio.run(_body())


def test_dialect_upgrade_runs_inside_terminal_hook_before_outer_post_connect():
    """Sync-parity ordering regression (default_plugin.py:82): the database-
    dialect upgrade must run INSIDE the terminal plugin's connect, so OUTER
    plugins' post-connect logic (e.g. the connection tracker's writer pin, an
    eager topology refresh) already sees the corrected dialect. Previously the
    upgrade ran only after the whole pipeline (aio/wrapper.py), leaving hooks
    on the pattern-guessed dialect -- the root asymmetry behind the MySQL
    tracker-pin and exception-classification defenses."""
    conn = MagicMock(name="conn")
    svc, provider = _bookkeeping_service_and_provider(connect_result=conn)
    plugin = AsyncDefaultPlugin(svc)
    host = HostInfo("writer-1.cluster.rds", 3306)

    order = []
    svc.update_database_dialect = AsyncMock(
        side_effect=lambda *_a, **_k: order.append("upgrade"))

    async def _outer_post_connect_probe():
        # Simulates an outer plugin: its post-connect code runs after the
        # terminal returns. By then the upgrade must have happened.
        result = await plugin.connect(
            target_driver_func=MagicMock(),
            driver_dialect=MagicMock(),
            host_info=host,
            props=Properties(),
            is_initial_connection=True,
            connect_func=AsyncMock(),
        )
        order.append("outer-post-connect")
        return result

    asyncio.run(_outer_post_connect_probe())
    svc.update_database_dialect.assert_awaited_once_with(conn)
    assert order == ["upgrade", "outer-post-connect"]


def _async_conn_answering(responses, default=()):
    """A connection whose cursor answers each verification query with its own rows.

    Keys are substrings matched against the query being executed. One cursor
    serves every rule, so a single return value cannot express
    "the global tables exist but there is only one region".
    """
    state = {"query": ""}

    def rows():
        for fragment, response in responses.items():
            if fragment in state["query"]:
                return list(response)
        return list(default)

    async def execute(query, *args, **kwargs):
        state["query"] = query

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(side_effect=execute)
    cursor.fetchall = AsyncMock(side_effect=lambda: rows())
    cursor.fetchone = AsyncMock(side_effect=lambda: next(iter(rows()), None))
    cursor.close = MagicMock()

    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)
    return conn


def _manager_guessing(dialect, dialect_code):
    """A dialect manager mid-resolution: it has a guess that live verification
    may still replace, which is the state the wrapper hands over."""
    from aws_advanced_python_wrapper.database_dialect import \
        DatabaseDialectManager

    manager = DatabaseDialectManager(Properties())
    manager._dialect = dialect
    manager._dialect_code = dialect_code
    manager._can_update = True
    manager._confirmed = False
    return manager


def _service_with_manager(dialect, dialect_code, props=None):
    props = props if props is not None else Properties()
    dd = MagicMock()
    dd._driver_name = "aiomysql"
    dd.is_in_transaction = AsyncMock(return_value=False)
    svc = AsyncPluginServiceImpl(props, dd)
    svc.database_dialect = dialect
    svc.database_dialect_manager = _manager_guessing(dialect, dialect_code)
    return svc


def test_update_database_dialect_confirms_when_no_manager():
    """With no manager there is no candidate chain to walk, so the dialect
    settles rather than being retried on every connect forever. A confirmed
    dialect is never re-verified."""
    dd = MagicMock()
    dd._driver_name = "psycopg"
    svc = AsyncPluginServiceImpl(Properties(), dd)
    conn = MagicMock(name="conn")

    async def _run():
        assert svc.is_dialect_confirmed is False
        await svc.update_database_dialect(conn)
        assert svc.is_dialect_confirmed is True
        conn.cursor.assert_not_called()
        # Second call is a fast no-op.
        await svc.update_database_dialect(conn)
        conn.cursor.assert_not_called()

    asyncio.run(_run())


def test_a_failed_candidate_settles_on_the_pattern_guess():
    """A candidate that could not be asked is a candidate that answered no.

    Deliberate trade-off, and a reversal: this used to leave the dialect
    unconfirmed so the next connect would retry. But a rule error is
    indistinguishable from "this engine has no such object", which on Aurora
    is the normal answer for the Multi-AZ rule -- so that reading left every
    Aurora cluster endpoint permanently unconfirmed with no topology. Sync
    and the reference both swallow the error and settle. The cost is that a
    genuinely dead connection now confirms the guess and caches it.
    """
    from aws_advanced_python_wrapper.database_dialect import (DialectCode,
                                                              RdsMysqlDialect)

    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL)
    conn = MagicMock(name="conn")
    conn.cursor.side_effect = RuntimeError("connection went away")

    async def _run():
        await svc.update_database_dialect(conn)
        assert svc.is_dialect_confirmed is True
        assert isinstance(svc.database_dialect, RdsMysqlDialect)

    asyncio.run(_run())


def test_update_database_dialect_rebuilds_provider_on_dialect_change():
    """An Aurora MySQL instance endpoint is guessed as RdsMysqlDialect, whose
    supplier is the static provider. Once verification upgrades the dialect the
    provider must be REPLACED with the Aurora one -- reconfiguring is not
    enough, the provider type itself is dialect specific."""
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncAuroraHostListProvider, AsyncStaticHostListProvider)
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, DialectCode, RdsMysqlDialect)

    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com", "port": "3306"})
    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL, props)
    svc.host_list_provider = AsyncStaticHostListProvider(props)

    # aurora_version present, but not a global database: Aurora MySQL.
    conn = _async_conn_answering({
        "aurora_version": [("aurora_version", "3.0")],
        "AURORA_GLOBAL_DB_STATUS": [],
    })

    async def _run():
        await svc.update_database_dialect(conn)
        assert isinstance(svc.database_dialect, AuroraMysqlDialect)
        assert svc.is_dialect_confirmed is True
        provider = svc.host_list_provider
        assert isinstance(provider, AsyncAuroraHostListProvider)
        # The replacement carries the MySQL topology SQL and port, not the
        # provider ctor's PostgreSQL defaults.
        assert provider._topology_query == AuroraMysqlDialect().topology_query
        assert provider._default_port == 3306

    asyncio.run(_run())


def test_verified_dialect_is_cached_under_the_url_the_next_connect_reads():
    """The endpoint cache is what keeps the next connect from re-running the
    whole candidate chain, and what lets it start out already confirmed.
    ``DatabaseDialectManager.get_dialect`` keys it by
    ``PropertiesUtils.get_url``, so confirming under ``HostInfo.url`` -- the
    same endpoint plus a trailing slash -- writes a key nothing ever reads."""
    from aws_advanced_python_wrapper.database_dialect import (
        DatabaseDialectManager, DialectCode, RdsMysqlDialect)
    from aws_advanced_python_wrapper.utils.properties import PropertiesUtils

    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com", "port": "3306"})
    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL, props)
    svc.initial_connection_host_info = HostInfo(host=props["host"], port=3306)
    svc.force_refresh_host_list = AsyncMock()

    conn = _async_conn_answering({
        "aurora_version": [("aurora_version", "3.0")],
        "AURORA_GLOBAL_DB_STATUS": [],
    })

    async def _run():
        await svc.update_database_dialect(conn)

    DatabaseDialectManager._known_endpoint_dialects.clear()
    try:
        asyncio.run(_run())
        assert DatabaseDialectManager._known_endpoint_dialects.get(
            PropertiesUtils.get_url(props)) is DialectCode.AURORA_MYSQL
    finally:
        DatabaseDialectManager._known_endpoint_dialects.clear()


def test_update_database_dialect_reaches_global_aurora_mysql():
    """The whole candidate chain is walked, not one hardcoded query: an
    Aurora MySQL cluster-endpoint guess must upgrade to Global Aurora MySQL
    on a multi-region cluster, since that dialect carries the region-aware
    topology SQL and the region-filtering host list provider."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, DialectCode, GlobalAuroraMysqlDialect)

    svc = _service_with_manager(AuroraMysqlDialect(), DialectCode.AURORA_MYSQL)
    # Both global views present, and more than one region.
    conn = _async_conn_answering({
        "AURORA_GLOBAL_DB_STATUS": [(1,)],
        "AURORA_GLOBAL_DB_INSTANCE_STATUS": [(1,)],
        "aurora_global_db_status": [(2,)],
    })

    async def _run():
        await svc.update_database_dialect(conn)
        assert isinstance(svc.database_dialect, GlobalAuroraMysqlDialect)
        assert svc.is_dialect_confirmed is True

    asyncio.run(_run())


def test_update_database_dialect_stops_at_first_matching_candidate():
    """Candidate order decides the answer, and the first match wins.

    RdsMysqlDialect lists AURORA_MYSQL before GLOBAL_AURORA_MYSQL, and the
    Aurora rules match a global cluster too -- so an RDS MySQL *instance*
    endpoint on a global cluster resolves to AuroraMysqlDialect and the
    walk never reaches the global candidate. This pins sync's behaviour
    (database_dialect.py:412), not a preference: changing it means
    reordering the shared candidate tuple for both engines."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, DialectCode, GlobalAuroraMysqlDialect,
        RdsMysqlDialect)

    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL)
    # A genuine multi-region global cluster.
    conn = _async_conn_answering({
        "aurora_version": [("aurora_version", "3.0")],
        "AURORA_GLOBAL_DB_STATUS": [(1,)],
        "AURORA_GLOBAL_DB_INSTANCE_STATUS": [(1,)],
        "aurora_global_db_status": [(2,)],
    })

    async def _run():
        await svc.update_database_dialect(conn)
        assert isinstance(svc.database_dialect, AuroraMysqlDialect)
        assert not isinstance(svc.database_dialect, GlobalAuroraMysqlDialect)

    asyncio.run(_run())


def test_update_database_dialect_single_region_stops_at_aurora():
    """A single-region Aurora cluster has the global views but only one
    region, so it must NOT be taken for a global database."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, DialectCode, GlobalAuroraMysqlDialect,
        RdsMysqlDialect)

    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL)
    conn = _async_conn_answering({
        "aurora_version": [("aurora_version", "3.0")],
        "AURORA_GLOBAL_DB_STATUS": [(1,)],
        "AURORA_GLOBAL_DB_INSTANCE_STATUS": [(1,)],
        "aurora_global_db_status": [(1,)],
    })

    async def _run():
        await svc.update_database_dialect(conn)
        assert isinstance(svc.database_dialect, AuroraMysqlDialect)
        assert not isinstance(svc.database_dialect, GlobalAuroraMysqlDialect)

    asyncio.run(_run())


def test_update_database_dialect_verifies_pg_candidates():
    """PG is verified too, not just MySQL: an Aurora PG instance endpoint is
    guessed as RdsPgDialect, whose supplier has no topology query at all."""
    from aws_advanced_python_wrapper.database_dialect import (AuroraPgDialect,
                                                              DialectCode,
                                                              RdsPgDialect)

    svc = _service_with_manager(RdsPgDialect(), DialectCode.RDS_PG)
    svc.driver_dialect._driver_name = "psycopg"
    conn = _async_conn_answering({
        "pg_proc": [(1,)],
        "aurora_stat_utils": [(True,)],
        "aurora_replica_status": [(1,)],
        "aurora_global_db_status": [],
    })

    async def _run():
        await svc.update_database_dialect(conn)
        assert isinstance(svc.database_dialect, AuroraPgDialect)
        assert svc.is_dialect_confirmed is True

    asyncio.run(_run())


def test_update_database_dialect_verification_timeout_leaves_unconfirmed():
    """Verification that hangs must not hang the connect. The timeout comes from
    AUXILIARY_QUERY_TIMEOUT_SEC, and the resulting QueryTimeoutError leaves
    the dialect unconfirmed for the next connect to retry."""
    from aws_advanced_python_wrapper.database_dialect import (DialectCode,
                                                              RdsMysqlDialect)

    props = Properties({"auxiliary_query_timeout_sec": "0.05"})
    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL, props)

    async def _never_returns(*args, **kwargs):
        await asyncio.sleep(30)

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(side_effect=_never_returns)
    cursor.close = MagicMock()
    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)

    async def _run():
        await svc.update_database_dialect(conn)
        assert svc.is_dialect_confirmed is False
        assert isinstance(svc.database_dialect, RdsMysqlDialect)

    asyncio.run(_run())


def test_provider_identity_covers_every_dialect_supplied_argument():
    """Keeps _PROVIDER_DIALECT_ATTRS honest.

    Provider equivalence is decided by comparing a fixed list of
    attributes. If a supplier in aio/host_list_provider gains a new
    dialect-derived argument and the list is not extended, two providers
    differing only in that argument would compare equal and the stale one
    would be kept -- a wrong-SQL bug that no other test would catch. This
    asserts the list names every keyword-only argument the suppliers take.
    """
    import inspect

    from aws_advanced_python_wrapper.aio import host_list_provider as hlp

    suppliers = (hlp.create_static_provider, hlp.create_aurora_provider,
                 hlp.create_global_aurora_provider, hlp.create_multi_az_provider)
    dialect_supplied = {
        name
        for supplier in suppliers
        for name, param in inspect.signature(supplier).parameters.items()
        if param.kind is inspect.Parameter.KEYWORD_ONLY
    }
    covered = {attr.lstrip("_")
               for attr in AsyncPluginServiceImpl._PROVIDER_DIALECT_ATTRS}
    missing = dialect_supplied - covered
    assert not missing, (
        f"supplier argument(s) {sorted(missing)} are not in "
        f"_PROVIDER_DIALECT_ATTRS, so providers differing only in them "
        f"would be treated as interchangeable")


def test_host_list_provider_is_kept_when_the_dialect_supplies_the_same_one():
    """A dialect change is not a provider change.

    Several dialects supply the same no-topology provider. Replacing it
    anyway tears down the topology monitor and its connection and discards
    the cached topology, so an equivalent provider must be kept.
    """
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncStaticHostListProvider
    from aws_advanced_python_wrapper.database_dialect import (
        MysqlDatabaseDialect, RdsMysqlDialect)

    props = Properties({"host": "h", "port": "3306"})
    svc = _service_with_manager(RdsMysqlDialect(), None, props)
    existing = AsyncStaticHostListProvider(props)
    existing.stop = AsyncMock()
    svc.host_list_provider = existing

    # Both dialects supply the static provider, so the upgrade must not
    # swap the instance.
    svc.database_dialect = MysqlDatabaseDialect()

    async def _run():
        await svc._update_host_list_provider(MagicMock(name="conn"))
        assert svc.host_list_provider is existing
        existing.stop.assert_not_awaited()

    asyncio.run(_run())


def test_host_list_provider_is_replaced_when_the_topology_sql_differs():
    """The same provider *class* is not enough: Aurora MySQL and Aurora PG
    both supply AsyncAuroraHostListProvider but with different topology SQL
    and ports, so the instance must still be replaced."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, AuroraPgDialect)

    props = Properties({"host": "h", "port": "5432"})
    svc = _service_with_manager(AuroraPgDialect(), None, props)
    existing = AuroraPgDialect().get_async_host_list_provider_supplier()(svc, props)
    existing.stop = AsyncMock()
    svc.host_list_provider = existing
    svc.force_refresh_host_list = AsyncMock()

    svc.database_dialect = AuroraMysqlDialect()

    async def _run():
        await svc._update_host_list_provider(MagicMock(name="conn"))
        provider = svc.host_list_provider
        assert provider is not existing
        assert type(provider) is type(existing)
        assert provider._topology_query == AuroraMysqlDialect().topology_query
        assert provider._default_port == 3306

    asyncio.run(_run())


def test_replacing_the_provider_does_not_stop_the_outgoing_one():
    """The outgoing provider is left alone, which is only safe because it has
    nothing to stop: its monitor is built on the first refresh, and a provider
    built from an unconfirmed dialect refuses to refresh. Awaiting its stop
    would instead close a monitor connection on the path that needed a dialect
    upgrade."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, MultiAzClusterMysqlDialect)

    props = Properties({"host": "h", "port": "3306"})
    svc = _service_with_manager(AuroraMysqlDialect(), None, props)
    existing = AuroraMysqlDialect().get_async_host_list_provider_supplier()(svc, props)
    existing.stop = AsyncMock()
    svc.host_list_provider = existing
    svc.force_refresh_host_list = AsyncMock()

    svc.database_dialect = MultiAzClusterMysqlDialect()

    async def _run():
        await svc._update_host_list_provider(MagicMock(name="conn"))
        existing.stop.assert_not_awaited()
        assert existing._monitor is None
        assert svc.host_list_provider is not existing

    asyncio.run(_run())


def test_no_topology_monitor_is_built_before_the_dialect_is_confirmed():
    """The gate that makes stopping the outgoing provider's monitor
    unnecessary: until the dialect is confirmed a provider reports the hosts
    parsed from the connection properties and builds no monitor, so no monitor
    can carry the guessed dialect's SQL."""
    from aws_advanced_python_wrapper.database_dialect import AuroraMysqlDialect

    props = Properties({"host": "h", "port": "3306"})
    svc = _service_with_manager(AuroraMysqlDialect(), None, props)
    provider = AuroraMysqlDialect().get_async_host_list_provider_supplier()(svc, props)

    async def _run():
        assert svc.is_dialect_confirmed is False
        await provider.force_refresh(MagicMock(name="conn"))
        assert provider._monitor is None

    asyncio.run(_run())


def test_replacement_provider_inherits_the_panic_mode_probe():
    """The wrapper wires set_probe_host once, onto the provider built before
    the connect. A replacement that does not inherit it cannot discover the
    new writer when its monitoring connection dies mid-failover -- which is
    exactly the case on the connections that needed a dialect upgrade."""
    from aws_advanced_python_wrapper.database_dialect import (
        AuroraMysqlDialect, AuroraPgDialect, RdsMysqlDialect)

    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com", "port": "3306"})
    svc = _service_with_manager(RdsMysqlDialect(), None, props)
    existing = AuroraMysqlDialect().get_async_host_list_provider_supplier()(svc, props)
    existing.stop = AsyncMock()

    async def _probe(host_info):
        return (MagicMock(name="conn"), HostRole.WRITER)

    existing.set_probe_host(_probe)
    svc.host_list_provider = existing
    svc.force_refresh_host_list = AsyncMock()

    # A dialect whose provider differs, so a replacement really happens.
    svc.database_dialect = AuroraPgDialect()

    async def _run():
        await svc._update_host_list_provider(MagicMock(name="conn"))
        replacement = svc.host_list_provider
        assert replacement is not existing
        assert replacement._probe_host is _probe

    asyncio.run(_run())


def test_is_static_host_list_provider_reflects_provider_type():
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncStaticHostListProvider

    svc = _make_service()
    assert svc.is_static_host_list_provider() is False  # none installed yet
    svc.host_list_provider = AsyncStaticHostListProvider(
        Properties({"host": "h", "port": "5432"}))
    assert svc.is_static_host_list_provider() is True
    svc.host_list_provider = MagicMock()  # any dynamic provider
    assert svc.is_static_host_list_provider() is False


def _conn_raising_for(fragments, answers=None):
    """A connection whose cursor RAISES for queries matching any fragment.

    Models the real shape: on Aurora the Multi-AZ rule asks about rds_tools /
    mysql.rds_topology, which do not exist, so the server errors rather than
    returning no rows.
    """
    answers = answers or {}
    state = {"query": ""}

    async def execute(query, *args, **kwargs):
        state["query"] = query
        for frag in fragments:
            if frag in query:
                raise RuntimeError(f"relation does not exist: {frag}")

    def rows():
        for frag, response in answers.items():
            if frag in state["query"]:
                return list(response)
        return []

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(side_effect=execute)
    cursor.fetchall = AsyncMock(side_effect=lambda: rows())
    cursor.fetchone = AsyncMock(side_effect=lambda: next(iter(rows()), None))
    cursor.close = MagicMock()
    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)
    return conn


def test_aurora_cluster_endpoint_confirms_despite_an_erroring_candidate():
    """An erroring candidate is a candidate that answered no, not a fatal
    condition. On a plain Aurora cluster the Multi-AZ rule ALWAYS errors
    (no rds_tools / mysql.rds_topology), and the global rule answers no, so
    escalating left every Aurora cluster endpoint with an unconfirmed dialect,
    a one-host topology and no monitor -- silently, and permanently."""
    from aws_advanced_python_wrapper.database_dialect import (AuroraPgDialect,
                                                              DialectCode)

    props = Properties({"host": "db.cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"})
    svc = _service_with_manager(AuroraPgDialect(), DialectCode.AURORA_PG, props)
    svc.driver_dialect._driver_name = "psycopg"
    conn = _conn_raising_for(
        ["rds_tools", "multi_az"],
        {"pg_proc": [(1,)], "aurora_stat_utils": [(True,)],
         "aurora_replica_status": [(1,)],
         "AURORA_GLOBAL_DB_STATUS": [], "aurora_global_db_status": []})

    async def _run():
        await svc.update_database_dialect(conn)
        assert svc.is_dialect_confirmed is True, (
            "an erroring candidate must not leave the dialect unconfirmed")
        assert isinstance(svc.database_dialect, AuroraPgDialect)

    asyncio.run(_run())


def test_verification_timeout_is_actually_bounded(caplog):
    """The post-state alone cannot tell a real timeout from a slow walk that
    eventually gave up: with wait_for removed this test still passed, taking
    90 seconds. Assert the timeout's own message reaches the log."""
    import logging

    from aws_advanced_python_wrapper.database_dialect import (DialectCode,
                                                              RdsMysqlDialect)
    from aws_advanced_python_wrapper.utils.messages import Messages

    props = Properties({"auxiliary_query_timeout_sec": "0.05"})
    svc = _service_with_manager(RdsMysqlDialect(), DialectCode.RDS_MYSQL, props)

    async def _never_returns(*args, **kwargs):
        await asyncio.sleep(30)

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(side_effect=_never_returns)
    cursor.close = MagicMock()
    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)

    async def _run():
        with caplog.at_level(
                logging.DEBUG,
                logger="aws_advanced_python_wrapper.aio.plugin_service"):
            await svc.update_database_dialect(conn)
        assert svc.is_dialect_confirmed is False
        timeout_text = Messages.get("DatabaseDialectManager.QueryForDialectTimeout")
        assert timeout_text in caplog.text, (
            "the timeout message never reached the log, so wait_for may not "
            f"have fired; got: {caplog.text[:400]}")

    asyncio.run(_run())


def test_unconfirmed_dialect_reports_the_seed_host_without_querying():
    """The gating this branch exists to add: until the dialect is verified a
    provider must report the connection-string host and must NOT run topology
    SQL, because the SQL it would run belongs to the unverified guess."""
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncAuroraHostListProvider

    props = Properties({"host": "db.cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"})
    service = MagicMock()
    service.is_dialect_confirmed = False
    service.driver_dialect = MagicMock()

    provider = AsyncAuroraHostListProvider(
        props, driver_dialect=MagicMock(),
        topology_query="SELECT should_never_run", default_port=5432)
    provider._provider_service = service

    cursor = MagicMock(name="cursor")
    cursor.execute = AsyncMock(return_value=None)
    cursor.fetchall = AsyncMock(return_value=[])
    cursor.fetchone = AsyncMock(return_value=None)
    cursor.close = MagicMock()
    conn = MagicMock(name="conn")
    conn.cursor = MagicMock(return_value=cursor)

    seed = provider._seed_topology()[0]

    async def _run():
        for topology in (await provider.refresh(conn),
                         await provider.force_refresh(conn)):
            assert [(h.host, h.port) for h in topology] == [(seed.host, seed.port)]
        # The gate is the point: no SQL ran and no monitor was built.
        cursor.execute.assert_not_awaited()
        assert provider._topology_cache is None
        assert provider._monitor is None

        # And the gate tracks the service flag rather than latching. The
        # open direction is asserted on the predicate, not on downstream
        # SQL: that path runs through the topology monitor, which this
        # provider deliberately has no connection factory for.
        assert provider._is_dialect_confirmed() is False
        service.is_dialect_confirmed = True
        assert provider._is_dialect_confirmed() is True

    asyncio.run(_run())


def test_the_seed_host_is_a_fresh_object_on_every_report():
    """set_availability marks the live HostInfo, and the default availability
    strategy never expires a mark. Handing out the utils' one seed object
    would let a single failed connect exclude the host from selection for the
    process lifetime, with nothing to clear it."""
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncAuroraHostListProvider

    props = Properties({"host": "db.cluster-xyz.us-east-1.rds.amazonaws.com", "port": "5432"})
    provider = AsyncAuroraHostListProvider(
        props, driver_dialect=MagicMock(),
        topology_query="SELECT should_never_run", default_port=5432)

    first = provider._seed_topology()
    second = provider._seed_topology()

    assert first[0] is not second[0]
    assert (first[0].host, first[0].port) == (second[0].host, second[0].port)

    first[0].set_availability(HostAvailability.UNAVAILABLE)
    assert second[0].get_availability() == HostAvailability.AVAILABLE
    assert provider._seed_topology()[0].get_availability() == HostAvailability.AVAILABLE
