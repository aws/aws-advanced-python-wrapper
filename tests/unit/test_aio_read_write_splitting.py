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

"""F3-B SP-6: async read/write splitting plugin."""

from __future__ import annotations

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.aio.read_write_splitting_plugin import \
    AsyncReadWriteSplittingPlugin
from aws_advanced_python_wrapper.errors import ReadWriteSplittingError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import Properties


def _build(topology: Optional[tuple] = None):
    props = Properties({"host": "cluster.example", "port": "5432"})

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(
        side_effect=lambda host_info, props, fn: MagicMock(
            name=f"conn-to-{host_info.host}"
        )
    )
    driver_dialect.is_closed = AsyncMock(return_value=False)
    driver_dialect.is_in_transaction = AsyncMock(return_value=False)
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    )
    # Simulate initial writer conn.
    writer_conn = MagicMock(name="writer_conn")
    svc._current_connection = writer_conn
    # _open() opens reader/writer connections with the wired target driver func
    # (no longer hardcoded to psycopg). The mocked driver_dialect.connect
    # ignores it, so any callable works here.
    svc.set_target_driver_func(MagicMock(name="target_driver_func"))

    # Default stub for get_host_info_by_strategy: preserve the old
    # "first matching host in candidates" semantics so existing tests
    # that don't care about the selector strategy keep working.
    svc.get_host_info_by_strategy = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda role, strategy, candidates: (
            next((h for h in (candidates or ()) if h.role == role), None)))

    _topology = topology or (
        HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),
        HostInfo(host="reader.example", port=5432, role=HostRole.READER),
    )
    hlp = MagicMock()
    hlp.refresh = AsyncMock(return_value=_topology)
    # _switch_to_reader re-probes with force_refresh when a refresh returns no
    # readers (transient writer-only topology); mirror refresh's result.
    hlp.force_refresh = AsyncMock(return_value=_topology)

    plugin = AsyncReadWriteSplittingPlugin(svc, hlp, props)
    plugin._writer_conn = writer_conn
    plugin._writer_host_info = HostInfo(
        host="writer.example", port=5432, role=HostRole.WRITER
    )
    return plugin, svc, hlp, driver_dialect, writer_conn


def test_non_set_read_only_call_is_pass_through():
    async def _body() -> None:
        plugin, _, hlp, dd, _ = _build()

        async def _work() -> str:
            return "ok"

        result = await plugin.execute(
            target=object(),
            method_name=DbApiMethod.CURSOR_EXECUTE.method_name,
            execute_func=_work,
        )
        assert result == "ok"
        hlp.refresh.assert_not_called()

    asyncio.run(_body())


def test_set_read_only_true_switches_to_reader():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Reader conn cached + bound.
        assert plugin._reader_conn is not None
        assert svc.current_connection is plugin._reader_conn
        # Writer conn still remembered.
        assert plugin._writer_conn is writer_conn
        # A new driver connect was made to the reader.
        dd.connect.assert_awaited()

    asyncio.run(_body())


def test_set_read_only_false_switches_back_to_writer():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # First: flip to read-only (reader).
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        reader_conn = svc.current_connection
        assert reader_conn is plugin._reader_conn

        # Reset connect mock call count so we can observe the second flip.
        dd.connect.reset_mock()

        # Then: flip back to writer.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        # Cached writer reused (no new driver connect).
        dd.connect.assert_not_called()
        assert svc.current_connection is writer_conn

    asyncio.run(_body())


def test_set_read_only_true_reuses_cached_reader():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # Flip to reader (first time: opens a new reader conn).
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        first_reader = plugin._reader_conn
        dd.connect.reset_mock()

        # Flip back to writer, then to reader again.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Cached reader reused.
        dd.connect.assert_not_called()
        assert svc.current_connection is first_reader

    asyncio.run(_body())


def test_set_read_only_true_falls_back_when_no_reader_in_topology():
    """A reader-less topology (Aurora's aurora_replica_status() transiently
    drops the reader row) must NOT raise on set_read_only(True). Mirrors sync
    ReadWriteSplittingPlugin.NoReadersFound: stay on the current connection and
    warn. (Before the fix the async plugin raised
    'No reader host available in the current topology.')"""
    async def _body() -> None:
        plugin, svc, *_ = _build(
            topology=(HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),)
        )
        before = svc.current_connection

        async def _work() -> None:
            return None

        # Must NOT raise.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Stayed on the current connection; no reader swap occurred.
        assert svc.current_connection is before
        assert plugin._reader_conn is None

    asyncio.run(_body())


def test_reopens_reader_when_cached_reader_closed():
    async def _body() -> None:
        plugin, svc, hlp, dd, writer_conn = _build()

        async def _work() -> None:
            return None

        # Initial flip -> cache reader.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Simulate reader conn being closed between requests.
        dd.is_closed = AsyncMock(return_value=True)
        dd.connect.reset_mock()

        # Flip to writer, then back to reader -- should reopen.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        # Connect was called again (at least once) to reopen.
        assert dd.connect.call_count >= 1

    asyncio.run(_body())


def test_subscribed_methods_cover_set_read_only_and_execute_pipeline():
    # set_read_only drives the reader/writer swap; the execute-pipeline methods
    # are subscribed so a FailoverError raised mid-command evicts stale pooled
    # connections (sync #1117 parity).
    plugin, *_ = _build()
    subscribed = plugin.subscribed_methods
    assert DbApiMethod.CONNECTION_SET_READ_ONLY.method_name in subscribed
    for m in (
            DbApiMethod.CURSOR_EXECUTE,
            DbApiMethod.CURSOR_FETCHONE,
            DbApiMethod.CURSOR_FETCHMANY,
            DbApiMethod.CURSOR_FETCHALL,
            DbApiMethod.CONNECTION_COMMIT,
            DbApiMethod.CONNECTION_ROLLBACK):
        assert m.method_name in subscribed


def test_failover_failed_error_evicts_all_cached_connections():
    # FailoverFailedError -> no usable connection: evict BOTH cached conns,
    # including the in-use one, so a dead pooled conn is never reused (#1117).
    async def _body() -> None:
        from aws_advanced_python_wrapper.errors import FailoverFailedError
        plugin, svc, hlp, dd, writer_conn = _build()
        reader_conn = MagicMock(name="reader_conn")
        plugin._reader_conn = reader_conn
        plugin._reader_host_info = HostInfo(
            host="reader.example", port=5432, role=HostRole.READER)

        async def _boom() -> None:
            raise FailoverFailedError("no writer")

        with pytest.raises(FailoverFailedError):
            await plugin.execute(
                object(), DbApiMethod.CURSOR_EXECUTE.method_name, _boom)

        assert reader_conn.close.called
        assert writer_conn.close.called  # in-use conn evicted too
        assert plugin._reader_conn is None
        assert plugin._writer_conn is None

    asyncio.run(_body())


def test_failover_success_error_evicts_idle_keeps_current():
    # A non-failed FailoverError (success / txn-resolution-unknown) means the
    # wrapper reconnected: evict only the IDLE cached conns; keep the in-use one.
    async def _body() -> None:
        from aws_advanced_python_wrapper.errors import FailoverSuccessError
        plugin, svc, hlp, dd, writer_conn = _build()
        reader_conn = MagicMock(name="reader_conn")
        plugin._reader_conn = reader_conn
        plugin._reader_host_info = HostInfo(
            host="reader.example", port=5432, role=HostRole.READER)

        async def _boom() -> None:
            raise FailoverSuccessError("reconnected to new writer")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(
                object(), DbApiMethod.CURSOR_EXECUTE.method_name, _boom)

        assert reader_conn.close.called          # idle reader evicted
        assert plugin._reader_conn is None
        assert not writer_conn.close.called      # in-use writer kept
        assert plugin._writer_conn is writer_conn

    asyncio.run(_body())


def test_initial_connect_seeds_writer_cache():
    async def _body() -> None:
        plugin, svc, hlp, dd, _ = _build()
        # Clear the writer cache set in _build().
        plugin._writer_conn = None

        new_conn = MagicMock(name="fresh_writer")

        async def _connect_func() -> object:
            return new_conn

        result = await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=dd,
            host_info=HostInfo(host="w", port=5432),
            props=Properties({"host": "w"}),
            is_initial_connection=True,
            connect_func=_connect_func,
        )
        assert result is new_conn
        assert plugin._writer_conn is new_conn

    asyncio.run(_body())


def test_switch_to_reader_uses_configured_strategy():
    """RWS picks via plugin_service.get_host_info_by_strategy with the configured strategy."""
    r1 = HostInfo(host="r1.example", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2.example", port=5432, role=HostRole.READER)
    writer = HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, _, _ = _build(topology=(writer, r1, r2))

    # Inject strategy into props
    svc._props["reader_host_selector_strategy"] = "round_robin"

    # Stub get_host_info_by_strategy
    svc.get_host_info_by_strategy = MagicMock(return_value=r1)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())

    svc.get_host_info_by_strategy.assert_called_once()
    args = svc.get_host_info_by_strategy.call_args.args
    # (role, strategy, candidates)
    assert args[0] == HostRole.READER
    assert args[1] == "round_robin"
    # candidates exclude the writer
    assert writer not in args[2]
    assert r1 in args[2] and r2 in args[2]


def test_switch_to_reader_defaults_to_random_strategy():
    """No strategy prop -> 'random' passed to get_host_info_by_strategy."""
    r = HostInfo(host="r.example", port=5432, role=HostRole.READER)
    plugin, svc, hlp, _, _ = _build(topology=(
        HostInfo(host="w.example", port=5432, role=HostRole.WRITER),
        r,
    ))
    svc.get_host_info_by_strategy = MagicMock(return_value=r)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())

    assert svc.get_host_info_by_strategy.call_args.args[1] == "random"


def test_switch_to_reader_recovers_via_force_refresh_on_transient_no_reader():
    """A transient writer-only refresh is retried with force_refresh; when the
    retry surfaces the reader, the switch succeeds (no fallback, no raise)."""
    async def _body() -> None:
        plugin, svc, hlp, dd, _ = _build()
        writer_only = (
            HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),)
        full = (
            HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),
            HostInfo(host="reader.example", port=5432, role=HostRole.READER),
        )
        hlp.refresh = AsyncMock(return_value=writer_only)
        hlp.force_refresh = AsyncMock(return_value=full)
        # get_host_role is consulted by execute()'s gate, which probes the
        # CURRENT (writer) connection -> WRITER so the switch proceeds. Keep
        # the stub connection-aware so any probe of a non-writer connection
        # reports READER.
        _writer_conn = svc.current_connection

        async def _role(conn=None, *a, **k):
            return HostRole.WRITER if conn is _writer_conn else HostRole.READER

        svc.get_host_role = _role

        async def _work() -> None:
            return None

        await plugin.execute(
            object(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work, True)

        hlp.force_refresh.assert_awaited()  # the transient retry happened
        assert plugin._reader_host_info is not None
        assert plugin._reader_host_info.role == HostRole.READER

    asyncio.run(_body())


def test_switch_to_reader_raises_when_strategy_returns_none():
    """When readers EXIST in topology but the strategy can't pick one, raise
    'Could not open a reader connection...' (distinct from the reader-less
    fallback path)."""
    plugin, svc, hlp, dd, _ = _build(topology=(
        HostInfo(host="w", port=5432, role=HostRole.WRITER),
        HostInfo(host="r", port=5432, role=HostRole.READER),
    ))
    svc.get_host_info_by_strategy = MagicMock(return_value=None)

    # Test _switch_to_reader's internal raise directly; execute()'s sync-parity
    # fallback-to-current is covered by
    # test_set_read_only_true_falls_back_to_current_when_no_reader.
    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(plugin._switch_to_reader(dd, svc.current_connection))


def test_switch_to_reader_silently_no_ops_mid_transaction():
    """Mid-txn reader-swap request is silently skipped (sync parity:243-249)."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=True)

    async def _run():
        async def _set_ro():
            return None

        # Must NOT raise; must NOT swap
        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    assert plugin._reader_conn is None  # no swap happened


def test_switch_to_writer_refuses_mid_transaction():
    """Mid-txn writer swap raises ReadWriteSplittingError (sync parity:261-265)."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=True)
    # A writer swap only triggers when we're NOT already on the writer, so start
    # on a reader; flipping read_only=False mid-txn must then raise.
    svc._current_host_info = HostInfo(host="reader.example", port=5432, role=HostRole.READER)
    # Seed a different conn so a real writer swap would be attempted
    writer_conn = MagicMock(name="writer")
    plugin._writer_conn = writer_conn

    async def _run():
        async def _set_ro():
            return None

        # read_only=False + in_txn -> raise
        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, False)

    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(_run())


def test_switch_to_reader_allowed_when_not_in_transaction():
    """Not-in-txn case works normally (sanity)."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=False)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())  # no exception
    assert plugin._reader_conn is not None


def test_set_read_only_true_falls_back_to_current_when_no_reader():
    """When no reader can be opened but the current connection is usable,
    set_read_only(True) stays on it and warns rather than raising (sync parity:
    read_write_splitting_plugin.py:209-221)."""
    plugin, svc, hlp, dd, writer_conn = _build()
    dd.is_in_transaction = AsyncMock(return_value=False)
    dd.is_closed = AsyncMock(return_value=False)  # current connection still usable
    # No reader can be opened.
    plugin._switch_to_reader = AsyncMock(  # type: ignore[method-assign]
        side_effect=ReadWriteSplittingError("Could not open a reader connection"))

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())  # must NOT raise -- falls back to the current connection
    assert svc.current_connection is writer_conn


def test_set_read_only_true_propagates_when_current_also_dead():
    """If reader switching fails AND the current connection is dead, propagate."""
    plugin, svc, hlp, dd, _ = _build()
    dd.is_in_transaction = AsyncMock(return_value=False)
    dd.is_closed = AsyncMock(return_value=True)  # current connection is dead too
    plugin._switch_to_reader = AsyncMock(  # type: ignore[method-assign]
        side_effect=ReadWriteSplittingError("Could not open a reader connection"))

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(_run())


def test_reader_switch_retries_next_candidate_on_connect_failure():
    """Dead reader is skipped; next candidate tried."""
    r1 = HostInfo(host="r1.example", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2.example", port=5432, role=HostRole.READER)
    writer = HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, r1, r2))

    # Strategy returns r1 first, then r2
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r1, r2])

    # First connect fails; second succeeds
    attempts = []

    async def _connect(host_info, props, fn):
        attempts.append(host_info.host)
        if host_info is r1:
            raise OSError("r1 down")
        return MagicMock(name=f"conn-to-{host_info.host}")

    dd.connect = AsyncMock(side_effect=_connect)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    assert attempts == ["r1.example", "r2.example"]
    # r2 is now the cached reader
    assert plugin._reader_conn is not None


def test_reader_switch_raises_when_all_candidates_fail():
    """All readers dead -> ReadWriteSplittingError."""
    r = HostInfo(host="r.example", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w.example", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, r))
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r, None])
    dd.connect = AsyncMock(side_effect=OSError("dead"))

    # _switch_to_reader itself raises when no candidate connects; execute()'s
    # fallback-to-current is covered separately.
    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(plugin._switch_to_reader(dd, svc.current_connection))


def test_reader_switch_bounded_by_2x_hosts():
    """Retry loop stops after 2*len(hosts) attempts (sync parity)."""
    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, r1, r2))

    # Strategy returns a candidate repeatedly even after "removal" (simulate
    # a broken strategy that doesn't update internal state). The plugin's
    # own remove-from-list should exhaust candidates eventually.
    svc.get_host_info_by_strategy = MagicMock(
        side_effect=lambda role, strategy, candidates: (
            candidates[0] if candidates else None))
    dd.connect = AsyncMock(side_effect=OSError("always fail"))

    with pytest.raises(ReadWriteSplittingError):
        asyncio.run(plugin._switch_to_reader(dd, svc.current_connection))
    # 2 readers in topology -> max 2 attempts (since we remove dead ones
    # each iteration, the 4 iterations of sync's loop become 2 effective
    # attempts). Verify we didn't spin more than necessary.
    assert dd.connect.await_count <= 4  # generous upper bound


def test_switch_to_reader_discards_cached_conn_when_host_not_in_topology():
    """If the cached reader's host was removed from topology, drop cache + reopen."""
    old_reader = HostInfo(host="old-reader", port=5432, role=HostRole.READER)
    new_reader = HostInfo(host="new-reader", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, new_reader))

    # Seed a cached reader that's NOT in the new topology
    old_reader_conn = MagicMock(name="old_reader_conn")
    plugin._reader_conn = old_reader_conn
    plugin._reader_host_info = old_reader

    svc.get_host_info_by_strategy = MagicMock(return_value=new_reader)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    # The cached reader was discarded, a new one opened for new_reader
    dd.connect.assert_awaited()
    # _reader_host_info now reflects the NEW reader, not the old one
    assert plugin._reader_host_info is new_reader


def test_switch_to_writer_discards_cached_conn_when_host_not_in_topology():
    """Same but for writer-side cache."""
    old_writer = HostInfo(host="old-w", port=5432, role=HostRole.WRITER)
    new_writer = HostInfo(host="new-w", port=5432, role=HostRole.WRITER)
    reader = HostInfo(host="r", port=5432, role=HostRole.READER)
    plugin, svc, hlp, dd, _ = _build(topology=(new_writer, reader))

    old_writer_conn = MagicMock(name="old_writer_conn")
    plugin._writer_conn = old_writer_conn
    plugin._writer_host_info = old_writer

    # Start as if we're on a reader -- flipping read_only=False triggers writer swap
    current_reader_conn = MagicMock(name="current_reader_conn")
    svc._current_connection = current_reader_conn
    svc._current_host_info = reader

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, False)

    asyncio.run(_run())
    # A new writer connection was opened to new_writer
    dd.connect.assert_awaited()
    assert plugin._writer_host_info is new_writer


def test_cached_reader_reuse_still_works_when_host_in_topology():
    """Sanity: when the cached reader IS in topology, reuse is preserved."""
    reader = HostInfo(host="r", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, reader))

    cached_reader_conn = MagicMock(name="cached_reader")
    plugin._reader_conn = cached_reader_conn
    plugin._reader_host_info = reader

    initial_connect_count = dd.connect.await_count

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    # No new connect -- cached reader reused
    assert dd.connect.await_count == initial_connect_count
    assert plugin._reader_conn is cached_reader_conn


def test_swap_to_reader_releases_pool_backed_writer_conn():
    """Pool-backed current conn is closed after swap so it returns to the pool."""
    reader = HostInfo(host="r", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, reader))

    # Replace current connection with a SA-pool-looking mock
    pool_conn = MagicMock(name="pool_conn")
    pool_conn.close = MagicMock()
    # Fake the __module__ on the MagicMock's type so the helper detects it
    type(pool_conn).__module__ = "sqlalchemy.pool.base"
    svc._current_connection = pool_conn

    svc.get_host_info_by_strategy = MagicMock(return_value=reader)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    pool_conn.close.assert_called()


def test_swap_does_not_close_non_pool_conn():
    """Non-SA-pool current conn is NOT closed by the swap helper."""
    reader = HostInfo(host="r", port=5432, role=HostRole.READER)
    writer = HostInfo(host="w", port=5432, role=HostRole.WRITER)
    plugin, svc, hlp, dd, _ = _build(topology=(writer, reader))

    # Plain MagicMock -- __module__ is unittest.mock
    raw_conn = MagicMock(name="raw_conn")
    raw_conn.close = MagicMock()
    svc._current_connection = raw_conn

    svc.get_host_info_by_strategy = MagicMock(return_value=reader)

    async def _run():
        async def _set_ro():
            return None

        await plugin.execute(
            MagicMock(), DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _set_ro, True)

    asyncio.run(_run())
    raw_conn.close.assert_not_called()


def test_is_pool_connection_helper():
    """_is_pool_connection is now an instance method that consults the
    ConnectionProviderManager first, falling back to the SA pool module
    heuristic."""
    plugin, *_ = _build()
    assert plugin._is_pool_connection(None) is False

    class _FakePool:
        pass
    _FakePool.__module__ = "sqlalchemy.pool.impl"
    assert plugin._is_pool_connection(_FakePool()) is True

    class _FakeRaw:
        pass
    _FakeRaw.__module__ = "psycopg"
    assert plugin._is_pool_connection(_FakeRaw()) is False


# ---- Telemetry counters ------------------------------------------------


def _build_with_counters(topology=None):
    """Same as _build() but wires a MagicMock telemetry factory onto the
    plugin service BEFORE the plugin is constructed, so the counters the
    plugin captures in __init__ are the mocks we can assert on.

    Returns (plugin, svc, driver_dialect, counters).
    """
    props = Properties({"host": "cluster.example", "port": "5432"})

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(
        side_effect=lambda host_info, props, fn: MagicMock(
            name=f"conn-to-{host_info.host}"
        )
    )
    driver_dialect.is_closed = AsyncMock(return_value=False)
    driver_dialect.is_in_transaction = AsyncMock(return_value=False)
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(
        props, driver_dialect, HostInfo(host="writer.example", port=5432, role=HostRole.WRITER)
    )
    writer_conn = MagicMock(name="writer_conn")
    svc._current_connection = writer_conn
    svc.set_target_driver_func(MagicMock(name="target_driver_func"))
    svc.get_host_info_by_strategy = MagicMock(  # type: ignore[method-assign]
        side_effect=lambda role, strategy, candidates: (
            next((h for h in (candidates or ()) if h.role == role), None)))

    # Wire fake telemetry BEFORE constructing the plugin.
    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)
    svc.set_telemetry_factory(fake_tf)

    _topology = topology or (
        HostInfo(host="writer.example", port=5432, role=HostRole.WRITER),
        HostInfo(host="reader.example", port=5432, role=HostRole.READER),
    )
    hlp = MagicMock()
    hlp.refresh = AsyncMock(return_value=_topology)
    # _switch_to_reader re-probes with force_refresh when a refresh returns no
    # readers (transient writer-only topology); mirror refresh's result.
    hlp.force_refresh = AsyncMock(return_value=_topology)

    plugin = AsyncReadWriteSplittingPlugin(svc, hlp, props)
    plugin._writer_conn = writer_conn
    plugin._writer_host_info = HostInfo(
        host="writer.example", port=5432, role=HostRole.WRITER
    )
    return plugin, svc, driver_dialect, fake_counters


def test_switch_to_reader_emits_telemetry_counter():
    """rws.switches.to_reader.count increments on a successful reader swap."""
    async def _body() -> None:
        plugin, svc, dd, counters = _build_with_counters()

        async def _work() -> None:
            return None

        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )

        assert counters["rws.switches.to_reader.count"].inc.called
        assert counters["rws.switches.to_writer.count"].inc.called is False

    asyncio.run(_body())


def test_switch_to_writer_emits_telemetry_counter():
    """rws.switches.to_writer.count increments on a successful writer swap
    (cached-writer path triggered by flipping read_only back to False)."""
    async def _body() -> None:
        plugin, svc, dd, counters = _build_with_counters()

        async def _work() -> None:
            return None

        # First flip to reader, then back to writer -- second flip is
        # the writer-swap we want to observe.
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            True,
        )
        counters["rws.switches.to_writer.count"].inc.reset_mock()
        await plugin.execute(
            object(),
            DbApiMethod.CONNECTION_SET_READ_ONLY.method_name,
            _work,
            False,
        )

        assert counters["rws.switches.to_writer.count"].inc.called

    asyncio.run(_body())
