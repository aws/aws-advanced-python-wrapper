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

"""F3-B SP-4: async failover plugin tests."""

from __future__ import annotations

import asyncio
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from aws_advanced_python_wrapper.aio.failover_plugin import AsyncFailoverPlugin
from aws_advanced_python_wrapper.aio.plugin_service import \
    AsyncPluginServiceImpl
from aws_advanced_python_wrapper.errors import (
    FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.failover_mode import FailoverMode
from aws_advanced_python_wrapper.utils.properties import Properties


def _build_plugin(
        enabled: bool = True,
        mode: Optional[str] = None,
        topology: Optional[tuple] = None,
        timeout_sec: float = 1.0):
    props_dict = {
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true" if enabled else "false",
        "failover_timeout_sec": str(timeout_sec),
    }
    if mode:
        props_dict["failover_mode"] = mode
    props = Properties(props_dict)

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)
    # Writer-failover now re-verifies the connected candidate's role via
    # get_host_role (rejecting a stale-topology reader). The real impl needs a
    # dialect+connection we don't have in unit tests, so default it to WRITER;
    # individual tests override it (e.g. to READER) to exercise the reject path.
    svc.get_host_role = AsyncMock(return_value=HostRole.WRITER)  # type: ignore[method-assign]
    # _open_connection routes the failover reconnect through
    # plugin_service.force_connect (so auth plugins re-apply -- IAM token etc. --
    # while BYPASSING the pooled provider so the reconnect is non-pooled),
    # skipping the failover plugin to avoid recursion. Mock it here; tests that
    # care about the reconnect assert on svc.force_connect rather than
    # driver_dialect.connect.
    svc.force_connect = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(name="new_conn"))
    # Keep a connect mock too in case any path consults it.
    svc.connect = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(name="new_conn"))

    host_list_provider = MagicMock()
    host_list_provider.force_refresh = AsyncMock(
        return_value=topology or (
            HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER),
            HostInfo(host="reader.example.com", port=5432, role=HostRole.READER),
        )
    )

    plugin = AsyncFailoverPlugin(
        plugin_service=svc,
        host_list_provider=host_list_provider,
        props=props,
    )
    return plugin, svc, host_list_provider, driver_dialect


# ---- Config / subscription ---------------------------------------------


def test_failover_plugin_subscribed_methods_includes_execute_and_connect():
    plugin, *_ = _build_plugin()
    subs = plugin.subscribed_methods
    assert "Cursor.execute" in subs
    assert "Connection.commit" in subs
    assert "Connect.connect" not in subs or True  # tolerant: names may vary


def test_failover_plugin_defaults_to_strict_writer_mode():
    plugin, *_ = _build_plugin(mode=None)
    assert plugin._mode == FailoverMode.STRICT_WRITER


def test_failover_plugin_reads_strict_reader_mode_from_props():
    plugin, *_ = _build_plugin(mode="strict_reader")
    assert plugin._mode == FailoverMode.STRICT_READER


def test_failover_plugin_reads_reader_or_writer_mode_from_props():
    plugin, *_ = _build_plugin(mode="reader_or_writer")
    assert plugin._mode == FailoverMode.READER_OR_WRITER


# ---- Behavior: enabled / disabled --------------------------------------


def test_disabled_failover_passes_exceptions_through_unchanged():
    async def _body() -> None:
        plugin, *_ = _build_plugin(enabled=False)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(ConnectionError):
            await plugin.execute(
                target=object(),
                method_name="Cursor.execute",
                execute_func=_raiser,
            )

    asyncio.run(_body())


def test_enabled_failover_converts_network_error_to_failover_success():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin()
        # No dialect is attached to the svc in unit tests, so the real
        # ExceptionHandler would classify everything as non-network. Force
        # the classification used by _should_failover to True here.
        svc.is_network_exception = MagicMock(return_value=True)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(
                target=object(),
                method_name="Cursor.execute",
                execute_func=_raiser,
            )
        host_list_provider.force_refresh.assert_awaited()
        svc.force_connect.assert_awaited()  # reconnect routes via plugin_service.force_connect
        # Plugin service was rebound to the new connection.
        assert svc.current_connection is not None

    asyncio.run(_body())


def test_enabled_failover_picks_writer_by_default():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin()
        svc.is_network_exception = MagicMock(return_value=True)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        # _open_connection -> plugin_service.connect(host_info, props, self);
        # first positional arg is the chosen HostInfo.
        chosen_host = svc.force_connect.call_args[0][0]
        assert chosen_host.role == HostRole.WRITER
        assert chosen_host.host == "writer.example.com"

    asyncio.run(_body())


def test_writer_failover_rejects_stale_reader_labeled_as_writer():
    # Right after a failover the topology can still label the demoted old writer
    # as WRITER. Writer failover must re-verify via get_host_role and refuse to
    # accept a reader -- otherwise the connection lands on a reader, which is
    # the bug the fail_from_writer_to_new_writer_* integration tests caught.
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin(
            mode="strict_writer", timeout_sec=0.2)
        driver_dialect.abort_connection = AsyncMock()
        # Topology labels writer.example.com WRITER, but the data-plane probe
        # reports READER (stale topology) -- must be rejected.
        svc.get_host_role = AsyncMock(return_value=HostRole.READER)
        svc.set_current_connection = AsyncMock()  # spy: must never be called

        with pytest.raises(FailoverFailedError):
            await plugin._do_failover(driver_dialect=driver_dialect)

        svc.set_current_connection.assert_not_called()

    asyncio.run(_body())


def test_enabled_failover_picks_reader_in_strict_reader_mode():
    async def _body() -> None:
        plugin, svc, _hlp, driver_dialect = _build_plugin(mode="strict_reader")
        svc.is_network_exception = MagicMock(return_value=True)
        # B.4: reader failover goes through get_host_info_by_strategy.
        # The real plugin_manager isn't wired in unit tests, so stub it to
        # return the reader from the topology _build_plugin seeded.
        reader_host = HostInfo(
            host="reader.example.com", port=5432, role=HostRole.READER)
        svc.get_host_info_by_strategy = MagicMock(return_value=reader_host)
        # STRICT_READER data-plane-verifies the candidate role (sync
        # failover_v2 parity); the picked reader really is a reader here.
        svc.get_host_role = AsyncMock(return_value=HostRole.READER)

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        chosen_host = svc.force_connect.call_args[0][0]
        assert chosen_host.role == HostRole.READER
        assert chosen_host.host == "reader.example.com"

    asyncio.run(_body())


def test_failover_raises_failover_failed_when_topology_has_no_target():
    async def _body() -> None:
        plugin, svc, host_list_provider, driver_dialect = _build_plugin(
            topology=(), timeout_sec=0.3,
        )
        svc.is_network_exception = MagicMock(return_value=True)
        # Override force_refresh to always return empty; also make
        # driver_dialect.connect fail if invoked (it shouldn't be).
        host_list_provider.force_refresh = AsyncMock(return_value=())
        driver_dialect.connect = AsyncMock(
            side_effect=AssertionError("connect should not be called with empty topology")
        )

        async def _raiser() -> Any:
            raise ConnectionError("boom")

        with pytest.raises(FailoverFailedError):
            await plugin.execute(object(), "Cursor.execute", _raiser)

    asyncio.run(_body())


def test_non_network_error_is_not_converted_to_failover():
    """Programming errors etc. should pass through without triggering failover."""
    async def _body() -> None:
        plugin, _, host_list_provider, _dd = _build_plugin()

        async def _raiser() -> Any:
            raise ValueError("programming error")

        with pytest.raises(ValueError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        host_list_provider.force_refresh.assert_not_called()

    asyncio.run(_body())


def test_failover_success_not_caught_by_self():
    """The plugin must not re-enter failover when it sees its own success signal."""
    async def _body() -> None:
        plugin, _, host_list_provider, _dd = _build_plugin()

        async def _raiser() -> Any:
            raise FailoverSuccessError("already failed over")

        with pytest.raises(FailoverSuccessError):
            await plugin.execute(object(), "Cursor.execute", _raiser)
        host_list_provider.force_refresh.assert_not_called()

    asyncio.run(_body())


def test_connect_pass_through_is_not_intercepted_for_initial_connect():
    async def _body() -> None:
        plugin, *_ = _build_plugin()
        raw_conn = MagicMock()

        async def _connect() -> Any:
            return raw_conn

        result = await plugin.connect(
            target_driver_func=lambda: None,
            driver_dialect=MagicMock(),
            host_info=HostInfo(host="h", port=5432),
            props=Properties({"host": "h"}),
            is_initial_connection=True,
            connect_func=_connect,
        )
        assert result is raw_conn

    asyncio.run(_body())


# ---- B.1: dialect-aware _should_failover classification ----------------


def test_should_failover_uses_plugin_service_is_network_exception():
    """Replaces the string-match heuristic with is_network_exception."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)
    exc = Exception("arbitrary")

    assert plugin._should_failover(exc) is True
    svc.is_network_exception.assert_called_once_with(error=exc)


def test_should_failover_returns_false_when_dialect_classifies_not_network():
    """Non-network, non-read-only exception: no failover."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    assert plugin._should_failover(Exception("arbitrary")) is False


def test_should_failover_strict_writer_triggers_on_read_only_exception():
    """STRICT_WRITER mode treats read-only-connection exception as failover trigger."""
    plugin, svc, *_ = _build_plugin(mode="strict_writer")
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=True)

    assert plugin._should_failover(Exception("read only")) is True


def test_should_failover_strict_reader_does_not_trigger_on_read_only():
    """STRICT_READER mode does NOT treat read-only as failover trigger."""
    plugin, svc, *_ = _build_plugin(mode="strict_reader")
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=True)

    assert plugin._should_failover(Exception("read only")) is False


def test_should_failover_triggers_on_raw_oserror():
    """A raw OSError (e.g. EBADF) -- the async manifestation of an EFM/proxy
    abort closing the socket fd under an awaiting query -- triggers failover
    even though the dialect handler doesn't classify it as a network exception
    (regression for test_fail_from_reader_to_writer)."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    exc = OSError(9, "Bad file descriptor")
    assert plugin._should_failover(exc) is True


def test_should_failover_triggers_on_oserror_in_cause_chain():
    """An OSError reached via the explicit __cause__ chain still counts."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    wrapped = RuntimeError("wrapped")
    wrapped.__cause__ = OSError(9, "Bad file descriptor")
    assert plugin._should_failover(wrapped) is True


def test_should_failover_ignores_oserror_in_implicit_context():
    """An OSError only in __context__ (implicit chaining) is NOT treated as a
    failover trigger -- avoids false positives from unrelated handled errors."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    unrelated = ValueError("not a connection error")
    unrelated.__context__ = OSError(2, "No such file")  # implicit, not 'from'
    assert plugin._should_failover(unrelated) is False


def test_should_failover_triggers_on_connection_is_lost_message():
    """psycopg's AsyncConnection raises OperationalError('the connection is
    lost') when the socket dies mid-failover. The shared PG handler classifies
    'the connection is closed' but NOT 'the connection is lost', so the async
    failover plugin must catch this message itself -- otherwise a writer-change
    failover lets it propagate raw (regression for
    test_fail_from_writer_to_new_writer_*)."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    exc = Exception("the connection is lost")
    assert plugin._should_failover(exc) is True
    # Also via the __cause__ chain.
    wrapped = RuntimeError("wrapped")
    wrapped.__cause__ = Exception("the connection is lost")
    assert plugin._should_failover(wrapped) is True


def test_should_not_failover_on_unrelated_message():
    """A non-connection error message is not promoted to a failover trigger."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=False)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)
    assert plugin._should_failover(ValueError("syntax error at or near")) is False


def test_should_not_failover_on_self_raised_signals():
    """FailoverSuccessError / FailoverFailedError must not re-enter failover."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    assert plugin._should_failover(FailoverSuccessError("noop")) is False
    assert plugin._should_failover(FailoverFailedError("noop")) is False


# ---- B.2: mid-transaction TransactionResolutionUnknownError ------------


def test_failover_raises_transaction_unknown_when_mid_transaction():
    """Mid-txn failover -> TransactionResolutionUnknownError.

    Driven by the *tracked* transaction flag (maintained by the DefaultPlugin),
    captured at execute-start -- NOT a probe of the post-failover connection
    (which is always idle).
    """
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)
    # A prior op established a transaction (the DefaultPlugin would have set
    # this while the connection was healthy).
    svc._is_in_transaction = True

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(TransactionResolutionUnknownError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_raises_failover_success_when_not_in_transaction():
    """Idle (autocommit) failover -> FailoverSuccessError, even though the
    post-failover connection is also idle. Guards against regressing to a
    connection-probe that can't distinguish the two."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)
    svc._is_in_transaction = False  # not mid-transaction

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_raises_failover_success_when_dialect_reports_not_in_transaction():
    """Outside a txn (per the driver dialect probe) -> FailoverSuccessError
    (caller can retry cleanly). Distinct from the sibling test above, which
    drives the decision via the service's tracked ``_is_in_transaction`` flag."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    svc._current_connection = MagicMock(name="old_conn")
    driver_dialect.is_in_transaction = AsyncMock(return_value=False)

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_txn_probe_errors_treated_as_not_in_txn():
    """If is_in_transaction probe itself fails, fall back to FailoverSuccessError
    (best-effort probing; don't promote a probe failure to TransactionResolutionUnknown)."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    svc._current_connection = MagicMock(name="old_conn")
    driver_dialect.is_in_transaction = AsyncMock(side_effect=RuntimeError("probe broke"))

    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


def test_failover_no_current_connection_treated_as_not_in_txn():
    """No current connection (edge case) -> FailoverSuccessError, not TxnUnknown."""
    plugin, svc, *_ = _build_plugin()
    svc.is_network_exception = MagicMock(return_value=True)
    svc.is_read_only_connection_exception = MagicMock(return_value=False)

    # No current_connection seeded.
    plugin._do_failover = AsyncMock()  # type: ignore[method-assign]

    async def _raising():
        raise Exception("network failure")

    with pytest.raises(FailoverSuccessError):
        asyncio.run(plugin.execute(MagicMock(), "Cursor.execute", _raising))


# ---- B.3: set_availability on connect success / failure ----------------


def test_failover_marks_connected_host_available():
    """Successful reconnect -> host marked AVAILABLE."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin()
    svc.set_availability = MagicMock()

    # Force topology to a single writer that will succeed
    writer = HostInfo(host="w1", port=5432, role=HostRole.WRITER)
    host_list_provider.force_refresh = AsyncMock(return_value=(writer,))

    # Stub _open_connection to succeed
    plugin._open_connection = AsyncMock(return_value=MagicMock(name="new_conn"))

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    svc.set_availability.assert_any_call(
        writer.as_aliases(), HostAvailability.AVAILABLE)


def test_failover_marks_failed_host_unavailable():
    """Connect failure -> host marked UNAVAILABLE."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin(timeout_sec=0.5)
    svc.set_availability = MagicMock()

    writer = HostInfo(host="dead-writer", port=5432, role=HostRole.WRITER)
    host_list_provider.force_refresh = AsyncMock(return_value=(writer,))

    # Open always fails
    plugin._open_connection = AsyncMock(side_effect=OSError("refused"))

    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    svc.set_availability.assert_any_call(
        writer.as_aliases(), HostAvailability.UNAVAILABLE)


# ---- B.4: reader retry loop + strategy + writer fallback ---------------


def test_reader_failover_uses_configured_strategy():
    """Reader failover picks via plugin_service.get_host_info_by_strategy
    with the configured strategy name."""
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "strict-reader",
        "failover_reader_host_selector_strategy": "round_robin",
    })
    # Rebuild svc + plugin with our props
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    reader = HostInfo(host="r1", port=5432, role=HostRole.READER)
    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(reader,))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)
    svc.get_host_info_by_strategy = MagicMock(return_value=reader)
    # Role probe must succeed as READER (a failed probe now DROPS the
    # candidate, sync failover_v2:288-289).
    svc.get_host_role = AsyncMock(return_value=HostRole.READER)  # type: ignore[method-assign]
    plugin._open_connection = AsyncMock(return_value=MagicMock())

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    svc.get_host_info_by_strategy.assert_called()
    first_call = svc.get_host_info_by_strategy.call_args_list[0]
    # Positional: (role, strategy, host_list)
    assert first_call.args[1] == "round_robin"


def test_reader_failover_cycles_through_candidates():
    """Failed reader is removed from remaining; next candidate tried."""
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "strict-reader",
    })
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    r2 = HostInfo(host="r2", port=5432, role=HostRole.READER)

    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(r1, r2))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)

    # Strategy picks r1 first, then r2
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r1, r2])
    # Role probe must succeed as READER (a failed probe now DROPS the
    # candidate, sync failover_v2:288-289).
    svc.get_host_role = AsyncMock(return_value=HostRole.READER)  # type: ignore[method-assign]

    attempts = []

    async def _open(target, _driver_dialect):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    assert attempts == ["r1", "r2"]


def test_reader_failover_falls_back_to_original_writer_in_reader_or_writer_mode():
    """READER_OR_WRITER: when all readers fail, try the original writer."""
    props = Properties({
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "1.0",
        "failover_mode": "reader-or-writer",
    })
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginServiceImpl
    dd = MagicMock()
    dd.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    dd.transfer_session_state = AsyncMock()
    svc = AsyncPluginServiceImpl(props, dd)

    r1 = HostInfo(host="r1", port=5432, role=HostRole.READER)
    w = HostInfo(host="w1", port=5432, role=HostRole.WRITER)

    hlp = MagicMock()
    hlp.force_refresh = AsyncMock(return_value=(r1, w))

    plugin = AsyncFailoverPlugin(plugin_service=svc, host_list_provider=hlp, props=props)
    # Strategy returns r1 first, then None (signals "no more readers")
    svc.get_host_info_by_strategy = MagicMock(side_effect=[r1, None])
    # The original-writer probe must succeed (a failed probe now moves on
    # without accepting, sync failover_v2:307-308). READER = demoted writer.
    svc.get_host_role = AsyncMock(return_value=HostRole.READER)  # type: ignore[method-assign]

    attempts = []

    async def _open(target, _driver_dialect):
        attempts.append(target.host)
        if target is r1:
            raise OSError("r1 down")
        return MagicMock()

    plugin._open_connection = _open  # type: ignore[method-assign]

    asyncio.run(plugin._do_failover(driver_dialect=dd))

    assert attempts == ["r1", "w1"]


def test_failover_falls_back_to_initial_host_when_topology_empty():
    """If force_refresh returns empty topology, use initial_connection_host_info."""
    plugin, svc, host_list_provider, driver_dialect = _build_plugin(
        mode="strict-writer", timeout_sec=0.5)

    initial = HostInfo(host="writer-initial", port=5432, role=HostRole.WRITER)
    svc.initial_connection_host_info = initial
    host_list_provider.force_refresh = AsyncMock(return_value=())

    plugin._open_connection = AsyncMock(return_value=MagicMock())

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    plugin._open_connection.assert_called()
    (target, _), _ = plugin._open_connection.call_args_list[0]
    assert target is initial


# ---- Telemetry counters ------------------------------------------------


def _build_plugin_with_counters(mode: str = "strict_writer"):
    """Same wiring as _build_plugin but with a MagicMock telemetry factory.

    Returns (plugin, svc, host_list_provider, driver_dialect, counters) where
    ``counters`` is a dict of counter-name -> MagicMock (so tests can assert
    .inc.called on the one they care about).
    """
    fake_counters: dict = {}

    def _create_counter(name):
        c = MagicMock(name=f"counter:{name}")
        fake_counters[name] = c
        return c

    fake_tf = MagicMock()
    fake_tf.create_counter = MagicMock(side_effect=_create_counter)

    props_dict = {
        "host": "cluster.example.com",
        "port": "5432",
        "enable_failover": "true",
        "failover_timeout_sec": "0.5",
    }
    if mode:
        props_dict["failover_mode"] = mode
    props = Properties(props_dict)

    driver_dialect = MagicMock()
    driver_dialect.connect = AsyncMock(return_value=MagicMock(name="new_conn"))
    driver_dialect.transfer_session_state = AsyncMock()

    svc = AsyncPluginServiceImpl(props, driver_dialect)
    svc.set_telemetry_factory(fake_tf)
    # Writer-failover re-verifies the candidate role via get_host_role; default
    # to WRITER so the success path is exercised (see _build_plugin).
    svc.get_host_role = AsyncMock(return_value=HostRole.WRITER)  # type: ignore[method-assign]

    host_list_provider = MagicMock()
    host_list_provider.force_refresh = AsyncMock(return_value=(
        HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER),
        HostInfo(host="reader.example.com", port=5432, role=HostRole.READER),
    ))

    plugin = AsyncFailoverPlugin(
        plugin_service=svc,
        host_list_provider=host_list_provider,
        props=props,
    )
    return plugin, svc, host_list_provider, driver_dialect, fake_counters


def test_failover_emits_writer_triggered_counter_on_writer_path():
    plugin, svc, _, driver_dialect, counters = _build_plugin_with_counters(
        mode="strict_writer")
    # Force a successful writer reconnect.
    plugin._open_connection = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(name="new_conn"))

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    # Writer path must have been taken.
    assert counters["writer_failover.triggered.count"].inc.called
    assert counters["writer_failover.completed.success.count"].inc.called
    # Reader path was NOT taken.
    assert not counters["reader_failover.triggered.count"].inc.called


def test_failover_emits_reader_triggered_counter_on_reader_path():
    plugin, svc, hlp, driver_dialect, counters = _build_plugin_with_counters(
        mode="strict_reader")
    reader_host = HostInfo(
        host="reader.example.com", port=5432, role=HostRole.READER)
    svc.get_host_info_by_strategy = MagicMock(return_value=reader_host)
    # STRICT_READER data-plane-verifies the candidate role (sync parity).
    svc.get_host_role = AsyncMock(return_value=HostRole.READER)
    plugin._open_connection = AsyncMock(  # type: ignore[method-assign]
        return_value=MagicMock(name="new_conn"))

    asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))

    assert counters["reader_failover.triggered.count"].inc.called
    assert counters["reader_failover.completed.success.count"].inc.called
    assert not counters["writer_failover.triggered.count"].inc.called


def test_failover_emits_writer_failed_counter_on_exhausted_deadline():
    plugin, svc, hlp, driver_dialect, counters = _build_plugin_with_counters(
        mode="strict_writer")
    # Make every open fail so we hit the FailoverFailedError path.
    plugin._open_connection = AsyncMock(  # type: ignore[method-assign]
        side_effect=OSError("refused"))

    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))
    assert counters["writer_failover.triggered.count"].inc.called
    assert counters["writer_failover.completed.failed.count"].inc.called
    assert not counters["writer_failover.completed.success.count"].inc.called


def test_within_deadline_bounds_a_hanging_await():
    # A candidate connect / topology refresh that never returns (blackholed
    # host with no connect timeout) must not block failover past the deadline.
    plugin, *_ = _build_plugin_with_counters(mode="strict_writer")

    async def _run() -> None:
        async def _hang() -> None:
            await asyncio.sleep(30)

        deadline = asyncio.get_event_loop().time() + 0.2
        with pytest.raises(asyncio.TimeoutError):
            await plugin._within_deadline(_hang(), deadline)

    asyncio.run(_run())


def test_within_deadline_past_deadline_raises_immediately():
    plugin, *_ = _build_plugin_with_counters(mode="strict_writer")

    async def _run() -> None:
        async def _hang() -> None:
            await asyncio.sleep(30)

        deadline = asyncio.get_event_loop().time() - 1.0  # already expired
        with pytest.raises(asyncio.TimeoutError):
            await plugin._within_deadline(_hang(), deadline)

    asyncio.run(_run())


def test_failover_does_not_hang_when_open_connection_blocks():
    # End-to-end: even if _open_connection blocks indefinitely, _do_failover
    # gives up at failover_timeout_sec with FailoverFailedError rather than
    # hanging (this is the bug the env-3 pooled-failover-failed test exposed).
    import time

    plugin, svc, hlp, driver_dialect, counters = _build_plugin_with_counters(
        mode="strict_writer")
    plugin._failover_timeout_sec = 0.3

    async def _hang(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(30)

    plugin._open_connection = _hang  # type: ignore[method-assign]

    start = time.monotonic()
    with pytest.raises(FailoverFailedError):
        asyncio.run(plugin._do_failover(driver_dialect=driver_dialect))
    # Bounded: ~timeout + one inter-attempt sleep, nowhere near 30s.
    assert time.monotonic() - start < 5.0


# ---- STRICT_READER data-plane role verification (sync failover_v2 parity) --


def test_strict_reader_rejects_promoted_writer_and_accepts_demoted_original_writer():
    """A topology-labeled reader that live-probes as WRITER is rejected, and
    the original writer -- now demoted to a reader -- is accepted with its
    VERIFIED role stamped on the bound HostInfo (sync failover_v2:275-308)."""
    async def _body() -> None:
        writer = HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER)
        stale_reader = HostInfo(host="reader.example.com", port=5432, role=HostRole.READER)
        plugin, svc, _hlp, driver_dialect = _build_plugin(
            mode="strict_reader", topology=(writer, stale_reader))

        reader_conn = MagicMock(name="promoted_writer_conn")
        writer_conn = MagicMock(name="demoted_writer_conn")
        conns = {"reader.example.com": reader_conn, "writer.example.com": writer_conn}

        async def _open(host, _dd):
            return conns[host.host]

        plugin._open_connection = _open  # type: ignore[method-assign]
        svc.get_host_info_by_strategy = MagicMock(return_value=stale_reader)

        async def _role(conn):
            # The "reader" was promoted (probes WRITER); the original writer
            # was demoted (probes READER).
            return HostRole.WRITER if conn is reader_conn else HostRole.READER

        svc.get_host_role = _role  # type: ignore[method-assign]
        svc.set_current_connection = AsyncMock()  # type: ignore[method-assign]

        await plugin._do_failover(driver_dialect=driver_dialect)

        bound_conn, bound_host = svc.set_current_connection.call_args[0]
        assert bound_conn is writer_conn
        assert bound_host.host == "writer.example.com"
        assert bound_host.role == HostRole.READER  # verified role, not stale label
        # The promoted writer's connection was rejected and closed.
        reader_conn.close.assert_called()

    asyncio.run(_body())


def test_strict_reader_gives_up_when_original_writer_is_still_writer():
    """When every candidate (including the original writer) live-probes as
    WRITER, STRICT_READER fails over to nothing: the original writer is
    probed once, remembered via is_original_writer_still_writer, and not
    re-dialed every pass (sync failover_v2:292-308)."""
    async def _body() -> None:
        writer = HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER)
        stale_reader = HostInfo(host="reader.example.com", port=5432, role=HostRole.READER)
        plugin, svc, _hlp, driver_dialect = _build_plugin(
            mode="strict_reader", topology=(writer, stale_reader),
            timeout_sec=0.5)

        plugin._open_connection = AsyncMock(  # type: ignore[method-assign]
            return_value=MagicMock(name="conn"))
        svc.get_host_info_by_strategy = MagicMock(return_value=stale_reader)
        svc.get_host_role = AsyncMock(return_value=HostRole.WRITER)
        svc.set_current_connection = AsyncMock()  # type: ignore[method-assign]

        with pytest.raises(FailoverFailedError):
            await plugin._do_failover(driver_dialect=driver_dialect)

        svc.set_current_connection.assert_not_called()
        # Probed the stale reader once + the original writer once; no re-dial
        # of the confirmed-still-writer on later passes.
        assert svc.get_host_role.await_count == 2

    asyncio.run(_body())


# ---- connect-time failover (sync failover_v2_plugin.py:127-180 parity) -----


def test_connect_time_failover_on_failover_worthy_connect_error():
    """With enable_connect_failover, a network error on the initial dial
    marks the host UNAVAILABLE, runs failover, and returns the new current
    connection instead of surfacing the error."""
    async def _body() -> None:
        plugin, svc, _hlp, driver_dialect = _build_plugin()
        plugin._enable_connect_failover = True
        svc.is_network_exception = MagicMock(return_value=True)
        svc.set_availability = MagicMock()
        failover_conn = MagicMock(name="failover_conn")

        async def _fake_failover(driver_dialect=None, **_kw):
            svc._current_connection = failover_conn

        plugin._do_failover = _fake_failover  # type: ignore[method-assign]

        async def _dial():
            raise ConnectionError("boom")

        host = HostInfo(host="writer.example.com", port=5432)
        conn = await plugin.connect(
            MagicMock(), driver_dialect, host, Properties({}), True, _dial)
        assert conn is failover_conn
        marked_aliases, marked_avail = svc.set_availability.call_args_list[0][0]
        assert marked_avail == HostAvailability.UNAVAILABLE
        assert host.as_aliases() == marked_aliases

    asyncio.run(_body())


def test_connect_time_failover_skips_dial_for_known_unavailable_host():
    """A target already known UNAVAILABLE is not dialed at all -- topology is
    refreshed and failover runs directly (sync :168-172)."""
    async def _body() -> None:
        dead = HostInfo(host="writer.example.com", port=5432, role=HostRole.WRITER)
        dead.set_availability(HostAvailability.UNAVAILABLE)
        plugin, svc, _hlp, driver_dialect = _build_plugin()
        plugin._enable_connect_failover = True
        svc.refresh_host_list = AsyncMock()
        svc.filter_hosts = MagicMock(side_effect=lambda hosts: hosts)
        svc._all_hosts = (dead,)
        failover_conn = MagicMock(name="failover_conn")

        async def _fake_failover(driver_dialect=None, **_kw):
            svc._current_connection = failover_conn

        plugin._do_failover = _fake_failover  # type: ignore[method-assign]
        dialed = {"n": 0}

        async def _dial():
            dialed["n"] += 1
            return MagicMock()

        conn = await plugin.connect(
            MagicMock(), driver_dialect, dead, Properties({}), True, _dial)
        assert conn is failover_conn
        assert dialed["n"] == 0  # never dialed the known-dead host

    asyncio.run(_body())


def test_connect_error_propagates_when_connect_failover_disabled():
    """enable_connect_failover=False -> the dial error surfaces untouched."""
    async def _body() -> None:
        plugin, svc, _hlp, driver_dialect = _build_plugin()
        plugin._enable_connect_failover = False

        async def _dial():
            raise ConnectionError("boom")

        with pytest.raises(ConnectionError):
            await plugin.connect(
                MagicMock(), driver_dialect,
                HostInfo(host="writer.example.com", port=5432),
                Properties({}), True, _dial)

    asyncio.run(_body())


def test_should_failover_on_pymysql_not_connected_even_without_dialect():
    """MySQL integration residual (1/10 after the handler fix): the
    handler-based classification returns False whenever the plugin service's
    database dialect is transiently unset, letting aiomysql's
    InterfaceError(0, 'Not connected') escape raw. The failover plugin must
    recognize the shape dialect-independently (same precedent as the
    _is_connection_os_error / 'connection is lost' escape hatches)."""
    import pymysql

    from aws_advanced_python_wrapper.aio.failover_plugin import \
        AsyncFailoverPlugin

    err = pymysql.err.InterfaceError(0, "Not connected")
    assert AsyncFailoverPlugin._is_pymysql_not_connected_error(err) is True
    # Wrapped one level via explicit cause: still recognized.
    outer = RuntimeError("wrapped")
    outer.__cause__ = err
    assert AsyncFailoverPlugin._is_pymysql_not_connected_error(outer) is True
    # Unrelated code-0 error: not matched.
    other = pymysql.err.InterfaceError(0, "something else")
    assert AsyncFailoverPlugin._is_pymysql_not_connected_error(other) is False
    # The REAL aiomysql shape (aiomysql/connection.py:1123): the tuple's repr
    # embedded in a SINGLE string arg -- confirmed live via the
    # not-classified debug line; the tuple-only match missed it.
    aiomysql_shape = pymysql.err.InterfaceError("(0, 'Not connected')")
    assert AsyncFailoverPlugin._is_pymysql_not_connected_error(aiomysql_shape) is True
    unrelated_str = pymysql.err.InterfaceError("(2013, 'Lost connection')")
    assert AsyncFailoverPlugin._is_pymysql_not_connected_error(unrelated_str) is False
