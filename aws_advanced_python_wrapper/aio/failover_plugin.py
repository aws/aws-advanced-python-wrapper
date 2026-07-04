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

"""Async failover plugin.

Listens on the async plugin pipeline for connection/query failures,
probes topology, and opens a replacement connection against the new
writer (or a reader, per ``failover_mode``). On success raises
``FailoverSuccessError`` so the caller retries its unit of work against
the new connection.

Shares the sync failover plugin's connection properties
(``failover_mode``, ``failover_timeout_sec``, ``enable_failover``).
"""

from __future__ import annotations

import asyncio
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, List, NoReturn,
                    Optional, Set)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.aio.retry_util import AsyncRetryUtil
from aws_advanced_python_wrapper.errors import (
    AwsWrapperError, FailoverFailedError, FailoverSuccessError,
    TransactionResolutionUnknownError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.failover_mode import (FailoverMode,
                                                             get_failover_mode)
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import (
        AsyncHostListProvider, Topology)
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncFailoverPlugin(AsyncPlugin):
    """Async counterpart of :class:`FailoverPlugin`."""

    _SUBSCRIBED: Set[str] = {
        DbApiMethod.CONNECT.method_name,
        DbApiMethod.FORCE_CONNECT.method_name,
        DbApiMethod.CURSOR_EXECUTE.method_name,
        DbApiMethod.CURSOR_EXECUTEMANY.method_name,
        DbApiMethod.CURSOR_FETCHONE.method_name,
        DbApiMethod.CURSOR_FETCHMANY.method_name,
        DbApiMethod.CURSOR_FETCHALL.method_name,
        DbApiMethod.CONNECTION_COMMIT.method_name,
        DbApiMethod.CONNECTION_ROLLBACK.method_name,
    }

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            host_list_provider: AsyncHostListProvider,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._host_list_provider = host_list_provider
        self._props = props
        # Shared writer-failover convergence helper (also reused by the GDB
        # failover subclass). See AsyncRetryUtil.get_writer_connection.
        self._retry_util = AsyncRetryUtil()
        self._enabled = WrapperProperties.ENABLE_FAILOVER.get_bool(props)
        # API parity with sync ``failover_v2_plugin.py``: accept the v2-style
        # ``enable_connect_failover`` toggle alongside the v1-style
        # ``enable_failover``. Gates connect-time failover
        # (``_connect_with_failover``), matching sync v2's gating
        # (failover_v2_plugin.py:140-150): the flag (default False) AND
        # ``enable_failover`` must both be true for connect-time failover to
        # run. Execute-path failover is gated by ``enable_failover`` alone.
        self._enable_connect_failover = (
            WrapperProperties.ENABLE_CONNECT_FAILOVER.get_bool(props))
        # When set, async failover emits a top-level copy of the per-failover
        # telemetry span at FORCE_TOP_LEVEL trace level. Mirrors sync v2
        # behavior at ``failover_v2_plugin.py:81-82,248-252,386-390`` so
        # users with X-Ray/OTLP sampling-by-top-level see failover events.
        self._telemetry_failover_additional_top_trace = (
            WrapperProperties.TELEMETRY_FAILOVER_ADDITIONAL_TOP_TRACE.get_bool(props))
        timeout = WrapperProperties.FAILOVER_TIMEOUT_SEC.get_float(props)
        self._failover_timeout_sec = float(timeout) if timeout is not None else 300.0
        self._mode = self._determine_mode(props)
        # Snapshot of plugin_service.is_in_transaction taken at the start of
        # each execute, so a failover triggered by this op knows whether the
        # caller was mid-transaction (see execute / _raise_*).
        self._is_in_transaction: bool = False

        # Telemetry counters -- match sync failover_plugin.py:103-113.
        # NullTelemetryFactory returns a no-op counter object, but real
        # factories may return None, so every .inc() guards with an
        # ``is not None`` check (mirrors sync conventions).
        tf = self._plugin_service.get_telemetry_factory()
        self._failover_writer_triggered = tf.create_counter(
            "writer_failover.triggered.count")
        self._failover_writer_completed = tf.create_counter(
            "writer_failover.completed.success.count")
        self._failover_writer_failed = tf.create_counter(
            "writer_failover.completed.failed.count")
        self._failover_reader_triggered = tf.create_counter(
            "reader_failover.triggered.count")
        self._failover_reader_completed = tf.create_counter(
            "reader_failover.completed.success.count")
        self._failover_reader_failed = tf.create_counter(
            "reader_failover.completed.failed.count")

    @staticmethod
    def _determine_mode(props: Properties) -> FailoverMode:
        mode = get_failover_mode(props)
        return mode if mode is not None else FailoverMode.STRICT_WRITER

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        conn = await self._connect_with_failover(
            driver_dialect, host_info, connect_func)

        # Eagerly populate the topology cache from the LIVE initial connection,
        # mirroring sync failover_v2_plugin.py:177-178
        # (``if is_initial_connection: self._plugin_service.refresh_host_list(conn)``).
        #
        # Without this the host list stays at the single seed host until the
        # background topology monitor's first refresh. If the app loses that
        # connection before the monitor has run -- e.g. a reader is disabled
        # within milliseconds of connecting in test_fail_from_reader_to_writer --
        # failover has only the seed host to work with: force_refresh through the
        # now-dead connection returns nothing, _do_failover falls back to the lone
        # initial host (defaulted to WRITER), and the writer loop retries the dead
        # host until the failover timeout instead of discovering the real writer.
        # Refreshing here means the full [writer, reader,...] topology is cached
        # before any failure, so a later force_refresh can fall back to it.
        #
        # Best-effort: a topology probe failure must not fail the initial connect
        # (the monitor / a later force_refresh will repopulate it).
        if is_initial_connection and self._enabled:
            try:
                await self._plugin_service.refresh_host_list(conn)
            except Exception:  # noqa: BLE001 - topology will be (re)fetched later
                pass

        return conn

    async def _connect_with_failover(
            self,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        """Connect-time failover (sync failover_v2_plugin.py:127-180 parity).

        With ``enable_connect_failover`` (and failover) enabled: a
        failover-worthy connect error marks the target host UNAVAILABLE and
        runs failover instead of surfacing the error; a target already known
        to be UNAVAILABLE skips the doomed dial entirely and fails over
        directly (after a topology refresh). FailoverFailedError propagates,
        matching sync. Stale-DNS verification is not embedded here -- async
        ships it as the separate stale_dns plugin.
        """
        if not (self._enabled and self._enable_connect_failover):
            return await connect_func()

        known = next(
            (h for h in self._plugin_service.hosts
             if h.host == host_info.host and h.port == host_info.port),
            None)

        if known is None or known.get_availability() != HostAvailability.UNAVAILABLE:
            try:
                return await connect_func()
            except Exception as e:
                if not self._should_failover(e):
                    raise
                self._plugin_service.set_availability(
                    host_info.as_aliases(), HostAvailability.UNAVAILABLE)
                await self._do_failover(driver_dialect)
        else:
            # Known-dead target: refresh topology and fail over without
            # dialing it (sync :168-172).
            try:
                await self._plugin_service.refresh_host_list()
            except Exception:  # noqa: BLE001 - failover refreshes again itself
                pass
            await self._do_failover(driver_dialect)

        conn = self._plugin_service.current_connection
        if conn is None:
            # Sync raises the same bare message at failover_v2_plugin.py:174.
            raise AwsWrapperError("Unable to connect")
        return conn

    async def execute(
            self,
            target: object,
            method_name: str,
            execute_func: Callable[..., Awaitable[Any]],
            *args: Any,
            **kwargs: Any) -> Any:
        if not self._enabled:
            return await execute_func()
        # Capture transaction state BEFORE running this op (parity with sync
        # failover_v2.execute:101). After failover the connection is a fresh,
        # idle writer, so probing it post-failover always reports "not in a
        # transaction"; the pre-op flag (maintained by the DefaultPlugin) is
        # what determines whether the transaction's fate is unknown.
        self._is_in_transaction = self._plugin_service.is_in_transaction
        try:
            return await execute_func()
        except Exception as exc:
            if not self._should_failover(exc):
                raise
            await self._do_failover(driver_dialect=self._plugin_service.driver_dialect)
            await self._raise_failover_success_or_txn_unknown(exc)

    def _should_failover(self, exc: Exception) -> bool:
        """Decide whether ``exc`` indicates a failover-worthy error.

        Mirrors sync v2 _should_exception_trigger_connection_switch at
        failover_v2_plugin.py:416-427: delegate to the dialect-aware
        ExceptionHandler through the plugin service, with a STRICT_WRITER
        escape hatch for read-only-connection exceptions (promotes stale
        read-only replicas to failover triggers).
        """
        # Avoid catching our own failover signals.
        if isinstance(exc, (FailoverSuccessError, FailoverFailedError)):
            return False
        if self._plugin_service.is_network_exception(error=exc):
            return True
        # Async-specific connection-loss signal. When the EFM monitor (or a
        # network-outage proxy) closes the connection's socket while a query is
        # awaiting on its file descriptor, psycopg's asyncio wait surfaces a raw
        # OSError (e.g. ``[Errno 9] Bad file descriptor``) from the event-loop
        # selector rather than a psycopg ``OperationalError``. Sync never hits
        # this: it aborts from a separate thread, so the blocked query returns a
        # clean OperationalError that ``is_network_exception`` already classifies
        # (test_fail_from_reader_to_writer). The failover plugin only wraps DB
        # operations, where an OSError means the underlying socket is gone -- a
        # failover-worthy connection failure.
        if self._is_connection_os_error(exc):
            return True
        # Async-specific connection-loss MESSAGE. The shared PG exception handler
        # (utils/pg_exception_handler.py) classifies "the connection is closed",
        # "connection socket closed", etc. as network errors -- but NOT "the
        # connection is lost", which is precisely the message psycopg's
        # AsyncConnection raises when the socket dies mid-failover (sync psycopg
        # surfaces different wording, so the shared list never needed it). Without
        # this, a writer-change failover where the connection drops on the next
        # query lets that OperationalError propagate raw instead of triggering
        # failover (test_fail_from_writer_to_new_writer_*).
        if self._is_connection_lost_error(exc):
            return True
        return (self._is_strict_writer_failover_mode()
                and self._plugin_service.is_read_only_connection_exception(error=exc))

    def _is_strict_writer_failover_mode(self) -> bool:
        """Whether STRICT_WRITER failover applies (gates the read-only-exception
        escape hatch in :meth:`_should_failover`).

        Extracted as a method so the GDB failover subclass can make it
        region-aware. Mirrors sync ``FailoverV2Plugin._is_strict_writer_failover_mode``.
        """
        return self._mode == FailoverMode.STRICT_WRITER

    @staticmethod
    def _is_connection_os_error(exc: BaseException) -> bool:
        """True if ``exc`` (or anything it was raised ``from``) is an OSError.

        Walks the explicit ``__cause__`` chain so a connection-loss OSError
        wrapped by an inner layer still counts; ``__context__`` is deliberately
        ignored to avoid implicit-chaining false positives.
        """
        seen: set = set()
        cur: Optional[BaseException] = exc
        while cur is not None and id(cur) not in seen:
            if isinstance(cur, OSError):
                return True
            seen.add(id(cur))
            cur = cur.__cause__
        return False

    @staticmethod
    def _is_connection_lost_error(exc: BaseException) -> bool:
        """True if ``exc`` (or anything it was raised ``from``) carries the
        async-psycopg "the connection is lost" message.

        Walks the ``__cause__`` chain (like :meth:`_is_connection_os_error`) and
        matches the message text. Kept narrow -- a single specific phrase the
        shared handler omits -- to avoid broadening failover triggers.
        """
        seen: set = set()
        cur: Optional[BaseException] = exc
        while cur is not None and id(cur) not in seen:
            args = getattr(cur, "args", None)
            if args:
                msg = args[0]
                if isinstance(msg, str) and "the connection is lost" in msg:
                    return True
            seen.add(id(cur))
            cur = cur.__cause__
        return False

    async def _raise_failover_success_or_txn_unknown(
            self, original_exc: Exception) -> NoReturn:
        """Signal successful failover with the right exception type.

        Mirrors sync v2 _throw_failover_success_exception at
        failover_v2_plugin.py:312-321. If the caller was mid-transaction,
        their transaction's fate is unknown -- raise
        TransactionResolutionUnknownError so they don't blindly retry.
        Otherwise raise FailoverSuccessError so they can retry cleanly.
        """
        # Use the transaction state captured BEFORE failover (in execute) or
        # the service's tracked flag -- NOT a probe of current_connection,
        # which post-failover is a fresh idle writer and would always say
        # "not in a transaction". Mirrors sync _throw_failover_success_exception
        # (failover_v2_plugin.py:313).
        in_txn = self._is_in_transaction or self._plugin_service.is_in_transaction
        if in_txn:
            # The transaction was rolled back by the failover; clear the flag so
            # the caller's next op starts clean.
            try:
                await self._plugin_service.update_in_transaction(False)
            except Exception:  # noqa: BLE001
                pass
            raise TransactionResolutionUnknownError(
                Messages.get("FailoverPlugin.TransactionResolutionUnknownError")
            ) from original_exc
        raise FailoverSuccessError(
            Messages.get("FailoverPlugin.ConnectionChangedError")
        ) from original_exc

    async def _do_failover(self, driver_dialect: AsyncDriverDialect) -> None:
        """Dispatch failover based on failover_mode.

        Mirrors sync v2 _failover_reader / _failover_writer at
        failover_v2_plugin.py:254-310 / :323-390.
        """
        # Mark the current (failed) host UNAVAILABLE first so selection
        # strategies deprioritize it during the loops below (sync v2 marks it
        # in _deal_with_original_exception, failover_v2_plugin.py:161).
        failed_host = self._plugin_service.current_host_info
        if failed_host is not None:
            self._plugin_service.set_availability(
                failed_host.as_aliases(), HostAvailability.UNAVAILABLE)

        # Invalidate the current (likely broken) connection before
        # attempting failover so any pool-proxied target is evicted from
        # the owning pool. Mirrors sync v2 _invalidate_current_connection
        # called from _failover at ``failover_v2_plugin.py:201``.
        await self._invalidate_current_connection()

        deadline = asyncio.get_running_loop().time() + self._failover_timeout_sec
        last_error: Optional[BaseException] = None

        # STRICT_WRITER discovery is monitor-first (sync parity:
        # failover_v2_plugin.py:333 blocks on
        # force_monitoring_refresh_host_list(True, timeout) BEFORE the retry
        # loop -- the monitor's panic fan-out finds the promoted writer on
        # monitor-owned connections; sync never queries topology through the
        # app's dead connection). Unlike sync's hard raise on failure, we fall
        # through to the fallback chain below so monitor-less providers
        # (static dialects, test doubles) still fail over.
        topology: Topology = ()
        if self._mode == FailoverMode.STRICT_WRITER:
            try:
                remaining = max(0.0, deadline - asyncio.get_running_loop().time())
                refreshed = await self._within_deadline(
                    self._plugin_service.force_monitoring_refresh_host_list(
                        True, remaining),
                    deadline)
                if refreshed:
                    topology = self._plugin_service.all_hosts
            except Exception as e:  # noqa: BLE001 - fall through to fallbacks
                last_error = e

        if not topology:
            try:
                topology = await self._within_deadline(
                    self._host_list_provider.force_refresh(
                        self._plugin_service.current_connection),
                    deadline)
            except Exception as e:  # noqa: BLE001 - refresh failure is recoverable
                topology = ()
                last_error = e

        # When the live refresh returns nothing (it ran through the now-dead
        # connection), fall back to the cached full topology so the failover
        # handlers can reach a surviving reader and converge on the new writer.
        # Mirrors sync, whose failover operates on ``plugin_service.all_hosts``
        # (failover_plugin.py:323) rather than a single host.
        if not topology:
            topology = self._plugin_service.all_hosts
        # Last resort: the single host the user originally connected to.
        if not topology and self._plugin_service.initial_connection_host_info is not None:
            topology = (self._plugin_service.initial_connection_host_info,)

        if self._mode == FailoverMode.STRICT_WRITER:
            await self._failover_writer(topology, driver_dialect, deadline, last_error)
        else:
            await self._failover_reader(topology, driver_dialect, deadline, last_error)

    async def _within_deadline(self, coro: Any, deadline: float) -> Any:
        """Await ``coro`` but never past ``deadline``.

        The reader/writer loops check ``deadline`` only between iterations, so
        an individual await -- topology ``force_refresh`` or a per-candidate
        ``_open_connection`` -- can block indefinitely on a blackholed /
        unreachable host (no connect timeout) and never return to the deadline
        check, hanging until the socket / pytest timeout. Bounding each await by
        the remaining budget keeps ``failover_timeout_sec`` real; on expiry it
        raises ``asyncio.TimeoutError``, which the loops treat like any other
        failed attempt and fall through to ``FailoverFailedError``.
        """
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            close = getattr(coro, "close", None)
            if callable(close):
                close()  # avoid 'coroutine was never awaited' warning
            raise asyncio.TimeoutError()
        return await asyncio.wait_for(coro, timeout=remaining)

    async def _failover_reader(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        if self._failover_reader_triggered is not None:
            self._failover_reader_triggered.inc()
        # Open a telemetry span around the failover-to-reader operation;
        # mirrors sync v2 at ``failover_v2_plugin.py:218-219``. Closed in
        # the finally block below.
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            "failover to replica", TelemetryTraceLevel.NESTED)
        try:
            await self._failover_reader_impl(
                topology, driver_dialect, deadline, last_error)
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    telemetry_factory.post_copy(
                        telemetry_context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    async def _failover_reader_impl(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        reader_candidates = [h for h in topology if h.role == HostRole.READER]
        original_writer = next(
            (h for h in topology if h.role == HostRole.WRITER), None)
        is_original_writer_still_writer = False
        strategy = (WrapperProperties.FAILOVER_READER_HOST_SELECTOR_STRATEGY
                    .get(self._props) or "random")

        while asyncio.get_running_loop().time() < deadline:
            remaining = list(reader_candidates)
            while remaining and asyncio.get_running_loop().time() < deadline:
                try:
                    candidate = self._plugin_service.get_host_info_by_strategy(
                        HostRole.READER, strategy, remaining)
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    break
                if candidate is None:
                    break

                try:
                    new_conn = await self._within_deadline(
                        self._open_connection(candidate, driver_dialect), deadline)
                except Exception as e:  # noqa: BLE001
                    self._plugin_service.set_availability(
                        candidate.as_aliases(), HostAvailability.UNAVAILABLE)
                    last_error = e
                    if candidate in remaining:
                        remaining.remove(candidate)
                    continue

                # Data-plane role verification (sync failover_v2:275-289): the
                # topology labeled this host a reader, but right after failover
                # the label can be stale -- in STRICT_READER we must not hand
                # back a promoted writer. Bind the VERIFIED role.
                role = await self._probe_role(new_conn)
                if role is None:
                    # Probe failed: drop the candidate for this pass (sync
                    # failover_v2:288-289; the close is additive -- sync leaves
                    # the conn to GC, closing is strictly safer).
                    if candidate in remaining:
                        remaining.remove(candidate)
                    await self._close_quietly(new_conn)
                    continue
                if role == HostRole.READER or self._mode != FailoverMode.STRICT_READER:
                    verified = HostInfo(candidate.host, candidate.port, role)
                    self._plugin_service.set_availability(
                        candidate.as_aliases(), HostAvailability.AVAILABLE)
                    await self._plugin_service.set_current_connection(new_conn, verified)
                    if self._failover_reader_completed is not None:
                        self._failover_reader_completed.inc()
                    return

                # STRICT_READER and the live role is not READER: reject.
                if candidate in remaining:
                    remaining.remove(candidate)
                await self._close_quietly(new_conn)
                if role == HostRole.WRITER and candidate in reader_candidates:
                    # It is the new writer -- drop it from future passes.
                    reader_candidates.remove(candidate)

            # Readers exhausted for this pass. Try the original writer -- also
            # in STRICT_READER (a demoted old writer now serves as a reader),
            # unless we already verified it is still the writer (sync
            # failover_v2:292-308).
            if (original_writer is None
                    or asyncio.get_running_loop().time() >= deadline
                    or (self._mode == FailoverMode.STRICT_READER
                        and is_original_writer_still_writer)):
                await asyncio.sleep(1.0)
                continue

            try:
                new_conn = await self._within_deadline(
                    self._open_connection(original_writer, driver_dialect), deadline)
            except Exception as e:  # noqa: BLE001
                self._plugin_service.set_availability(
                    original_writer.as_aliases(), HostAvailability.UNAVAILABLE)
                last_error = e
            else:
                role = await self._probe_role(new_conn)
                if role is None:
                    # Probe failed: neither accept nor mark still-writer --
                    # sync failover_v2:307-308 (`except: pass`) just moves on.
                    await self._close_quietly(new_conn)
                elif role == HostRole.READER or self._mode != FailoverMode.STRICT_READER:
                    verified = HostInfo(
                        original_writer.host, original_writer.port, role)
                    self._plugin_service.set_availability(
                        original_writer.as_aliases(), HostAvailability.AVAILABLE)
                    await self._plugin_service.set_current_connection(
                        new_conn, verified)
                    if self._failover_reader_completed is not None:
                        self._failover_reader_completed.inc()
                    return
                else:
                    await self._close_quietly(new_conn)
                    if role == HostRole.WRITER:
                        is_original_writer_still_writer = True

            await asyncio.sleep(1.0)

        if self._failover_reader_failed is not None:
            self._failover_reader_failed.inc()
        raise FailoverFailedError(
            Messages.get("FailoverPlugin.UnableToConnectToReader")
        ) from last_error

    async def _probe_role(self, conn: Any) -> Optional[HostRole]:
        """Data-plane role probe. Returns ``None`` when the probe fails --
        sync parity (failover_v2_plugin.py:277/288 and :298/307): a role-probe
        error DROPS the candidate for this pass rather than accepting it under
        an assumed role."""
        try:
            role = await self._plugin_service.get_host_role(conn)
        except Exception:  # noqa: BLE001 - probe failure -> drop candidate
            return None
        return role if isinstance(role, HostRole) else None

    @staticmethod
    async def _close_quietly(conn: Any) -> None:
        try:
            close_result = conn.close()
            if asyncio.iscoroutine(close_result):
                await close_result
        except Exception:  # noqa: BLE001 - rejected candidate, best-effort close
            pass

    async def _failover_writer(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        if self._failover_writer_triggered is not None:
            self._failover_writer_triggered.inc()
        # Open a telemetry span around the failover-to-writer operation;
        # mirrors sync v2 at ``failover_v2_plugin.py:325-326``. Closed in
        # the finally block below.
        telemetry_factory = self._plugin_service.get_telemetry_factory()
        telemetry_context = telemetry_factory.open_telemetry_context(
            "failover to writer host", TelemetryTraceLevel.NESTED)
        try:
            await self._failover_writer_impl(
                topology, driver_dialect, deadline, last_error)
        finally:
            if telemetry_context is not None:
                telemetry_context.close_context()
                if self._telemetry_failover_additional_top_trace:
                    telemetry_factory.post_copy(
                        telemetry_context, TelemetryTraceLevel.FORCE_TOP_LEVEL)

    async def _failover_writer_impl(
            self,
            topology: Topology,
            driver_dialect: AsyncDriverDialect,
            deadline: float,
            last_error: Optional[BaseException]) -> None:
        # The writer-acquisition + convergence loop lives in the shared helper
        # (also used by AsyncGdbFailoverPlugin's STRICT_WRITER mode). This method
        # keeps the plugin-owned concerns: binding the connection and the
        # success/failure telemetry counters.
        try:
            result = await self._retry_util.get_writer_connection(
                self._plugin_service,
                self._host_list_provider,
                lambda host: self._open_connection(host, driver_dialect),
                topology,
                deadline)
        except TimeoutError as e:
            if self._failover_writer_failed is not None:
                self._failover_writer_failed.inc()
            raise FailoverFailedError(
                Messages.get("FailoverPlugin.UnableToConnectToWriter")
            ) from (last_error or e)
        await self._plugin_service.set_current_connection(result.connection, result.host_info)
        if self._failover_writer_completed is not None:
            self._failover_writer_completed.inc()

    async def _invalidate_current_connection(self) -> None:
        """Invalidate the current connection prior to attempting failover.

        For pool-proxied targets (SQLAlchemy's async ``_ConnectionFairy``),
        marks the pool record as invalidated so the next checkout returns
        a fresh connection instead of handing back this (likely broken)
        one. Mirrors sync ``failover_plugin._invalidate_current_connection``:

        1) Null out the record's ``dbapi_connection`` BEFORE invalidating.
           With ``soft=True``, SA leaves the dbapi_connection attached and
           on the next checkout calls ``__close(terminate=False)`` on it --
           which on a connection whose libpq socket died during failover
           raises "the connection is lost" inside do_rollback. Clearing
           the reference here makes SA take the "create new" branch on
           next checkout.
        2) Use ``inv(soft=True)`` rather than ``inv()`` so the underlying
           driver connection is NOT torn down on this thread. The async
           event loop doesn't share the sync wrapper's libpq-thread race
           surface, but the dead-conn rollback issue is the same and the
           ``soft=True`` keeps the recycle semantics identical to sync.
        """
        conn = self._plugin_service.current_connection
        if conn is None:
            return
        # See sync ``failover_plugin.py:_invalidate_current_connection`` for
        # the full rationale and SA-version caveat. ``hasattr`` + nested
        # ``try/except AttributeError`` keep us safe across SA layout
        # changes (we're bound to ``SQLAlchemy = "^2.0.49"`` per
        # ``pyproject.toml``).
        inv = getattr(conn, "invalidate", None)
        if callable(inv):
            try:
                record = getattr(conn, "_connection_record", None)
                if record is not None and hasattr(record, "dbapi_connection"):
                    try:
                        if record.dbapi_connection is not None:
                            record.dbapi_connection = None
                    except AttributeError:
                        pass
                result = inv(soft=True)
                if asyncio.iscoroutine(result):
                    await result
                return
            except TypeError:
                # ``invalidate`` exists but doesn't accept ``soft=`` --
                # fall through to ``.close()`` rather than calling
                # ``inv()`` with synchronous teardown.
                pass
            except Exception:  # noqa: BLE001 - fall through to close
                pass
        close = getattr(conn, "close", None)
        if close is None:
            return
        try:
            result = close()
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001 - best-effort
            pass

    async def _open_connection(
            self,
            target: HostInfo,
            driver_dialect: AsyncDriverDialect) -> Any:
        """Open a connection to ``target`` through the plugin pipeline,
        skipping THIS failover plugin to avoid recursive failover-on-failover.

        Routes through ``plugin_service.force_connect(target, props, self)`` --
        NOT ``connect`` and NOT a raw ``driver_dialect.connect``. ``force_connect``
        still runs the plugin pipeline, so the auth plugins (IAM, Secrets,
        Federated, Okta) re-apply on the reconnect -- a raw connect would omit
        the IAM token (the IAM plugin generates it per-connect; it is NOT stored
        in props), failing the writer reconnect with ``PAM authentication
        failed`` (test_failover_with_iam). But unlike ``connect``, ``force_connect``
        uses the DEFAULT driver provider, bypassing any custom (pooled) provider.
        The failover reconnect must yield a fresh, non-pooled connection: routing
        it through the pooled provider creates internal pools on the failover
        target, which breaks the pooled-failover tests -- a cluster-URL
        connection must stay unpooled even after failover
        (test_pooled_connection__cluster_url_failover) and a pooled connection
        must be replaced by a non-pooled one after failover
        (test_pooled_connection__failover). ``self`` as plugin_to_skip excludes
        the failover plugin from this connect so a connect failure can't
        recursively trigger failover.
        """
        return await self._plugin_service.force_connect(
            target, self._plugin_service.props, self)

    def _build_target_props(self, target: HostInfo) -> Properties:
        props_copy = self._plugin_service.props.copy()  # type: ignore[attr-defined]
        props_copy["host"] = target.host
        if target.is_port_specified():
            props_copy["port"] = str(target.port)
        return props_copy  # type: ignore[return-value]

    def notify_host_list_changed(self, changes: Any) -> None:
        """Host list changes from the topology monitor are cached on the
        provider already; failover reads live topology via force_refresh
        so no action needed here."""
        return None


# Re-export for parity with sync failover plugin's public surface.
__all__ = ["AsyncFailoverPlugin"]


# The following attrs keep mypy happy when AsyncFailoverPlugin is used
# with asyncio.get_event_loop in older Python versions.
_unused: List[str] = []
