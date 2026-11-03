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

"""Async host list providers.

:class:`AsyncHostListProvider` is the async counterpart of
:class:`HostListProvider`. The module ships the Protocol plus four concrete
implementations:

* :class:`AsyncStaticHostListProvider` -- returns a fixed list built from
  connection props; used when no Aurora topology is needed (direct RDS
  instance, test environments).
* :class:`AsyncAuroraHostListProvider` -- queries the Aurora
  ``aurora_replica_status`` view over the current async connection and
  caches the result.
* :class:`AsyncMultiAzHostListProvider` -- RDS Multi-AZ DB cluster variant
  (different topology query/parsing).
* :class:`AsyncGlobalAuroraHostListProvider` -- Aurora Global Database
  variant, aware of cross-region topology.

A background refresh loop (:class:`AsyncClusterTopologyMonitor`) lives in
:mod:`aws_advanced_python_wrapper.aio.cluster_topology_monitor` and drives
:meth:`AsyncAuroraHostListProvider.force_refresh` on an interval.
"""

from __future__ import annotations

import asyncio
from copy import copy
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional,
                    Protocol, Tuple, runtime_checkable)

from aws_advanced_python_wrapper.aio import topology_store
from aws_advanced_python_wrapper.aio.cleanup import (register_shutdown_hook,
                                                     unregister_shutdown_hook)
from aws_advanced_python_wrapper.aio.topology_utils import (
    AsyncAuroraTopologyUtils, AsyncGlobalAuroraTopologyUtils,
    AsyncMultiAzTopologyUtils, AsyncTopologyUtils)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils import services_container
from aws_advanced_python_wrapper.utils.events import MonitorStopEvent
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

logger = Logger(__name__)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.topology_store import TopologyKey
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.utils.properties import Properties


Topology = Tuple[HostInfo, ...]


@runtime_checkable
class AsyncHostListProvider(Protocol):
    """Async host list provider contract."""

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return the current topology, possibly using a cache."""
        ...

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return the current topology, bypassing any cache."""
        ...

    def get_cluster_id(self) -> str:
        """Return a stable cluster identifier for pool-key generation."""
        ...

    async def stop(self) -> None:
        """Release any background tasks this provider owns."""
        ...


@runtime_checkable
class AsyncHostListProviderService(Protocol):
    """Async counterpart of :class:`HostListProviderService`
    (host_list_provider.py:107): the surface the ``init_host_provider``
    pipeline hands to plugins so they can observe, validate, or replace
    the selected host list provider before the initial connect.
    ``AsyncPluginServiceImpl`` implements it.
    """

    @property
    def current_connection(self) -> Optional[Any]:
        ...

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        ...

    @property
    def driver_dialect(self) -> AsyncDriverDialect:
        ...

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        ...

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        ...

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: HostInfo) -> None:
        ...

    def is_static_host_list_provider(self) -> bool:
        ...

    @property
    def is_dialect_confirmed(self) -> bool:
        ...

    def get_target_driver_func(self) -> Optional[Callable]:
        ...


class AsyncStaticHostListProvider:
    """Static host list -- one host built from connection props.

    Used for plain RDS instances, local test environments, or any case
    where the wrapper does not need to discover cluster topology.
    """

    def __init__(self, props: Properties) -> None:
        host = props.get("host", "")
        port_raw = props.get("port")
        port = int(port_raw) if port_raw is not None else -1
        self._host_info = HostInfo(host=host, port=port, role=HostRole.WRITER)
        self._cluster_id = f"static:{host}:{port}"

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        return (self._host_info,)

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        return (self._host_info,)

    def get_cluster_id(self) -> str:
        return self._cluster_id

    async def stop(self) -> None:
        return None


class AsyncAuroraHostListProvider:
    """Aurora topology discovery over an async driver connection.

    The query and its row parsing live on an :class:`AsyncTopologyUtils`, so
    the topology monitor can query the cluster without holding a provider.
    Every function and operator in a PostgreSQL query the driver issues is
    pinned to ``pg_catalog`` so the session's ``search_path`` cannot redirect
    it.

    :meth:`refresh` returns the stored topology when there is one;
    :meth:`force_refresh` goes through the cluster's topology monitor, so
    panic-mode probing engages when writer discovery stalls on initial
    connect. The monitor is shared per cluster and event loop, and the
    topology it publishes lives in the storage service.
    """

    _DEFAULT_REFRESH_RATE_NS = 30 * 1_000_000_000  # 30 seconds
    _MONITOR_EXPIRATION_NS = 15 * 60 * 1_000_000_000  # 15 minutes
    _MONITOR_INACTIVE_TIMEOUT_NS = 3 * 60 * 1_000_000_000  # 3 minutes

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            topology_query: str = (
                "SELECT SERVER_ID, "
                "SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' AS IS_WRITER "
                "FROM pg_catalog.aurora_replica_status() "
                "WHERE EXTRACT(EPOCH FROM (pg_catalog.NOW() "
                "OPERATOR(pg_catalog.-) LAST_UPDATE_TIMESTAMP)) "
                "OPERATOR(pg_catalog.<=) 300 "
                "OR SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' "
                # A reader whose replica status hasn't reported yet has a NULL
                # LAST_UPDATE_TIMESTAMP -- the freshness check above is NULL
                # (not TRUE) for it, so without this clause every such reader is
                # dropped and the topology collapses to writer-only. The failover
                # host list is then stranded at one host and a writer outage
                # yields FailoverFailedError (fail_from_reader_to_writer,
                # failover_with_iam, failover_with_secrets_manager). Mirrors the
                # sync Aurora-PG topology query.
                "OR LAST_UPDATE_TIMESTAMP IS NULL"
            ),
            cluster_id: Optional[str] = None,
            default_port: int = 5432,
            monitor_connection_factory: Optional[
                Callable[[], Awaitable[Any]]] = None,
            topology_utils: Optional[AsyncTopologyUtils] = None,
            provider_service: Optional[AsyncHostListProviderService] = None) -> None:
        self._provider_service = provider_service
        self._props = props
        self._driver_dialect = driver_dialect
        self._default_port = default_port
        self._monitor_connection_factory = monitor_connection_factory
        self._cluster_id = cluster_id or self._derive_cluster_id(props)
        self._refresh_lock = asyncio.Lock()
        refresh_ms = WrapperProperties.TOPOLOGY_REFRESH_MS.get(props)
        self._refresh_ns: int = (
            int(refresh_ms) * 1_000_000 if refresh_ms is not None
            else self._DEFAULT_REFRESH_RATE_NS
        )
        # Owns this dialect's topology query and row parsing. Subclasses pass
        # their own; the monitor is handed the same object so it can query the
        # cluster without a provider.
        self._topology_utils: AsyncTopologyUtils = (
            topology_utils if topology_utils is not None
            else AsyncAuroraTopologyUtils(props, topology_query, default_port))
        self._last_conn: Optional[Any] = None

        # produced_data_type is what links a topology read to this monitor:
        # the monitor service turns a read of the stored topology into an
        # expiration extension for the monitor that produces it.
        from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
            AsyncClusterTopologyMonitor
        self._monitor_service = services_container.get_monitor_service()
        # The monitor stamps its activity once per tick, so the inactive
        # timeout has to outlast a tick, and topology_refresh_ms is
        # user-settable and can exceed the default on its own.
        self._monitor_service.register_monitor_type(
            AsyncClusterTopologyMonitor,
            expiration_timeout_ns=self._MONITOR_EXPIRATION_NS,
            inactive_timeout_ns=max(self._MONITOR_INACTIVE_TIMEOUT_NS,
                                    2 * self._refresh_ns),
            produced_data_type=topology_store.StoredTopology)
        # Panic-mode probe: opens a pipeline connection to a host and
        # classifies its role, used by the monitor to discover the new
        # writer when the monitoring connection dies (mirrors sync
        # ClusterTopologyMonitor's per-host HostMonitor threads). Wired by
        # the wrapper once the plugin service exists -- see
        # aio/wrapper.py and cluster_topology_monitor.build_probe_host.
        self._probe_host: Optional[Callable[..., Any]] = None

    def _is_dialect_confirmed(self) -> bool:
        """Whether the database dialect is finalized, and so whether topology may be queried yet.
        """
        if self._provider_service is None:
            return True
        return self._provider_service.is_dialect_confirmed

    def _seed_topology(self) -> Topology:
        """The host from the connection properties, reported until the dialect
        is confirmed.

        Taken from the topology utils rather than rebuilt here, so it carries
        the cluster's availability strategy and the same port resolution every
        topology host gets. Copied on each call because the default
        availability strategy never expires a mark: handing out the one object
        would let a single failed connect pin the seed host UNAVAILABLE for
        the life of the process, with nothing to clear it.
        """
        stored = self._get_stored_topology()
        if stored:
            return stored
        return (copy(self._topology_utils.initial_host_info),)

    def set_probe_host(self, probe_host: Callable[..., Any]) -> None:
        """Arm panic-mode writer discovery on the (future) topology monitor.

        Must be called before the monitor is lazily created (the wrapper
        calls it right after the plugin service is constructed, well before
        the first refresh). A monitor created earlier keeps running without
        panic mode -- same behavior as before wiring existed.
        """
        self._probe_host = probe_host

    @staticmethod
    def _derive_cluster_id(props: Properties) -> str:
        """Identify the cluster this provider is talking to.

        The ``cluster_id`` property, which defaults to ``"1"``, as the
        synchronous provider and the reference implementation both do.
        Deriving one from the endpoint was tried and abandoned upstream.

        Connections sharing an id share a topology entry and a topology
        monitor, so an application connecting to more than one cluster has to
        set it per cluster. Without that, the second cluster reuses the
        first's monitor -- and therefore the first's topology query and
        monitoring credentials -- and a blue/green reset for either resets
        both.
        """
        return str(WrapperProperties.CLUSTER_ID.get(props) or "1")

    def get_cluster_id(self) -> str:
        return self._cluster_id

    def _storage_key(self) -> Optional[TopologyKey]:
        return topology_store.topology_key(self._cluster_id)

    def _get_stored_topology(self) -> Optional[Topology]:
        return topology_store.get_topology(self._storage_key())

    def _store_topology(self, topology: Topology) -> None:
        topology_store.put_topology(self._storage_key(), topology)

    def _discard_stored_topology(self) -> None:
        topology_store.remove_topology(self._storage_key())

    async def refresh(self, connection: Optional[Any] = None) -> Topology:
        """Return the stored topology if there is one, else force a refresh.

        Matches sync :meth:`RdsHostListProvider.refresh`: a present entry is
        returned and the topology monitor is what keeps it fresh. No second
        freshness window here, on purpose -- the store already expires
        entries.
        """
        if connection is not None:
            self._last_conn = connection
        stored = self._get_stored_topology()
        if stored:
            return stored
        return await self.force_refresh(connection)

    async def force_refresh(self, connection: Optional[Any] = None) -> Topology:
        """Run the topology query and update the cache.

        Delegates to :class:`AsyncClusterTopologyMonitor.force_refresh_with_connection`
        so panic-mode probing engages when writer discovery stalls --
        sync parity with :meth:`RdsHostListProvider._force_refresh_monitor`.
        Falls back to a direct DB query if the monitor can't be built
        (e.g., no connection available and no cache).
        """
        if connection is not None:
            self._last_conn = connection

        if not self._is_dialect_confirmed():
            # No topology query and no monitor until the dialect is
            # confirmed, so the guessed dialect's SQL is never run.
            return self._seed_topology()

        if connection is None:
            return self._get_stored_topology() or ()

        async with self._refresh_lock:
            monitor = self._get_or_create_monitor()
            if monitor is None:
                # Monitor couldn't be built; fall back to direct query.
                return await self._fetch_and_cache(connection)
            try:
                topology = await monitor.force_refresh_with_connection(
                    connection, bypass_ignore_window=True)
            except Exception:  # noqa: BLE001 - monitor failure
                return await self._fetch_and_cache(connection)
            self._store_topology(topology)
            return topology or (self._get_stored_topology() or ())

    async def force_monitoring_refresh(
            self,
            should_verify_writer: bool,
            timeout_sec: float) -> Topology:
        """Blocking monitor-driven refresh -- sync parity with
        :meth:`RdsHostListProvider.force_monitoring_refresh` (:278-281):
        delegates discovery to the topology monitor (panic-mode probe fan-out
        on monitor-owned connections when ``should_verify_writer``) instead of
        querying through a caller connection. Falls back to
        :meth:`force_refresh` when no monitor can be built (static dialects).
        """
        if not self._is_dialect_confirmed():
            # No topology query and no monitor until the dialect is
            # confirmed, so the guessed dialect's SQL is never run.
            return self._seed_topology()
        monitor = self._get_or_create_monitor()
        if monitor is None:
            return await self.force_refresh(self._last_conn)
        topology = await monitor.force_monitoring_refresh(
            should_verify_writer, timeout_sec)
        # _publish_topology already adopted non-empty results into the cache.
        return topology or ()

    def _get_or_create_monitor(self) -> Optional[Any]:
        """Get the cluster's topology monitor from the monitor service.

        One monitor serves every provider on the cluster and loop, so a
        dialect upgrade or a second connection reuses it rather than starting
        a second background task.

        No ``await`` between acquiring the monitor and re-checking it, on
        purpose: ``run_if_absent``, ``is_running`` and the removal are all
        synchronous, so nothing can interleave. Making this method async
        would reopen that gap.
        """
        key = self._storage_key()
        if key is None or not self._topology_utils.topology_query:
            return None
        from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
            AsyncClusterTopologyMonitor

        monitor_service = services_container.get_monitor_service()
        monitor = monitor_service.run_if_absent(
            AsyncClusterTopologyMonitor, key,
            lambda: self._create_monitor(key))
        if monitor is None:
            return None
        # The monitor service has no liveness check of its own, and
        # compute_if_absent extends expiration on every hit, so a monitor
        # whose loop has closed would be handed out until the stuck-monitor
        # sweep removed it, while every blocking refresh waits for an update
        # that can never arrive.
        if not monitor.is_running():
            monitor_service.stop_and_remove_value(
                AsyncClusterTopologyMonitor, monitor)
            monitor = monitor_service.run_if_absent(
                AsyncClusterTopologyMonitor, key,
                lambda: self._create_monitor(key))
        return monitor

    def _create_monitor(self, key: TopologyKey) -> Any:
        from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
            AsyncClusterTopologyMonitor

        high_refresh_ms = WrapperProperties.CLUSTER_TOPOLOGY_HIGH_REFRESH_RATE_MS.get_int(
            self._props)
        # The background loop must never query the shared application
        # connection: driver connections cannot service concurrent queries, so
        # a background refresh there corrupts the application's in-flight
        # query. Without a dedicated monitoring connection the getter yields
        # None and the loop skips, rather than falling back to the shared
        # connection. Foreground force_refresh still refreshes on demand
        # through the connection passed to it.
        if self._monitor_connection_factory is None:
            logger.warning(
                "AsyncAuroraHostListProvider.NoMonitoringConnectionFactory",
                self._cluster_id)
        monitor = AsyncClusterTopologyMonitor(
            topology_utils=self._topology_utils,
            cluster_id=self._cluster_id,
            key=key,
            connection_getter=(
                (lambda: self._last_conn)
                if self._monitor_connection_factory is not None
                else (lambda: None)),
            refresh_interval_sec=self._refresh_ns / 1_000_000_000,
            high_refresh_rate_sec=(
                (high_refresh_ms / 1000.0) if high_refresh_ms else 1.0),
            probe_host=self._probe_host,
            connection_factory=self._monitor_connection_factory,
        )
        monitor.start()

        # A shutdown hook as well as the monitor-service entry:
        # release_resources() clears the cache, dropping the last reference
        # before the teardown stop() scheduled could run, and
        # release_resources_async only awaits registered hooks.
        async def _close_and_unregister() -> None:
            await monitor.aclose()

        # aclose calls on_closed itself, so this has to be callable without
        # awaiting: a coroutine function here is created and dropped, and the
        # hook stays registered for the life of the process.
        def _unregister() -> None:
            unregister_shutdown_hook(_close_and_unregister)

        monitor.on_closed = _unregister
        register_shutdown_hook(_close_and_unregister)
        return monitor

    async def _fetch_and_cache(self, connection: Any) -> Topology:
        """Direct topology query plus a store update, used as a fallback."""
        topology = await self._topology_utils.query_for_topology(connection)
        self._store_topology(topology)
        return topology or (self._get_stored_topology() or ())

    async def stop(self) -> None:
        """Stop the cluster's topology monitor.

        Publishes MonitorStopEvent as sync ``stop_monitor`` does, so the
        monitor service owns the disposal. The monitor is shared, so this
        stops it for every provider on the cluster and loop.
        """
        from aws_advanced_python_wrapper.aio.cluster_topology_monitor import \
            AsyncClusterTopologyMonitor
        key = self._storage_key()
        if key is None:
            return
        services_container.get_event_publisher().publish(
            MonitorStopEvent(monitor_type=AsyncClusterTopologyMonitor, key=key))


class AsyncMultiAzHostListProvider(AsyncAuroraHostListProvider):
    """Async MultiAz Cluster topology provider.

    Ports sync :class:`MultiAzTopologyUtils`
    (``host_list_provider.py:630-702``). Runs a two-step query:
    ``writer_host_query`` to identify the writer's ID, then
    ``topology_query`` for the full host list. Row shape:
    ``(id, host, port)``; role assigned by ``id == writer_id``.

    Instantiate with explicit SQL strings. Full auto-detection from
    :class:`DatabaseDialect` introspection is a future enhancement (sync
    reads them from ``MultiAzClusterMysqlDialect`` /
    ``MultiAzClusterPgDialect`` -- those dialects don't yet flow through
    the async PluginService dialect resolution chain).

    Two-step behavior matches sync:
    1. Execute ``writer_host_query``. A non-empty row => its first column
       is the writer's ID.
    2. Empty row (MySQL "we're the writer" case) => fall back to
       ``host_id_query`` for our own ID.
    3. Execute ``topology_query`` and parse ``(id, host, port)`` rows;
       role is ``WRITER`` iff ``id == writer_id``.

    Optional ``instance_template_host`` substitutes ``?`` in the template
    with the instance-ID extracted from the row's host via
    :meth:`RdsUtils.get_instance_id`, matching sync
    ``MultiAzTopologyUtils._create_multi_az_host``.
    """

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            writer_host_query: str,
            topology_query: str,
            host_id_query: str,
            cluster_id: Optional[str] = None,
            default_port: int = 5432,
            instance_template_host: Optional[str] = None,
            monitor_connection_factory: Optional[
                Callable[[], Awaitable[Any]]] = None,
            writer_host_column_index: int = 0,
            provider_service: Optional[AsyncHostListProviderService] = None,
    ) -> None:
        super().__init__(
            props=props,
            driver_dialect=driver_dialect,
            topology_query=topology_query,
            cluster_id=cluster_id,
            default_port=default_port,
            monitor_connection_factory=monitor_connection_factory,
            topology_utils=AsyncMultiAzTopologyUtils(
                props=props,
                topology_query=topology_query,
                default_port=default_port,
                writer_host_query=writer_host_query,
                host_id_query=host_id_query,
                writer_host_column_index=writer_host_column_index,
                instance_template_host=instance_template_host),
            provider_service=provider_service,
        )


class AsyncGlobalAuroraHostListProvider(AsyncAuroraHostListProvider):
    """Async Aurora Global Database topology provider.

    An Aurora global database spans regions, so each host is built from the
    template configured for its own region rather than this connection's DNS
    suffix. Templates come from the ``instance_templates_by_region`` argument
    or the ``global_cluster_instance_host_patterns`` property.

    A partial port: sync's GlobalAuroraTopologyMonitor thread machinery is not
    mirrored, so topology is fetched through the normal refresh path.
    """

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            topology_query: str,
            *,
            instance_templates_by_region: Optional[Dict[str, str]] = None,
            cluster_id: Optional[str] = None,
            default_port: int = 5432,
            monitor_connection_factory: Optional[
                Callable[[], Awaitable[Any]]] = None,
            provider_service: Optional[AsyncHostListProviderService] = None,
    ) -> None:
        super().__init__(
            props=props,
            driver_dialect=driver_dialect,
            topology_query=topology_query,
            cluster_id=cluster_id,
            default_port=default_port,
            monitor_connection_factory=monitor_connection_factory,
            topology_utils=AsyncGlobalAuroraTopologyUtils(
                props=props,
                topology_query=topology_query,
                default_port=default_port,
                instance_templates_by_region=instance_templates_by_region),
            provider_service=provider_service,
        )


def build_monitor_connection_factory(
        provider_service: AsyncHostListProviderService,
        props: Properties) -> Callable[[], Awaitable[Any]]:
    """Build the factory the topology monitor uses for its own connection.
    """
    from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                              PropertiesUtils)

    async def _connect() -> Any:
        target_func = provider_service.get_target_driver_func()
        if target_func is None:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "HostListProvider.MonitorConnectionNoTargetDriverFunc",
                    type(provider_service).__name__))
        host_info = (provider_service.initial_connection_host_info
                     or provider_service.current_host_info)
        if host_info is None:
            raise AwsWrapperError(
                Messages.get_formatted(
                    "HostListProvider.MonitorConnectionNoSeedHost",
                    type(provider_service).__name__))
        monitoring_props = PropertiesUtils.create_topology_monitoring_properties(
            Properties(dict(props)))
        return await provider_service.driver_dialect.connect(
            host_info, monitoring_props, target_func)

    return _connect


def create_static_provider(
        provider_service: AsyncHostListProviderService,
        props: Properties) -> AsyncHostListProvider:
    """Supplier body for dialects with no topology: a single seed host."""
    return AsyncStaticHostListProvider(props)


def create_aurora_provider(
        provider_service: AsyncHostListProviderService,
        props: Properties,
        *,
        topology_query: str,
        default_port: int) -> AsyncHostListProvider:
    """Supplier body for the Aurora topology dialects."""
    return AsyncAuroraHostListProvider(
        props,
        provider_service.driver_dialect,
        topology_query=topology_query,
        default_port=default_port,
        monitor_connection_factory=build_monitor_connection_factory(
            provider_service, props),
        provider_service=provider_service)


def create_global_aurora_provider(
        provider_service: AsyncHostListProviderService,
        props: Properties,
        *,
        topology_query: str,
        default_port: int) -> AsyncHostListProvider:
    """Supplier body for the Aurora Global Database dialects."""
    return AsyncGlobalAuroraHostListProvider(
        props,
        provider_service.driver_dialect,
        topology_query=topology_query,
        default_port=default_port,
        monitor_connection_factory=build_monitor_connection_factory(
            provider_service, props),
        provider_service=provider_service)


def create_multi_az_provider(
        provider_service: AsyncHostListProviderService,
        props: Properties,
        *,
        writer_host_query: str,
        topology_query: str,
        host_id_query: str,
        default_port: int,
        writer_host_column_index: int = 0) -> AsyncHostListProvider:
    """Supplier body for the RDS Multi-AZ DB cluster dialects."""
    return AsyncMultiAzHostListProvider(
        props,
        provider_service.driver_dialect,
        writer_host_query=writer_host_query,
        topology_query=topology_query,
        host_id_query=host_id_query,
        default_port=default_port,
        writer_host_column_index=writer_host_column_index,
        monitor_connection_factory=build_monitor_connection_factory(
            provider_service, props),
        provider_service=provider_service)
