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

"""``AsyncPluginService`` -- async counterpart of :class:`PluginService`.

SP-1 ships the shell: the Protocol that plugins depend on + a minimal
``AsyncPluginServiceImpl`` good enough to wire toy plugins in unit tests.
Rich behavior (dialect detection, host list management, session-state
service, status storage) lands in later SPs that need it.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import (TYPE_CHECKING, Any, Callable, ClassVar, Dict, FrozenSet,
                    List, Optional, Protocol, Set, Tuple)

from aws_advanced_python_wrapper.aio.connection_provider import \
    AsyncConnectionProviderManager
from aws_advanced_python_wrapper.aio.plugin import AsyncCanReleaseResources
from aws_advanced_python_wrapper.aio.session_state import (
    AsyncSessionStateService, AsyncSessionStateServiceImpl)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.exception_handling import ExceptionManager
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.notifications import (
    ConnectionEvent, HostEvent, OldConnectionSuggestedAction)
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.storage.cache_map import CacheMap

logger = Logger(__name__)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.host_list_provider import \
        AsyncHostListProvider
    from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
    from aws_advanced_python_wrapper.aio.plugin_manager import \
        AsyncPluginManager
    from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
        AllowedAndBlockedHosts
    from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
        TelemetryFactory


class AsyncPluginService(Protocol):
    """State + driver coordination for the async plugin pipeline.

    Pure getters stay sync. Anything that may talk to the database is async
    (e.g., ``is_in_transaction`` probes the connection; ``set_availability``
    is cache-only so stays sync).
    """

    @property
    def current_connection(self) -> Optional[Any]:
        """The currently-bound async driver connection, if any."""
        ...

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        ...

    @property
    def props(self) -> Properties:
        ...

    @property
    def driver_dialect(self) -> AsyncDriverDialect:
        ...

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        """The resolved :class:`DatabaseDialect`, or ``None`` before connect."""
        ...

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        ...

    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def is_read_only_connection_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        ...

    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        ...

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        ...

    def get_target_driver_func(self) -> Optional[Callable]:
        """The target driver's async connect callable, wired at connect time."""
        ...

    @property
    def plugin_manager(self) -> Optional[AsyncPluginManager]:
        ...

    @plugin_manager.setter
    def plugin_manager(self, value: AsyncPluginManager) -> None:
        ...

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        ...

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        ...

    @property
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        """Last-refreshed topology. Empty tuple before first refresh."""
        ...

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        """Topology filtered through ``allowed_and_blocked_hosts``.

        Mirrors sync ``PluginService.hosts`` (plugin_service.py:362-378):
        ``all_hosts`` is the raw topology; ``hosts`` is the allowed/blocked-
        filtered view plugins should use when picking connection targets.
        """
        ...

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        """First host the user connected to. ``None`` before connect."""
        ...

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: Optional[HostInfo]) -> None:
        ...

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        ...

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        ...

    async def force_monitoring_refresh_host_list(
            self,
            should_verify_writer: bool,
            timeout_sec: float) -> bool:
        """Blocking, monitor-driven topology refresh (sync parity with
        ``PluginService.force_monitoring_refresh_host_list``): the topology
        monitor discovers hosts on ITS OWN connections (panic fan-out when
        ``should_verify_writer``), never through the caller's -- possibly
        dead -- connection. Returns True when a fresh topology was adopted
        within ``timeout_sec``."""
        ...

    async def release_resources(self) -> None:
        """Best-effort shutdown. Idempotent. Does not raise."""
        ...

    def get_telemetry_factory(self) -> TelemetryFactory:
        """Return a TelemetryFactory for emitting counters/gauges/contexts.

        Defaults to NilTelemetryFactory (no-op) when no backend is
        wired. Plugins should call ``create_counter`` / ``create_gauge``
        eagerly in __init__ and then ``.inc()`` / ``.set()`` at runtime.
        """
        ...

    async def get_host_role(
            self,
            connection: Optional[Any] = None,
            timeout_sec: Optional[float] = None) -> HostRole:
        ...

    async def fill_aliases(
            self,
            connection: Optional[Any] = None,
            host_info: Optional[HostInfo] = None) -> None:
        """Populate ``host_info``'s aliases from the connection.

        Adds the host:port alias plus any aliases reported by the database's
        ``host_alias_query`` and by :meth:`identify_connection`. No-op when the
        host already has aliases. Mirrors sync ``PluginServiceImpl.fill_aliases``.
        """
        ...

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        """Open a connection to ``host_info`` through the plugin pipeline.

        Lets plugins (StaleDns, SimpleRWS, etc.) open fresh connections
        through the full plugin chain so auth plugins (IAM, Secrets,
        Federated, Okta) re-apply on the new connection. Mirrors sync
        plugin_service.py:605-614.

        ``plugin_to_skip`` is excluded from the pipeline for this call
        (prevents recursion when the caller is itself a plugin driving
        the new connection).
        """
        ...

    async def force_connect(
            self,
            host_info: HostInfo,
            props: Properties,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        """Like :meth:`connect` but BYPASSES any custom connection provider
        (e.g. the pooled provider), using the default driver provider, while
        still running the plugin pipeline. Mirrors sync
        ``plugin_service.force_connect``."""
        ...

    def set_status(
            self,
            clazz: type,
            key: str,
            status: Any) -> None:
        """Publish ``status`` under (clazz, key). Plugins use this for
        shared state within the connection's lifetime (e.g. BlueGreen
        status, custom endpoint member cache)."""
        ...

    def get_status(
            self,
            clazz: type,
            key: str) -> Optional[Any]:
        """Retrieve a previously set status, or None. Verifies the stored
        value is an instance of ``clazz`` (sync plugin_service.py:798-812
        raises ValueError on mismatch; async returns None instead for
        best-effort semantics)."""
        ...

    def remove_status(self, clazz: type, key: str) -> None:
        """Drop a (clazz, key) entry. No-op if absent."""
        ...

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        ...

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        ...

    def filter_hosts(self, hosts: List[HostInfo]) -> List[HostInfo]:
        """Restrict a host list to those permitted by allowed_and_blocked_hosts
        (custom endpoint membership). No-op when no permissions are set."""
        ...

    def get_connection_provider_manager(self) -> AsyncConnectionProviderManager:
        """Return the connection-provider manager for this service.

        Plugins use this to consult the provider registry (e.g., RWS
        checks whether a connection was produced by a pool provider).
        Mirrors sync plugin_service.py:301 + :719.
        """
        ...

    @property
    def session_state_service(self) -> AsyncSessionStateService:
        """Return the session-state service for this connection.

        Failover and RWS plugins call ``begin()`` / ``apply_current_session_state``
        to preserve autocommit/readonly across connection swaps. Mirrors
        sync plugin_service.py:186 + :497.
        """
        ...

    @property
    def allowed_and_blocked_hosts(self) -> Optional[AllowedAndBlockedHosts]:
        """Current include/exclude filter on the topology.

        ``None`` means unrestricted. Plugins set this to filter out
        hosts unavailable to the caller (custom endpoint plugin, blue/
        green plugin). Mirrors sync plugin_service.py:144.
        """
        ...

    @allowed_and_blocked_hosts.setter
    def allowed_and_blocked_hosts(
            self, value: Optional[AllowedAndBlockedHosts]) -> None:
        ...

    @property
    def is_in_transaction(self) -> bool:
        """Last-known transaction state of the current connection.

        Populated by :meth:`update_in_transaction`. Mirrors sync
        plugin_service.py:191.
        """
        ...

    async def update_in_transaction(
            self, is_in_transaction: Optional[bool] = None) -> None:
        """Refresh ``is_in_transaction`` from the driver if not supplied.

        Called by plugins after executing a SQL statement that may have
        opened or closed a transaction (BEGIN/COMMIT/ROLLBACK). Mirrors
        sync plugin_service.py:217.
        """
        ...

    def update_driver_dialect(self, connection_provider: Any) -> None:
        """Let the driver dialect manager re-pick the dialect now that
        ``connection_provider`` is known. No-op on async today (no
        pool-aware dialect manager yet) but present for sync parity.
        """
        ...

    async def update_database_dialect(self, connection: Any) -> None:
        """Upgrade the database dialect from the live connection (sync parity
        with ``PluginServiceImpl.update_dialect``, invoked from the terminal
        plugin's connect so outer plugins' post-connect logic sees the
        corrected dialect)."""
        ...

    async def identify_connection(
            self, connection: Optional[Any] = None) -> Optional[HostInfo]:
        """Return the HostInfo that ``connection`` is bound to, or None.

        Used by failover + RWS to correlate a fresh connection back to a
        known topology entry. Mirrors sync plugin_service.py:619.
        """
        ...

    @property
    def network_bound_methods(self) -> Set[str]:
        ...

    def is_network_bound_method(self, method_name: str) -> bool:
        ...

    async def set_current_connection(
            self,
            connection: Any,
            host_info: HostInfo) -> None:
        """Rebind the service to a new connection.

        Async because callers may need to transfer session state between the
        outgoing and incoming connections via the driver dialect.
        """
        ...

    def set_connection_wrapper(self, wrapper: Any) -> None:
        """Register the owning AsyncAwsWrapperConnection so connection
        switches can rebind its cached target connection."""
        ...


class AsyncPluginServiceImpl(AsyncPluginService):
    """Minimal concrete ``AsyncPluginService`` for SP-1.

    Holds the connection, host info, driver dialect, and properties. Does NOT
    yet manage:
      * host list refresh (SP-3)
      * dialect auto-upgrade (later SP)
      * session state service (later SP)
      * telemetry context (later SP)

    Those land as dedicated sub-projects. The shell is enough for toy plugins
    and for SP-2's ``AsyncAwsWrapperConnection`` to drive initial connect.
    """

    _host_availability_expiring_cache: ClassVar[CacheMap[str, HostAvailability]] = CacheMap()
    _HOST_AVAILABILITY_EXPIRATION_NANO: ClassVar[int] = 5 * 60 * 1_000_000_000  # 5 min

    def __init__(
            self,
            props: Properties,
            driver_dialect: AsyncDriverDialect,
            host_info: Optional[HostInfo] = None) -> None:
        self._props: Properties = props
        self._driver_dialect: AsyncDriverDialect = driver_dialect
        self._database_dialect: Optional[DatabaseDialect] = None
        # True once update_database_dialect has made a definitive decision
        # (sync parity: DatabaseDialectManager can_update going False).
        self._database_dialect_settled: bool = False
        self._exception_manager: ExceptionManager = ExceptionManager()
        self._plugin_manager: Optional[AsyncPluginManager] = None
        self._host_list_provider: Optional[AsyncHostListProvider] = None
        self._all_hosts: Tuple[HostInfo, ...] = ()
        self._initial_connection_host_info: Optional[HostInfo] = None
        self._current_host_info: Optional[HostInfo] = host_info
        self._current_connection: Optional[Any] = None
        # Back-reference to the owning AsyncAwsWrapperConnection so connection
        # switches (failover / RWS) can rebind its cached ``_target_conn``.
        self._connection_wrapper: Optional[Any] = None
        self._telemetry_factory: Optional[TelemetryFactory] = None
        self._target_driver_func: Optional[Callable] = None
        self._status_store: Dict[Tuple[type, str], Any] = {}
        self._connection_provider_manager: AsyncConnectionProviderManager = (
            AsyncConnectionProviderManager()
        )
        self._session_state_service: AsyncSessionStateService = (
            AsyncSessionStateServiceImpl(self, props)
        )
        self._allowed_and_blocked_hosts: Optional[AllowedAndBlockedHosts] = None
        self._is_in_transaction: bool = False

    @property
    def current_connection(self) -> Optional[Any]:
        return self._current_connection

    @property
    def current_host_info(self) -> Optional[HostInfo]:
        """The host the current connection is bound to.

        Port of sync ``PluginServiceImpl.current_host_info`` (:446-478): once
        set it is returned directly; otherwise it falls back to the initial
        connection host, then to the topology's writer (validated against the
        allowed-hosts filter), then to the first allowed host.

        Deviation from sync: where sync raises ``HostListEmpty`` /
        ``CouldNotDetermineCurrentHost`` when no host can be resolved, async
        returns ``None`` instead. The async failover / read-write-splitting
        plugins (outside this change's scope) read ``current_host_info`` and
        treat ``None`` as "no current host yet"; raising there would break their
        pre-topology paths. The ``CurrentHostNotAllowed`` guard (a real
        misconfiguration, not a not-yet-known state) IS still raised.
        """
        if self._current_host_info is not None:
            return self._current_host_info

        self._current_host_info = self._initial_connection_host_info
        if self._current_host_info is not None:
            return self._current_host_info

        all_hosts = self._all_hosts
        if not all_hosts:
            # Sync raises HostListEmpty; async tolerates the not-yet-known state.
            return None

        writer = next((h for h in all_hosts if h.role == HostRole.WRITER), None)
        allowed_hosts = self.filter_hosts(list(all_hosts))
        if writer is not None:
            target = writer.get_host_and_port()
            if not any(h.get_host_and_port() == target for h in allowed_hosts):
                allowed_str = ", ".join(h.get_host_and_port() for h in allowed_hosts)
                raise AwsWrapperError(
                    Messages.get_formatted(
                        "PluginServiceImpl.CurrentHostNotAllowed", target, allowed_str))
            self._current_host_info = writer
        elif allowed_hosts:
            self._current_host_info = allowed_hosts[0]

        # Sync raises CouldNotDetermineCurrentHost when still unresolved; async
        # returns None (same not-yet-known tolerance as the empty-list case).
        return self._current_host_info

    @property
    def props(self) -> Properties:
        return self._props

    @property
    def driver_dialect(self) -> AsyncDriverDialect:
        return self._driver_dialect

    @property
    def database_dialect(self) -> Optional[DatabaseDialect]:
        return self._database_dialect

    @database_dialect.setter
    def database_dialect(self, value: Optional[DatabaseDialect]) -> None:
        self._database_dialect = value

    def is_network_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_network_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def is_login_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_login_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def is_read_only_connection_exception(
            self,
            error: Optional[Exception] = None,
            sql_state: Optional[str] = None) -> bool:
        return self._exception_manager.is_read_only_connection_exception(
            self._database_dialect, error=error, sql_state=sql_state)

    def set_availability(
            self,
            host_aliases: FrozenSet[str],
            availability: HostAvailability) -> None:
        for alias in host_aliases:
            AsyncPluginServiceImpl._host_availability_expiring_cache.put(
                alias,
                availability,
                AsyncPluginServiceImpl._HOST_AVAILABILITY_EXPIRATION_NANO,
            )

    def get_availability(self, host_url: str) -> Optional[HostAvailability]:
        return AsyncPluginServiceImpl._host_availability_expiring_cache.get(host_url)

    @property
    def plugin_manager(self) -> Optional[AsyncPluginManager]:
        return self._plugin_manager

    @plugin_manager.setter
    def plugin_manager(self, value: AsyncPluginManager) -> None:
        self._plugin_manager = value

    def accepts_strategy(self, role: HostRole, strategy: str) -> bool:
        if self._plugin_manager is None:
            return False
        return self._plugin_manager.accepts_strategy(role, strategy)

    def filter_hosts(self, hosts: List[HostInfo]) -> List[HostInfo]:
        """Restrict a host list to those permitted by allowed_and_blocked_hosts.

        No-op when no permissions are set (the common case), so non-custom-
        endpoint connections are unaffected. The custom endpoint monitor sets
        allowed_and_blocked_hosts so failover / RWS / connect only use the
        endpoint's member instances. Mirrors sync PluginService.hosts.
        """
        perms = self._allowed_and_blocked_hosts
        if perms is None:
            return hosts

        def _instance_id(h: HostInfo) -> Optional[str]:
            # allowed/blocked sets hold RDS DB instance identifiers (the custom
            # endpoint's StaticMembers). Topology HostInfo.host_id is the
            # instance id when populated, but some async topology paths leave it
            # None; fall back to the leftmost DNS label of the instance endpoint
            # (e.g. "my-instance" from "my-instance.<hash>.<region>.rds...").
            if h.host_id:
                return h.host_id
            if h.host:
                return h.host.split(".", 1)[0]
            return None

        result = hosts
        allowed = perms.allowed_host_ids
        blocked = perms.blocked_host_ids
        if allowed is not None:
            result = [h for h in result if _instance_id(h) in allowed]
        if blocked is not None:
            result = [h for h in result if _instance_id(h) not in blocked]
        return result

    def get_host_info_by_strategy(
            self,
            role: HostRole,
            strategy: str,
            host_list: Optional[List[HostInfo]] = None) -> Optional[HostInfo]:
        if self._plugin_manager is None:
            return None
        candidates = list(host_list) if host_list is not None else list(self._all_hosts)
        candidates = self.filter_hosts(candidates)
        return self._plugin_manager.get_host_info_by_strategy(role, strategy, candidates)

    def get_connection_provider_manager(self) -> AsyncConnectionProviderManager:
        return self._connection_provider_manager

    @property
    def session_state_service(self) -> AsyncSessionStateService:
        return self._session_state_service

    def set_connection_wrapper(self, wrapper: Any) -> None:
        """Register the owning AsyncAwsWrapperConnection.

        Lets ``set_current_connection`` rebind the wrapper's cached
        ``_target_conn`` on plugin-driven connection switches so the wrapper
        follows failover / read-write-splitting swaps.
        """
        self._connection_wrapper = wrapper

    @property
    def allowed_and_blocked_hosts(self) -> Optional[AllowedAndBlockedHosts]:
        return self._allowed_and_blocked_hosts

    @allowed_and_blocked_hosts.setter
    def allowed_and_blocked_hosts(
            self, value: Optional[AllowedAndBlockedHosts]) -> None:
        self._allowed_and_blocked_hosts = value

    @property
    def is_in_transaction(self) -> bool:
        return self._is_in_transaction

    async def update_in_transaction(
            self, is_in_transaction: Optional[bool] = None) -> None:
        if is_in_transaction is not None:
            self._is_in_transaction = is_in_transaction
            return
        if self._current_connection is None:
            raise AwsWrapperError(
                Messages.get("PluginServiceImpl.UnableToUpdateTransactionStatus"))
        self._is_in_transaction = await self._driver_dialect.is_in_transaction(
            self._current_connection)

    def update_driver_dialect(self, connection_provider: Any) -> None:
        # No-op: async has no pool-aware DriverDialectManager. The sync
        # site swaps dialects when a pool provider is installed; async
        # drivers don't differentiate by provider today. Kept for sync
        # parity so callers can invoke unconditionally.
        return

    async def update_database_dialect(self, connection: Any) -> None:
        """Upgrade the database dialect from the live connection.

        Async parity for sync ``PluginServiceImpl.update_dialect``
        (plugin_service.py:538), invoked from the terminal plugin's connect
        (sync default_plugin.py:82) so every OUTER plugin's post-connect logic
        already sees the corrected dialect. The async layer previously ran
        this upgrade only AFTER the whole connect pipeline
        (``_upgrade_database_dialect_after_connect`` in aio/wrapper.py), which
        left plugin connect hooks working with the pattern-guessed dialect --
        on an Aurora MySQL *instance* endpoint that guess is
        ``RdsMysqlDialect`` (no topology query; ``@@read_only`` role probe
        that does not flip on Aurora failover), so hook-time topology/role
        operations failed silently (the root asymmetry behind the
        connection-tracker's pin and exception-classification defenses).

        Only MySQL needs a live probe: the PG pattern guess is already
        Aurora-correct for every endpoint form. Settles after a definitive
        probe (mirrors sync ``can_update`` going False); a transient probe
        failure leaves it unsettled so the next connect retries -- sync runs
        update_dialect on every connect for the same reason.
        """
        if self._database_dialect_settled:
            return
        driver_dialect = self._driver_dialect
        if getattr(driver_dialect, "_driver_name", None) != "aiomysql":
            self._database_dialect_settled = True
            return
        from aws_advanced_python_wrapper.aio.host_list_provider import \
            AsyncAuroraHostListProvider
        from aws_advanced_python_wrapper.database_dialect import (
            AuroraMysqlDialect, DatabaseDialectManager, DialectCode,
            MysqlDatabaseDialect)

        database_dialect = self._database_dialect
        # A base/RDS MySQL dialect needs a live probe; a cluster-endpoint
        # connect already resolved to AuroraMysqlDialect by pattern and skips
        # the probe (but still needs the provider wired below).
        if (not isinstance(database_dialect, AuroraMysqlDialect)
                and isinstance(database_dialect, MysqlDatabaseDialect)):
            try:
                async with connection.cursor() as cur:
                    await cur.execute("SHOW VARIABLES LIKE 'aurora_version'")
                    # fetchall (not fetchone) so the result set is fully
                    # drained -- a partially-read result leaves the shared
                    # aiomysql connection dirty and the app's next query reads
                    # the leftover row.
                    is_aurora = len(await cur.fetchall()) > 0
            except Exception:  # noqa: BLE001 - keep initial dialect; retry next connect
                return
            if is_aurora:
                aurora = DatabaseDialectManager._known_dialects_by_code.get(
                    DialectCode.AURORA_MYSQL)
                if aurora is not None:
                    database_dialect = aurora
                    self._database_dialect = aurora

        if (isinstance(database_dialect, AuroraMysqlDialect)
                and isinstance(self._host_list_provider,
                               AsyncAuroraHostListProvider)):
            self._host_list_provider.reconfigure_topology_query(
                database_dialect.topology_query, default_port=3306)
        self._database_dialect_settled = True

    async def identify_connection(
            self, connection: Optional[Any] = None) -> Optional[HostInfo]:
        conn = connection if connection is not None else self._current_connection
        if conn is None:
            raise AwsWrapperError(
                Messages.get("PluginServiceImpl.ErrorIdentifyConnection"))
        if self._host_list_provider is None:
            return None
        if self._database_dialect is None:
            return None

        from aws_advanced_python_wrapper.aio.dialect_utils import \
            AsyncDialectUtils
        instance_id = await AsyncDialectUtils.get_instance_id(
            conn,
            self._driver_dialect,
            self._database_dialect.host_id_query,
        )
        if instance_id is None:
            return None

        topology = await self._host_list_provider.refresh(conn)
        found = next(
            (h for h in topology if h.host_id == instance_id), None)
        if found is None:
            topology = await self._host_list_provider.force_refresh(conn)
            found = next(
                (h for h in topology if h.host_id == instance_id), None)
        return found

    @property
    def host_list_provider(self) -> Optional[AsyncHostListProvider]:
        return self._host_list_provider

    @host_list_provider.setter
    def host_list_provider(self, value: AsyncHostListProvider) -> None:
        self._host_list_provider = value

    @property
    def all_hosts(self) -> Tuple[HostInfo, ...]:
        return self._all_hosts

    @property
    def hosts(self) -> Tuple[HostInfo, ...]:
        """Allowed/blocked-filtered view of the topology.

        Sync parity: ``PluginServiceImpl.hosts`` (plugin_service.py:362-378)
        filters ``all_hosts`` through the custom-endpoint/blue-green host
        permissions; ``all_hosts`` stays the raw view. The filtering itself is
        delegated to :meth:`filter_hosts` so both surfaces stay consistent.
        """
        return tuple(self.filter_hosts(list(self._all_hosts)))

    @property
    def initial_connection_host_info(self) -> Optional[HostInfo]:
        return self._initial_connection_host_info

    @initial_connection_host_info.setter
    def initial_connection_host_info(self, value: Optional[HostInfo]) -> None:
        self._initial_connection_host_info = value

    async def refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        if self._host_list_provider is None:
            raise AwsWrapperError(
                Messages.get("AsyncPluginService.HostListProviderNotSet"))
        conn = connection if connection is not None else self._current_connection
        updated = tuple(await self._host_list_provider.refresh(conn))
        # Never adopt an empty host list: a topology query through a
        # just-failed-over (dead) connection returns nothing, and clobbering
        # the cache would strand failover with no surviving hosts to try.
        # Mirrors sync RdsHostListProvider._get_topology ("use live only if
        # len > 0"); the last good topology stays available for failover.
        if updated and updated != self._all_hosts:
            self._update_host_availability(updated)
            self._update_hosts(updated)

    async def force_refresh_host_list(
            self,
            connection: Optional[Any] = None) -> None:
        if self._host_list_provider is None:
            raise AwsWrapperError(
                Messages.get("AsyncPluginService.HostListProviderNotSet"))
        conn = connection if connection is not None else self._current_connection
        updated = tuple(await self._host_list_provider.force_refresh(conn))
        # Same non-empty guard as refresh_host_list above.
        if updated and updated != self._all_hosts:
            self._update_host_availability(updated)
            self._update_hosts(updated)

    async def force_monitoring_refresh_host_list(
            self,
            should_verify_writer: bool,
            timeout_sec: float) -> bool:
        if self._host_list_provider is None:
            raise AwsWrapperError(
                Messages.get("AsyncPluginService.HostListProviderNotSet"))
        fmr = getattr(self._host_list_provider, "force_monitoring_refresh", None)
        if fmr is None or not inspect.iscoroutinefunction(fmr):
            # Provider without monitor support (static dialects / test
            # doubles): fall back to a plain forced refresh and report
            # whether we hold any topology.
            await self.force_refresh_host_list()
            return bool(self._all_hosts)
        updated = tuple(await fmr(should_verify_writer, timeout_sec))
        if updated and updated != self._all_hosts:
            self._update_host_availability(updated)
            self._update_hosts(updated)
        # Sync parity: report whether the monitor confirmed a topology within
        # the deadline (failover treats False as failure).
        return bool(updated)

    def _update_host_availability(self, hosts: Tuple[HostInfo, ...]) -> None:
        # Re-hydrate cached availability onto freshly-built HostInfo objects
        # (topology queries return AVAILABLE-by-default hosts). Mirrors sync
        # PluginServiceImpl._update_host_availability.
        for host in hosts:
            availability = AsyncPluginServiceImpl._host_availability_expiring_cache.get(host.url)
            if availability:
                host.set_availability(availability)

    def _update_hosts(self, new_hosts: Tuple[HostInfo, ...]) -> None:
        # Diff old vs new topology into per-host HostEvent sets and notify
        # plugins through the manager. Mirrors sync
        # PluginServiceImpl._update_hosts.
        old_hosts_dict = {x.url: x for x in self._all_hosts}
        new_hosts_dict = {x.url: x for x in new_hosts}

        changes: Dict[str, Set[HostEvent]] = {}

        for host in self._all_hosts:
            corresponding_new_host = new_hosts_dict.get(host.url)
            if corresponding_new_host is None:
                changes[host.url] = {HostEvent.HOST_DELETED}
            else:
                host_changes = self._compare(host, corresponding_new_host)
                if len(host_changes) > 0:
                    changes[host.url] = host_changes

        for key in new_hosts_dict:
            if key not in old_hosts_dict:
                changes[key] = {HostEvent.HOST_ADDED}

        if len(changes) > 0:
            self._all_hosts = tuple(new_hosts)
            if self._plugin_manager is not None:
                self._plugin_manager.notify_host_list_changed(changes)

    @staticmethod
    def _compare(host_a: HostInfo, host_b: HostInfo) -> Set[HostEvent]:
        # Mirrors sync PluginServiceImpl._compare.
        changes: Set[HostEvent] = set()
        if host_a.host != host_b.host or host_a.port != host_b.port:
            changes.add(HostEvent.URL_CHANGED)

        if host_a.role != host_b.role:
            if host_b.role == HostRole.WRITER:
                changes.add(HostEvent.CONVERTED_TO_WRITER)
            elif host_b.role == HostRole.READER:
                changes.add(HostEvent.CONVERTED_TO_READER)

        if host_a.get_availability() != host_b.get_availability():
            if host_b.get_availability() == HostAvailability.AVAILABLE:
                changes.add(HostEvent.WENT_UP)
            elif host_b.get_availability() == HostAvailability.UNAVAILABLE:
                changes.add(HostEvent.WENT_DOWN)

        if len(changes) > 0:
            changes.add(HostEvent.HOST_CHANGED)

        return changes

    async def release_resources(self) -> None:
        """Best-effort shutdown. Idempotent. Does not raise.

        CLOSES the current connection (sync parity: plugin_service.py:783-789
        calls ``current_connection.close()``) and, if the host list provider
        exposes ``release_resources``, awaits that too. Exceptions are
        swallowed so one bad cleanup step doesn't block others. Plugins can
        register their own async shutdown via
        :func:`aws_advanced_python_wrapper.aio.cleanup.register_shutdown_hook`.

        Deliberately NOT ``driver_dialect.abort_connection``: abort kills the
        raw SOCKET. With a pooled provider the current connection is a pool
        proxy whose underlying connection was just returned ALIVE to the pool
        by the wrapper's close pipeline -- aborting here left a corpse in the
        pool that the next acquirer received ('the connection is closed' /
        'SSL error: unexpected eof'; deterministic
        test_pooled_connection__different_users failure, F6). ``close()`` is
        the safe equivalent everywhere: idempotent no-op on an
        already-closed raw connection, idempotent pool-release on a proxy.
        """
        conn = self._current_connection
        if conn is not None:
            try:
                already_closed = bool(await self._driver_dialect.is_closed(conn))
            except Exception:  # noqa: BLE001 - unknown state: close anyway
                already_closed = False
            if not already_closed:
                try:
                    result = conn.close()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:  # noqa: BLE001 - intentional best-effort teardown
                    pass

        hlp = self._host_list_provider
        if isinstance(hlp, AsyncCanReleaseResources):
            try:
                await hlp.release_resources()
            except Exception:  # noqa: BLE001
                pass

    def get_telemetry_factory(self) -> TelemetryFactory:
        if self._telemetry_factory is None:
            from aws_advanced_python_wrapper.utils.telemetry.null_telemetry import \
                NullTelemetryFactory
            self._telemetry_factory = NullTelemetryFactory()
        return self._telemetry_factory

    def set_telemetry_factory(self, factory: TelemetryFactory) -> None:
        """Wire an external telemetry factory (e.g. from AsyncAwsWrapperConnection.connect)."""
        self._telemetry_factory = factory

    async def get_host_role(
            self,
            connection: Optional[Any] = None,
            timeout_sec: Optional[float] = None) -> HostRole:
        """Probe ``connection`` (or current_connection) to learn its role.

        Uses the resolved DatabaseDialect's ``is_reader_query`` via
        AsyncDialectUtils. Raises AwsWrapperError if no dialect is set,
        if the connection is None, or if the probe fails.

        When ``timeout_sec`` is not supplied, defaults to
        ``AUXILIARY_QUERY_TIMEOUT_SEC`` (sync parity) rather than a hardcoded 5s.
        """
        conn = connection if connection is not None else self._current_connection
        if conn is None:
            raise AwsWrapperError(
                Messages.get("PluginServiceImpl.GetHostRoleConnectionNone"))
        if self._database_dialect is None:
            raise AwsWrapperError(
                "AsyncPluginService.get_host_role requires a database_dialect; "
                "it is populated by AsyncAwsWrapperConnection.connect.")
        if timeout_sec is None:
            timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_float(
                self._props)
        from aws_advanced_python_wrapper.aio.dialect_utils import \
            AsyncDialectUtils
        return await AsyncDialectUtils.get_host_role(
            conn,
            self._driver_dialect,
            self._database_dialect.is_reader_query,
            timeout_sec=timeout_sec,
        )

    async def fill_aliases(
            self,
            connection: Optional[Any] = None,
            host_info: Optional[HostInfo] = None) -> None:
        """Populate ``host_info``'s aliases -- port of sync
        ``PluginServiceImpl.fill_aliases`` (plugin_service.py:671-706).

        Adds the ``host:port`` alias, then any aliases the database reports via
        ``host_alias_query``, then the aliases of the host that
        :meth:`identify_connection` resolves the connection to. No-op when the
        host already carries aliases (idempotent).
        """
        connection = self._current_connection if connection is None else connection
        host_info = self.current_host_info if host_info is None else host_info
        if connection is None or host_info is None:
            return

        if len(host_info.aliases) > 0:
            logger.debug("PluginServiceImpl.NonEmptyAliases", host_info.aliases)
            return

        host_info.add_alias(host_info.as_alias())

        aux_timeout_sec = WrapperProperties.AUXILIARY_QUERY_TIMEOUT_SEC.get_float(
            self._props)
        try:
            await asyncio.wait_for(
                self._fill_aliases_from_query(connection, host_info),
                timeout=aux_timeout_sec)
        except asyncio.TimeoutError as e:
            from aws_advanced_python_wrapper.errors import QueryTimeoutError
            raise QueryTimeoutError(
                Messages.get("PluginServiceImpl.FillAliasesTimeout")) from e
        except Exception as e:  # noqa: BLE001 - alias query is best-effort
            logger.debug("PluginServiceImpl.FailedToRetrieveHostPort", e)

        host = await self.identify_connection(connection)
        if host:
            host_info.add_alias(*host.as_aliases())

    async def _fill_aliases_from_query(
            self, connection: Any, host_info: HostInfo) -> None:
        dialect = self._database_dialect
        if dialect is None:
            return
        from aws_advanced_python_wrapper.database_dialect import \
            UnknownDatabaseDialect
        if isinstance(dialect, UnknownDatabaseDialect):
            return
        alias_query = dialect.host_alias_query
        if not alias_query:
            return
        async with connection.cursor() as cur:
            await cur.execute(alias_query)
            for row in await cur.fetchall():
                if row:
                    host_info.add_alias(row[0])

    async def connect(
            self,
            host_info: HostInfo,
            props: Properties,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        if self._plugin_manager is None:
            raise AwsWrapperError(
                "AsyncPluginService.connect requires a plugin_manager; "
                "it is populated by AsyncAwsWrapperConnection.connect.")
        # The plugin manager needs a target driver func. We need to reach
        # it from the same place AsyncAwsWrapperConnection does -- the
        # driver dialect's connect signature differs per driver, so we
        # require the target_func to have been stashed somewhere accessible.
        # For Phase I: require the caller to have recorded a target_func
        # on the plugin service via set_target_driver_func.
        if self._target_driver_func is None:
            raise AwsWrapperError(
                "AsyncPluginService.connect requires target_driver_func "
                "to be set; AsyncAwsWrapperConnection.connect normally "
                "wires this.")
        return await self._plugin_manager.connect(
            target_driver_func=self._target_driver_func,
            driver_dialect=self._driver_dialect,
            host_info=host_info,
            props=props,
            is_initial_connection=False,
            plugin_to_skip=plugin_to_skip,
        )

    async def force_connect(
            self,
            host_info: HostInfo,
            props: Properties,
            plugin_to_skip: Optional[AsyncPlugin] = None) -> Any:
        """Open a connection that BYPASSES any custom connection provider
        (e.g. the pooled provider), using the default driver provider, while
        still running the plugin pipeline so auth plugins (IAM, Secrets,
        Federated, Okta) re-apply. Mirrors sync ``plugin_service.force_connect``.

        Failover uses this rather than :meth:`connect`: the failover reconnect
        must produce a fresh, non-pooled connection. Routing it through the
        pooled provider creates internal pools on the failover target -- which
        breaks the pooled-connection failover tests, where a connection
        established via a cluster URL must stay unpooled even after failover
        (test_pooled_connection__cluster_url_failover) and a pooled connection
        must be replaced by a non-pooled one after failover
        (test_pooled_connection__failover).
        """
        if self._plugin_manager is None:
            raise AwsWrapperError(
                "AsyncPluginService.force_connect requires a plugin_manager; "
                "it is populated by AsyncAwsWrapperConnection.connect.")
        if self._target_driver_func is None:
            raise AwsWrapperError(
                "AsyncPluginService.force_connect requires target_driver_func "
                "to be set; AsyncAwsWrapperConnection.connect normally wires this.")
        return await self._plugin_manager.force_connect(
            target_driver_func=self._target_driver_func,
            driver_dialect=self._driver_dialect,
            host_info=host_info,
            props=props,
            is_initial_connection=False,
            plugin_to_skip=plugin_to_skip,
        )

    def set_target_driver_func(self, func: Callable) -> None:
        """Wired by AsyncAwsWrapperConnection.connect at connect time."""
        self._target_driver_func = func

    def get_target_driver_func(self) -> Optional[Callable]:
        """The target driver's async connect callable (aiomysql.connect /
        psycopg.AsyncConnection.connect), wired at connect time. Plugins that
        open their own raw connections (e.g. RWS reader/writer switches) need
        this so they connect with the RIGHT driver instead of hardcoding one."""
        return self._target_driver_func

    def set_status(self, clazz, key, status):
        self._status_store[(clazz, key)] = status

    def get_status(self, clazz, key):
        value = self._status_store.get((clazz, key))
        if value is None:
            return None
        if not isinstance(value, clazz):
            return None
        return value

    def remove_status(self, clazz, key):
        self._status_store.pop((clazz, key), None)

    @property
    def network_bound_methods(self) -> Set[str]:
        return self._driver_dialect.network_bound_methods

    def is_network_bound_method(self, method_name: str) -> bool:
        from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
        nb = self.network_bound_methods
        return DbApiMethod.ALL.method_name in nb or method_name in nb

    async def set_current_connection(
            self,
            connection: Any,
            host_info: HostInfo) -> None:
        prev = self._current_connection

        if prev is None:
            # Initial connection: no old-connection lifecycle to manage.
            self._current_connection = connection
            self._current_host_info = host_info
            if self._connection_wrapper is not None:
                self._connection_wrapper._target_conn = connection
            self._session_state_service.reset()
            if self._plugin_manager is not None:
                self._plugin_manager.notify_connection_changed(
                    {ConnectionEvent.INITIAL_CONNECTION})
            return

        if connection is None or prev is connection:
            self._current_connection = connection
            self._current_host_info = host_info
            if self._connection_wrapper is not None:
                self._connection_wrapper._target_conn = connection
            return

        # Connection switch: mirror sync PluginServiceImpl.set_current_connection
        # (plugin_service.py:396-444) -- session-state bracket, tracked-state
        # apply, ROLLBACK_ON_SWITCH for an in-transaction old connection, and
        # old-connection disposal unless a plugin votes PRESERVE.
        was_in_transaction = self._is_in_transaction
        self._session_state_service.begin()
        try:
            await self._apply_switch_session_state(prev, connection, host_info)
            await self.update_in_transaction(False)

            if was_in_transaction and WrapperProperties.ROLLBACK_ON_SWITCH.get_bool(self._props):
                try:
                    rollback_result = prev.rollback()
                    if asyncio.iscoroutine(rollback_result):
                        await rollback_result
                except Exception:  # noqa: BLE001 - best-effort rollback, sync parity
                    pass

            suggestion = OldConnectionSuggestedAction.NO_OPINION
            if self._plugin_manager is not None:
                suggestion = self._plugin_manager.notify_connection_changed(
                    {ConnectionEvent.CONNECTION_OBJECT_CHANGED})

            try:
                old_closed = await self._driver_dialect.is_closed(prev)
            except Exception:  # noqa: BLE001 - treat unknown as open, sync closes best-effort
                old_closed = False

            if suggestion != OldConnectionSuggestedAction.PRESERVE and not old_closed:
                try:
                    await self._session_state_service.apply_pristine_session_state(prev)
                except Exception:  # noqa: BLE001 - sync parity: ignore
                    pass
                try:
                    close_result = prev.close()
                    if asyncio.iscoroutine(close_result):
                        await close_result
                except Exception:  # noqa: BLE001 - sync parity: ignore
                    pass
        finally:
            self._session_state_service.complete()

    async def _apply_switch_session_state(
            self,
            prev: Any,
            connection: Any,
            host_info: HostInfo) -> None:
        self._current_connection = connection
        self._current_host_info = host_info
        # Keep the owning wrapper's cached target connection in sync so its
        # cursor() / commit() / etc. follow the switch (see
        # AsyncAwsWrapperConnection.connect's set_connection_wrapper call).
        if self._connection_wrapper is not None:
            self._connection_wrapper._target_conn = connection
        if prev is not None and prev is not connection:
            # Apply tracked session state to the new connection, then a
            # best-effort dialect-level transfer as a supplement.
            try:
                await self._session_state_service.apply_current_session_state(
                    connection)
            except Exception:  # noqa: BLE001 - best-effort apply
                pass
            # Sync's set_current_connection (plugin_service.py:392) applies state
            # SOLELY via apply_current_session_state above and never calls the
            # driver dialect's transfer_session_state here -- the transfer is an
            # async-only supplement and MUST NOT be fatal. transfer_session_state
            # calls set_autocommit on the new connection, which psycopg rejects
            # while that connection is mid-transaction
            # (ProgrammingError "can't change 'autocommit' now: ... INTRANS") --
            # e.g. a reader RWS just switched to, on which a role-check query
            # already opened a txn, or a freshly-reconnected failover target.
            # apply_current_session_state already applied the tracked
            # autocommit/read_only (the only state the async transfer copies), so
            # swallow transfer failures rather than abort the switch
            # (test_failover_with_secrets_manager,
            # test_sqlalchemy_creator_read_write_splitting).
            try:
                await self._driver_dialect.transfer_session_state(
                    prev, connection)
            except Exception:  # noqa: BLE001 - best-effort transfer
                pass


__all__ = ["AsyncPluginService", "AsyncPluginServiceImpl"]
