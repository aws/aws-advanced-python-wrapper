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

"""Async custom endpoint monitor (Task 1-B).

Periodically resolves Aurora custom endpoints to their member instance
IDs via the AWS RDS DescribeDBClusterEndpoints API. The real
:class:`AsyncCustomEndpointPlugin` reads the cached member list to
filter the topology used by failover / RWS so the wrapper respects the
custom endpoint's instance membership.

3.0.0 shipped :class:`AsyncCustomEndpointPlugin` as a subscribe-to-nothing
stub. Task 1-B replaces the stub with a plugin that starts the monitor
on connect and stops it on :func:`release_resources_async`.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import (TYPE_CHECKING, Any, Awaitable, Callable, List, Optional,
                    Set, Tuple)

from aws_advanced_python_wrapper.aio.plugin import AsyncPlugin
from aws_advanced_python_wrapper.allowed_and_blocked_hosts import \
    AllowedAndBlockedHosts
# Reuse the sync data classes -- they're pure holders, no I/O.
from aws_advanced_python_wrapper.custom_endpoint_plugin import (
    CustomEndpointInfo, CustomEndpointRoleType)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.pep249_methods import DbApiMethod
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.region_utils import RegionUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.driver_dialect.base import \
        AsyncDriverDialect
    from aws_advanced_python_wrapper.aio.plugin_service import \
        AsyncPluginService
    from aws_advanced_python_wrapper.hostinfo import HostInfo
    from aws_advanced_python_wrapper.utils.properties import Properties


class AsyncCustomEndpointMonitor:
    """Periodically fetches the instance list for an Aurora custom endpoint.

    Polls boto3 ``rds.describe_db_cluster_endpoints`` from a **background
    daemon thread** (not an asyncio task) -- mirroring the sync
    ``CustomEndpointMonitor`` -- so a caller that blocks the event loop (e.g.
    a test's synchronous ``wait_until_endpoint_has_members``, or any
    sync-over-async bridge) cannot starve the refresh. An asyncio-task monitor
    silently stops updating ``allowed_and_blocked_hosts`` whenever the loop is
    blocked, which left the custom-endpoint "changes" tests switching against a
    stale member set. Caches the result as a tuple of instance IDs.
    """

    def __init__(
            self,
            custom_endpoint_identifier: str,
            region: Optional[str] = None,
            refresh_interval_sec: float = 30.0,
            cluster_identifier: Optional[str] = None,
            plugin_service: Optional[Any] = None) -> None:
        self._endpoint_id = custom_endpoint_identifier
        self._region = region
        # Plugin service whose allowed_and_blocked_hosts we update on each
        # refresh so failover / RWS / connect host-selection is restricted to
        # the custom endpoint's members (mirrors sync CustomEndpointMonitor).
        self._plugin_service = plugin_service
        # Retained for logging/compat only. The RDS describe call resolves the
        # endpoint by its own identifier + custom-type filter (see
        # _fetch_members_blocking); it does NOT use a DBClusterIdentifier,
        # because the wrapper's CLUSTER_ID is an internal alias, not the real
        # RDS cluster id.
        self._cluster_id = cluster_identifier
        # Lower-bound of 10 ms prevents a misconfigured 0 or negative
        # interval from burning CPU; the production default is 30 s.
        self._interval_sec = max(0.01, float(refresh_interval_sec))
        # Background polling thread + threading primitives (NOT asyncio) so the
        # monitor keeps refreshing even while the event loop is blocked.
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # Set after the first successful non-empty member refresh. Plugins
        # wait on this event via wait_for_info() so the initial query can
        # see a populated allowed-hosts filter.
        self._info_ready_event = threading.Event()
        self._member_instance_ids: Tuple[str, ...] = ()
        self._last_refresh_ns: int = 0

    @property
    def member_instance_ids(self) -> Tuple[str, ...]:
        """Most-recently-resolved member list. Empty tuple if not yet refreshed."""
        return self._member_instance_ids

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_running():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="AsyncCustomEndpointMonitor",
            daemon=True)
        self._thread.start()

    def _run(self) -> None:
        # Runs in a background daemon thread (see start()). Synchronous on
        # purpose: it must not depend on the event loop being free.
        while not self._stop_event.is_set():
            try:
                members = self._fetch_members_blocking(
                    self._endpoint_id, self._region)
                if members:
                    self._member_instance_ids = tuple(members)
                    self._last_refresh_ns = time.monotonic_ns()
                    # Enforce membership: restrict the host list used by
                    # failover / RWS / connect to the endpoint's members.
                    # StaticMembers map to allowed host ids (mirrors sync
                    # CustomEndpointMonitor:193-194). Setting an attribute from
                    # this thread is GIL-atomic; filter_hosts reads it on the
                    # event loop (eventual consistency is fine).
                    if self._plugin_service is not None:
                        self._plugin_service.allowed_and_blocked_hosts = \
                            AllowedAndBlockedHosts(set(members), None)
                    # Unblock any plugin.connect() awaiting wait_for_info().
                    self._info_ready_event.set()
            except Exception:  # noqa: BLE001
                # Transient AWS failures must not kill the monitor.
                pass
            # Until the first successful (non-empty) refresh, poll aggressively
            # (cap at 1s) so the initial wait_for_info() resolves promptly --
            # RDS can take a few seconds to surface a freshly-created/modified
            # endpoint's StaticMembers, and the steady-state interval (30s) is
            # far longer than the connect-time wait budget. After the first
            # refresh, use the configured interval. stop_event.wait makes the
            # sleep interruptible.
            interval = self._interval_sec
            if not self._info_ready_event.is_set():
                interval = min(self._interval_sec, 1.0)
            self._stop_event.wait(interval)

    @staticmethod
    def _fetch_members_blocking(
            endpoint_id: str,
            region: Optional[str]) -> List[str]:
        import boto3
        kwargs: dict = {}
        if region:
            kwargs["region_name"] = region
        client = boto3.client("rds", **kwargs)
        # Resolve the endpoint by its own identifier + a custom-type filter,
        # mirroring the sync CustomEndpointMonitor. Do NOT pass a
        # DBClusterIdentifier: it would have to be the real RDS cluster id, but
        # the wrapper's CLUSTER_ID is an internal alias (e.g. "cluster1"), and
        # supplying it makes describe_db_cluster_endpoints resolve nothing --
        # the monitor then never produces members and connect() fails.
        resp = client.describe_db_cluster_endpoints(
            DBClusterEndpointIdentifier=endpoint_id,
            Filters=[{"Name": "db-cluster-endpoint-type", "Values": ["custom"]}],
        )
        members: List[str] = []
        for endpoint in resp.get("DBClusterEndpoints", []):
            members.extend(endpoint.get("StaticMembers") or [])
        return members

    async def wait_for_info(self, timeout_sec: float) -> bool:
        """Block up to ``timeout_sec`` seconds for the monitor's first
        successful refresh (non-empty member_instance_ids). Returns True
        if info arrived; False on timeout.

        The flag is a ``threading.Event`` set by the polling thread. We wait on
        it via run_in_executor so this coroutine yields the event loop while a
        worker thread does the blocking wait. Safe to call multiple times --
        once set the event stays set for the monitor's lifetime.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._info_ready_event.wait, timeout_sec)

    async def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        self._thread = None
        if thread is not None and thread.is_alive():
            # Join off the event loop so a slow in-flight fetch can't block it.
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, thread.join, 5.0)


class AsyncCustomEndpointPlugin(AsyncPlugin):
    """Async custom endpoint plugin (Task 1-B -- replaces SP-8 stub).

    On initial connect, spawns an :class:`AsyncCustomEndpointMonitor` to
    keep the member instance list fresh. On cleanup (via
    :func:`release_resources_async`), stops the monitor.

    Connection properties (shared with sync custom endpoint plugin):
      * ``custom_endpoint_monitor_info_refresh_rate_sec`` -- default 30s
      * ``cluster_id`` -- Aurora cluster identifier (also used for
        topology monitor)
      * Users identify the custom endpoint by the host portion of the
        connection URL (the endpoint name is the leftmost DNS label).
    """

    _SUBSCRIBED: Set[str] = {DbApiMethod.CONNECT.method_name}

    def __init__(
            self,
            plugin_service: AsyncPluginService,
            props: Properties) -> None:
        self._plugin_service = plugin_service
        self._props = props
        self._monitor: Optional[AsyncCustomEndpointMonitor] = None
        # Telemetry counter -- mirrors sync custom_endpoint_plugin.py:263's
        # "customEndpoint.waitForInfo.counter". Emitted only when the
        # plugin actually awaits ``monitor.wait_for_info(...)`` -- the
        # early-return paths (no monitor, wait disabled) skip the inc.
        # NullTelemetryFactory returns a no-op object; real factories may
        # return None, so the .inc() site guards with ``is not None``.
        tf = self._plugin_service.get_telemetry_factory()
        self._wait_for_info_counter = tf.create_counter(
            "custom_endpoint.wait_for_info.count")

    @property
    def subscribed_methods(self) -> Set[str]:
        return set(self._SUBSCRIBED)

    @property
    def monitor(self) -> Optional[AsyncCustomEndpointMonitor]:
        """Test hook: inspect the monitor instance."""
        return self._monitor

    @property
    def member_instance_ids(self) -> Tuple[str, ...]:
        """Most-recently-cached member instance IDs; empty if no monitor."""
        return self._monitor.member_instance_ids if self._monitor else ()

    async def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: AsyncDriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable[..., Awaitable[Any]]) -> Any:
        # Start the monitor and wait for the first member refresh BEFORE
        # connecting, so allowed_and_blocked_hosts is set and the initial
        # connection / failover host-selection is restricted to the custom
        # endpoint's members. (Connecting first -- as this did originally --
        # lets the chain pick a NON-member instance before the filter exists,
        # which is exactly the bug. Mirrors sync custom_endpoint_plugin, which
        # waits for info before connect_func().)
        if is_initial_connection and self._monitor is None:
            monitor = self._build_monitor(host_info, props)
            if monitor is not None:
                monitor.start()
                self._monitor = monitor
                # Register for cleanup via release_resources_async.
                from aws_advanced_python_wrapper.aio.cleanup import \
                    register_shutdown_hook
                register_shutdown_hook(monitor.stop)

                # N.3: realign with sync's hard-failure contract. Block
                # up to WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS for the
                # first refresh; on timeout, raise AwsWrapperError so callers
                # don't silently see stale/empty allowed-hosts.
                wait_enabled = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO.get_bool(props)
                if wait_enabled is None:
                    wait_enabled = True
                if wait_enabled:
                    timeout_ms = WrapperProperties.WAIT_FOR_CUSTOM_ENDPOINT_INFO_TIMEOUT_MS.get_int(props)
                    if timeout_ms is None or timeout_ms <= 0:
                        timeout_ms = 5000
                    if self._wait_for_info_counter is not None:
                        self._wait_for_info_counter.inc()
                    info_ready = await monitor.wait_for_info(
                        timeout_ms / 1000.0)
                    if not info_ready:
                        raise AwsWrapperError(
                            "Custom endpoint monitor did not produce "
                            "member instance info within "
                            f"{timeout_ms}ms. Increase "
                            "wait_for_custom_endpoint_info_timeout_ms or "
                            "disable wait_for_custom_endpoint_info to "
                            "accept unfiltered connections.")
        return await connect_func()

    def _build_monitor(
            self,
            host_info: HostInfo,
            props: Properties) -> Optional[AsyncCustomEndpointMonitor]:
        """Extract the custom endpoint identifier + region from the host.

        The endpoint is resolved by its OWN identifier and the region is
        derived from the hostname (the host encodes it), mirroring the sync
        CustomEndpointPlugin -- NOT from a DBClusterIdentifier / IAM_REGION.
        The wrapper's CLUSTER_ID is an internal alias (not the real RDS cluster
        id) and IAM_REGION is usually unset, so relying on them for the RDS
        describe call made it resolve nothing.
        """
        # Aurora custom endpoint host format:
        # <endpoint-name>.cluster-custom-<hash>.<region>.rds.amazonaws.com
        host = host_info.host
        if ".cluster-custom-" not in host:
            return None
        endpoint_id = host.split(".", 1)[0]
        cluster_id = WrapperProperties.CLUSTER_ID.get(props)
        if not cluster_id or cluster_id == "1":  # default placeholder
            cluster_id = None
        if not cluster_id:
            # Without a configured cluster id we fail open (no monitor, no
            # custom-endpoint filtering) rather than guess.
            return None
        region = RegionUtils().get_region_from_hostname(host)
        if not region:
            # Fall back to the IAM region prop if the host didn't encode one.
            region_prop = WrapperProperties.IAM_REGION.get(props)
            region = str(region_prop) if region_prop else None
        if not region:
            # Can't query RDS describe_db_cluster_endpoints without a region.
            return None
        return AsyncCustomEndpointMonitor(
            custom_endpoint_identifier=endpoint_id,
            region=region,
            cluster_identifier=str(cluster_id),
            plugin_service=self._plugin_service,
        )


__all__ = [
    "AsyncCustomEndpointMonitor",
    "AsyncCustomEndpointPlugin",
    "CustomEndpointInfo",
    "CustomEndpointRoleType",
]
