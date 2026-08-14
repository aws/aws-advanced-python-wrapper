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

"""Unit coverage for the ``some_regions_inaccessible`` panic-exit
machinery of ``ClusterTopologyMonitorImpl`` — the paths that let a GDB cluster
exit panic mode when the writer lives in an inaccessible region.

These tests exercise the *decision logic* in isolation with mocked host
threads: connection harvesting (``_adopt_harvested_monitoring_connection``), the stable-reader
fallback (``_check_for_stable_reader_topologies``), reader-observed
writer-change detection (``HostMonitor._reader_thread_fetch_topology``), and the
worker connection hand-off (``_harvest_connection``). They do not spin up real
monitoring threads — a bare monitor is built via ``__new__`` and only the
attributes each method touches are populated (same approach as
``test_global_aurora_topology_monitor_accessible_regions.py``)."""

from __future__ import annotations

import threading
from typing import List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.cluster_topology_monitor import (
    ClusterTopologyMonitorImpl, HostMonitor)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.atomic import AtomicReference
from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder

WRITER = HostInfo("writer.cluster-xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_A = HostInfo("reader1.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_B = HostInfo("reader2.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
NEW_WRITER = HostInfo("writer2.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)


def _host_and_port(h: HostInfo) -> str:
    return f"{h.host}:{h.port}"


def _bare_monitor(*, accessible_filter=None) -> ClusterTopologyMonitorImpl:
    """Build a monitor without running its constructor (which starts threads).

    ``accessible_filter`` optionally overrides ``_filter_hosts_for_host_monitoring``
    to simulate the GDB subclass dropping inaccessible hosts.
    """
    monitor = ClusterTopologyMonitorImpl.__new__(ClusterTopologyMonitorImpl)
    monitor._cluster_id = "cluster-xyz"
    monitor._host_threads_map_lock = threading.Lock()
    monitor._host_threads_connections = {}
    monitor._reader_topologies_by_id = {}
    monitor._completed_one_cycle = {}
    monitor._stable_topologies_start_nano = 0
    monitor._reader_observed_writer_host_info = AtomicReference(None)
    monitor._host_threads_stop = threading.Event()
    monitor._host_threads_latest_topology = AtomicReference(None)
    monitor._thread_pool_executor = AtomicReference(None)
    monitor._submitted_hosts = {}
    monitor._monitoring_connection = ThreadSafeConnectionHolder(None)
    monitor._writer_host_info = AtomicReference(None)
    monitor._connection_handler = MagicMock()

    if accessible_filter is not None:
        monitor._filter_hosts_for_host_monitoring = accessible_filter  # type: ignore[method-assign]
    return monitor


class TestHarvestConnections:
    def test_returns_false_and_clears_when_no_connections(self):
        monitor = _bare_monitor()
        # No harvested connections in the map.
        result = monitor._adopt_harvested_monitoring_connection(None, (WRITER, READER_A))

        assert result is False
        # State cleared; handler never consulted.
        assert monitor._host_threads_connections == {}
        monitor._connection_handler.accept_connections.assert_not_called()

    def test_adopts_selected_and_closes_the_rest(self):
        monitor = _bare_monitor()
        adopted_conn, dropped_conn = MagicMock(name="adopted"), MagicMock(name="dropped")
        adopted_holder = ThreadSafeConnectionHolder(adopted_conn)
        dropped_holder = ThreadSafeConnectionHolder(dropped_conn)
        monitor._host_threads_connections = {
            _host_and_port(READER_A): (READER_A, adopted_holder),
            _host_and_port(READER_B): (READER_B, dropped_holder),
        }
        # Handler adopts READER_A.
        monitor._connection_handler.accept_connections.return_value = READER_A

        result = monitor._adopt_harvested_monitoring_connection(None, (READER_A, READER_B))

        assert result is True
        monitor._connection_handler.accept_connections.assert_called_once()
        # The non-adopted connection is closed; the adopted one is left alone.
        dropped_conn.close.assert_called_once()
        adopted_conn.close.assert_not_called()
        # Map is emptied after harvest.
        assert monitor._host_threads_connections == {}

    def test_joins_workers_before_reading_map(self):
        """The executor must be joined (wait=True) BEFORE the harvest reads the
        map, so no worker is still touching a connection being handed off."""
        monitor = _bare_monitor()
        order: List[str] = []

        executor = MagicMock()
        executor.shutdown.side_effect = lambda **kw: order.append("shutdown")
        monitor._thread_pool_executor = AtomicReference(executor)

        holder = ThreadSafeConnectionHolder(MagicMock())
        monitor._host_threads_connections = {_host_and_port(READER_A): (READER_A, holder)}

        def _accept(conns, writer, topo):
            order.append("accept_connections")
            return READER_A
        monitor._connection_handler.accept_connections.side_effect = _accept

        monitor._adopt_harvested_monitoring_connection(None, (READER_A,))

        assert order == ["shutdown", "accept_connections"]
        executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)

    def test_no_selection_closes_all(self):
        monitor = _bare_monitor()
        c1, c2 = MagicMock(), MagicMock()
        monitor._host_threads_connections = {
            _host_and_port(READER_A): (READER_A, ThreadSafeConnectionHolder(c1)),
            _host_and_port(READER_B): (READER_B, ThreadSafeConnectionHolder(c2)),
        }
        monitor._connection_handler.accept_connections.return_value = None

        result = monitor._adopt_harvested_monitoring_connection(None, (READER_A, READER_B))

        assert result is False
        c1.close.assert_called_once()
        c2.close.assert_called_once()


class TestHandOffConnection:
    def test_moves_connection_into_map(self):
        monitor = _bare_monitor()
        conn = MagicMock()

        monitor._harvest_connection(READER_A, conn)

        key = _host_and_port(READER_A)
        assert key in monitor._host_threads_connections
        host, holder = monitor._host_threads_connections[key]
        assert host == READER_A
        assert holder.get() is conn
        conn.close.assert_not_called()

    def test_replacing_existing_entry_closes_previous(self):
        monitor = _bare_monitor()
        old_conn, new_conn = MagicMock(name="old"), MagicMock(name="new")
        monitor._harvest_connection(READER_A, old_conn)

        monitor._harvest_connection(READER_A, new_conn)

        old_conn.close.assert_called_once()
        _, holder = monitor._host_threads_connections[_host_and_port(READER_A)]
        assert holder.get() is new_conn


class TestReaderObservedWriterChange:
    def _worker(self, monitor, *, some_regions_inaccessible: bool,
                baseline_writer: Optional[HostInfo]) -> HostMonitor:
        worker = HostMonitor.__new__(HostMonitor)
        worker._monitor = monitor
        worker._host_info = READER_A
        worker._writer_host_info = baseline_writer
        worker._some_regions_inaccessible = some_regions_inaccessible
        worker._writer_changed = False
        worker._connection_attempts = 0
        return worker

    def _topology_with_writer(self, writer: HostInfo) -> Tuple[HostInfo, ...]:
        return (writer, READER_A, READER_B)

    def test_writer_change_signals_exit_when_regions_inaccessible(self, monkeypatch):
        monitor = _bare_monitor()
        monitor._update_topology_cache = MagicMock()  # type: ignore[method-assign]
        monitor._record_reader_topology = MagicMock()  # type: ignore[method-assign]
        worker = self._worker(
            monitor, some_regions_inaccessible=True, baseline_writer=WRITER)

        conn = MagicMock()
        monkeypatch.setattr(
            monitor, "_query_for_topology",
            lambda c: self._topology_with_writer(NEW_WRITER), raising=False)

        worker._reader_thread_fetch_topology(conn)

        # First observer wins the CAS and signals panic-mode exit.
        assert monitor._reader_observed_writer_host_info.get() == NEW_WRITER
        assert monitor._host_threads_stop.is_set()
        assert worker._writer_changed is True

    def test_writer_change_does_not_signal_when_all_regions_accessible(self, monkeypatch):
        monitor = _bare_monitor()
        monitor._update_topology_cache = MagicMock()  # type: ignore[method-assign]
        monitor._record_reader_topology = MagicMock()  # type: ignore[method-assign]
        worker = self._worker(
            monitor, some_regions_inaccessible=False, baseline_writer=WRITER)

        conn = MagicMock()
        monkeypatch.setattr(
            monitor, "_query_for_topology",
            lambda c: self._topology_with_writer(NEW_WRITER), raising=False)

        worker._reader_thread_fetch_topology(conn)

        # Writer change is detected, but no panic-exit signal (defer to the
        # standard host-thread-verifies-writer path).
        assert worker._writer_changed is True
        assert monitor._reader_observed_writer_host_info.get() is None
        assert not monitor._host_threads_stop.is_set()

    def test_no_signal_when_writer_unchanged(self, monkeypatch):
        monitor = _bare_monitor()
        monitor._update_topology_cache = MagicMock()  # type: ignore[method-assign]
        monitor._record_reader_topology = MagicMock()  # type: ignore[method-assign]
        worker = self._worker(
            monitor, some_regions_inaccessible=True, baseline_writer=WRITER)

        conn = MagicMock()
        monkeypatch.setattr(
            monitor, "_query_for_topology",
            lambda c: self._topology_with_writer(WRITER), raising=False)

        worker._reader_thread_fetch_topology(conn)

        assert worker._writer_changed is False
        assert monitor._reader_observed_writer_host_info.get() is None
        assert not monitor._host_threads_stop.is_set()

    def test_only_first_observer_wins_cas(self, monkeypatch):
        monitor = _bare_monitor()
        monitor._update_topology_cache = MagicMock()  # type: ignore[method-assign]
        monitor._record_reader_topology = MagicMock()  # type: ignore[method-assign]
        # A different worker already recorded an observed writer change.
        already = HostInfo("writer3.xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)
        monitor._reader_observed_writer_host_info.set(already)

        worker = self._worker(
            monitor, some_regions_inaccessible=True, baseline_writer=WRITER)
        conn = MagicMock()
        monkeypatch.setattr(
            monitor, "_query_for_topology",
            lambda c: self._topology_with_writer(NEW_WRITER), raising=False)

        worker._reader_thread_fetch_topology(conn)

        # CAS-from-None fails; the earlier observation is retained.
        assert monitor._reader_observed_writer_host_info.get() == already


class TestCheckForStableReaderTopologies:
    def _prime(self, monitor, hosts, *, monkeypatch, stored=None):
        stored = hosts if stored is None else stored
        monkeypatch.setattr(monitor, "_get_stored_hosts", lambda: stored, raising=False)
        monitor._update_topology_cache = MagicMock()  # type: ignore[method-assign]

    def test_waits_until_every_monitored_reader_completed_a_cycle(self, monkeypatch):
        monitor = _bare_monitor()
        hosts = (READER_A, READER_B)
        self._prime(monitor, hosts, monkeypatch=monkeypatch)
        # Only READER_A has completed a cycle.
        monitor._completed_one_cycle = {_host_and_port(READER_A): True}
        monitor._reader_topologies_by_id = {_host_and_port(READER_A): hosts}

        monitor._check_for_stable_reader_topologies()

        # Not stable yet; timer not started, no harvest.
        assert monitor._stable_topologies_start_nano == 0
        monitor._connection_handler.accept_connections.assert_not_called()

    def test_resets_timer_when_reader_topologies_disagree(self, monkeypatch):
        monitor = _bare_monitor()
        hosts = (READER_A, READER_B)
        self._prime(monitor, hosts, monkeypatch=monkeypatch)
        monitor._completed_one_cycle = {
            _host_and_port(READER_A): True, _host_and_port(READER_B): True}
        # Two readers disagree on topology.
        monitor._reader_topologies_by_id = {
            _host_and_port(READER_A): (READER_A, READER_B),
            _host_and_port(READER_B): (READER_A,),
        }
        monitor._stable_topologies_start_nano = 1  # pretend a timer was running

        monitor._check_for_stable_reader_topologies()

        assert monitor._stable_topologies_start_nano == 0
        monitor._connection_handler.accept_connections.assert_not_called()

    def test_starts_timer_when_agreement_first_reached(self, monkeypatch):
        monitor = _bare_monitor()
        hosts = (READER_A, READER_B)
        self._prime(monitor, hosts, monkeypatch=monkeypatch)
        monitor._completed_one_cycle = {
            _host_and_port(READER_A): True, _host_and_port(READER_B): True}
        monitor._reader_topologies_by_id = {
            _host_and_port(READER_A): hosts,
            _host_and_port(READER_B): hosts,
        }
        monitor.get_stable_topologies_duration_ns = lambda: 10 ** 12  # type: ignore[method-assign]

        monitor._check_for_stable_reader_topologies()

        # Timer started this cycle; duration not yet elapsed, so no harvest.
        assert monitor._stable_topologies_start_nano != 0
        monitor._connection_handler.accept_connections.assert_not_called()

    def test_harvests_after_stability_duration_elapses(self, monkeypatch):
        monitor = _bare_monitor()
        hosts = (READER_A, READER_B)
        self._prime(monitor, hosts, monkeypatch=monkeypatch)
        monitor._completed_one_cycle = {
            _host_and_port(READER_A): True, _host_and_port(READER_B): True}
        monitor._reader_topologies_by_id = {
            _host_and_port(READER_A): hosts,
            _host_and_port(READER_B): hosts,
        }
        # Timer already started far enough in the past that stability elapsed.
        monitor._stable_topologies_start_nano = 1
        monitor.get_stable_topologies_duration_ns = lambda: 0  # type: ignore[method-assign]
        # A harvested reader connection is available and gets adopted.
        holder = ThreadSafeConnectionHolder(MagicMock())
        monitor._host_threads_connections = {_host_and_port(READER_A): (READER_A, holder)}
        monitor._connection_handler.accept_connections.return_value = READER_A
        # No verified writer (it's in an inaccessible region).
        monitor._is_verified_writer_connection = False

        monitor._check_for_stable_reader_topologies()

        # Topology cache updated with the agreed reader topology, and the
        # harvest adopted a connection to exit panic mode.
        monitor._update_topology_cache.assert_called_once_with(hosts)
        monitor._connection_handler.accept_connections.assert_called_once()
        assert monitor._is_verified_writer_connection is True

    def test_skips_harvest_when_monitoring_connection_already_present(self, monkeypatch):
        monitor = _bare_monitor()
        hosts = (READER_A, READER_B)
        self._prime(monitor, hosts, monkeypatch=monkeypatch)
        monitor._completed_one_cycle = {
            _host_and_port(READER_A): True, _host_and_port(READER_B): True}
        monitor._reader_topologies_by_id = {
            _host_and_port(READER_A): hosts,
            _host_and_port(READER_B): hosts,
        }
        monitor._stable_topologies_start_nano = 1
        monitor.get_stable_topologies_duration_ns = lambda: 0  # type: ignore[method-assign]
        # Monitoring connection already established → no harvest needed.
        monitor._monitoring_connection = ThreadSafeConnectionHolder(MagicMock())

        monitor._check_for_stable_reader_topologies()

        monitor._connection_handler.accept_connections.assert_not_called()

    def test_no_stored_hosts_resets_timer(self, monkeypatch):
        monitor = _bare_monitor()
        self._prime(monitor, (), monkeypatch=monkeypatch, stored=())
        monitor._stable_topologies_start_nano = 123

        monitor._check_for_stable_reader_topologies()

        assert monitor._stable_topologies_start_nano == 0

    def test_stability_only_requires_monitored_hosts(self, monkeypatch):
        """A GDB subclass filters out inaccessible hosts; those must not block
        stability by appearing perpetually incomplete."""
        monitor = _bare_monitor(
            accessible_filter=lambda hosts: tuple(h for h in hosts if h != READER_B))
        stored = (READER_A, READER_B)  # READER_B is inaccessible
        self._prime(monitor, stored, monkeypatch=monkeypatch, stored=stored)
        # Only the monitored host (READER_A) completed a cycle.
        monitor._completed_one_cycle = {_host_and_port(READER_A): True}
        monitor._reader_topologies_by_id = {_host_and_port(READER_A): (READER_A,)}
        monitor.get_stable_topologies_duration_ns = lambda: 10 ** 12  # type: ignore[method-assign]

        monitor._check_for_stable_reader_topologies()

        # READER_B never completed a cycle, but it's not monitored, so stability
        # is not blocked: the timer starts.
        assert monitor._stable_topologies_start_nano != 0


class TestClearHostThreadsState:
    def test_clears_all_panic_exit_state(self):
        monitor = _bare_monitor()
        monitor._host_threads_connections = {"x": (READER_A, ThreadSafeConnectionHolder(None))}
        monitor._reader_topologies_by_id = {"x": (READER_A,)}
        monitor._completed_one_cycle = {"x": True}
        monitor._stable_topologies_start_nano = 999
        monitor._reader_observed_writer_host_info.set(NEW_WRITER)

        monitor._clear_host_threads_state()

        assert monitor._host_threads_connections == {}
        assert monitor._reader_topologies_by_id == {}
        assert monitor._completed_one_cycle == {}
        assert monitor._stable_topologies_start_nano == 0
        assert monitor._reader_observed_writer_host_info.get() is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
