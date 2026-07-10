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

from __future__ import annotations

from unittest.mock import MagicMock

from aws_advanced_python_wrapper.concrete_monitoring_connection_handlers import (
    AuroraMonitoringConnectionHandler, GdbMonitoringConnectionHandler)
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.atomic import AtomicReference
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder

WRITER_EAST = HostInfo("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_EAST = HostInfo("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_WEST = HostInfo("instance3.cluster-ro-xyz.us-west-2.rds.amazonaws.com", 5432, HostRole.READER)


def _make_aurora_handler(priority_value=None):
    props = Properties()
    if priority_value is not None:
        WrapperProperties.MONITORING_CONNECTION_PRIORITY.set(props, priority_value)
    monitoring_conn = ThreadSafeConnectionHolder(None)
    return AuroraMonitoringConnectionHandler(
        monitoring_conn, MagicMock(), MagicMock(), props, Properties()), monitoring_conn


def _make_gdb_handler(priority_value, writer_ref):
    props = Properties()
    WrapperProperties.GDB_MONITORING_CONNECTION_PRIORITY.set(props, priority_value)
    monitoring_conn = ThreadSafeConnectionHolder(None)
    return GdbMonitoringConnectionHandler(
        monitoring_conn, MagicMock(), MagicMock(), props, Properties(), writer_ref), monitoring_conn


class TestAuroraAcceptConnection:
    def test_accepts_first_connection(self):
        handler, monitoring_conn = _make_aurora_handler("strict-writer")
        conn = MagicMock()
        assert handler.accept_connection(conn, True, WRITER_EAST) is True
        assert monitoring_conn.get() is conn

    def test_default_priority_rejects_reader_after_writer(self):
        # Default priority is strict-writer (index 0 for writer, no reader match).
        handler, monitoring_conn = _make_aurora_handler("strict-writer")
        writer_conn = MagicMock()
        handler.accept_connection(writer_conn, True, WRITER_EAST)
        reader_conn = MagicMock()
        # Reader has no matching priority (-> _NO_PRIORITY_INDEX), worse than writer.
        assert handler.accept_connection(reader_conn, False, READER_EAST) is False
        assert monitoring_conn.get() is writer_conn

    def test_higher_priority_replaces(self):
        # Priority list: reader first (index 0), writer second (index 1).
        handler, monitoring_conn = _make_aurora_handler("strict-reader,strict-writer")
        writer_conn = MagicMock()
        # Writer matches index 1.
        assert handler.accept_connection(writer_conn, True, WRITER_EAST) is True
        reader_conn = MagicMock()
        # Reader matches index 0 (higher priority) -> replaces.
        assert handler.accept_connection(reader_conn, False, READER_EAST) is True
        assert monitoring_conn.get() is reader_conn

    def test_lower_priority_rejected(self):
        handler, monitoring_conn = _make_aurora_handler("strict-reader,strict-writer")
        reader_conn = MagicMock()
        handler.accept_connection(reader_conn, False, READER_EAST)  # index 0
        writer_conn = MagicMock()
        # Writer is index 1, worse than current index 0 -> rejected.
        assert handler.accept_connection(writer_conn, True, WRITER_EAST) is False
        assert monitoring_conn.get() is reader_conn


class TestAuroraAcceptConnections:
    def test_selects_best_by_priority(self):
        # writer-or-reader accepts anything at index 0; writer preferred nowhere,
        # so use strict-writer,strict-reader: writer index 0, reader index 1.
        handler, monitoring_conn = _make_aurora_handler("strict-writer,strict-reader")
        writer_conn = MagicMock()
        reader_conn = MagicMock()
        connections = [
            (READER_EAST, ThreadSafeConnectionHolder(reader_conn)),
            (WRITER_EAST, ThreadSafeConnectionHolder(writer_conn)),
        ]
        selected = handler.accept_connections(connections, WRITER_EAST, None)
        assert selected == WRITER_EAST
        assert monitoring_conn.get() is writer_conn

    def test_returns_none_for_empty(self):
        handler, _ = _make_aurora_handler("strict-writer")
        assert handler.accept_connections([], None, None) is None


class TestAuroraFindHostsForPriority:
    def test_strict_writer_filters_writers(self):
        handler, _ = _make_aurora_handler("strict-writer")
        hosts = [WRITER_EAST, READER_EAST]
        assert handler._find_hosts_for_priority(0, hosts) == [WRITER_EAST]

    def test_writer_or_reader_returns_all(self):
        handler, _ = _make_aurora_handler("writer-or-reader")
        hosts = [WRITER_EAST, READER_EAST]
        assert handler._find_hosts_for_priority(0, hosts) == hosts


class TestAuroraClose:
    def test_close_resets_priority_index(self):
        handler, _ = _make_aurora_handler("strict-writer")
        handler.accept_connection(MagicMock(), True, WRITER_EAST)
        handler.close()
        assert handler._current_priority_index == -1


class TestGdbAcceptConnections:
    def test_region_aware_selection_prefers_primary_writer(self):
        writer_ref: AtomicReference = AtomicReference(WRITER_EAST)
        handler, monitoring_conn = _make_gdb_handler(
            "strict-writer-primary,strict-reader-secondary", writer_ref)
        writer_conn = MagicMock()
        reader_conn = MagicMock()
        connections = [
            (READER_WEST, ThreadSafeConnectionHolder(reader_conn)),
            (WRITER_EAST, ThreadSafeConnectionHolder(writer_conn)),
        ]
        # Primary region derived from writer_host_info override (us-east-1).
        selected = handler.accept_connections(connections, WRITER_EAST, None)
        assert selected == WRITER_EAST
        assert monitoring_conn.get() is writer_conn

    def test_secondary_reader_selected_when_only_option(self):
        writer_ref: AtomicReference = AtomicReference(WRITER_EAST)
        handler, monitoring_conn = _make_gdb_handler("strict-reader-secondary", writer_ref)
        reader_conn = MagicMock()
        connections = [(READER_WEST, ThreadSafeConnectionHolder(reader_conn))]
        selected = handler.accept_connections(connections, WRITER_EAST, None)
        assert selected == READER_WEST
        assert monitoring_conn.get() is reader_conn

    def test_primary_region_from_cached_writer(self):
        writer_ref: AtomicReference = AtomicReference(WRITER_EAST)
        handler, _ = _make_gdb_handler("strict-writer-primary", writer_ref)
        assert handler._get_primary_region() == "us-east-1"
