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

import pytest

from aws_advanced_python_wrapper.cluster_topology_monitor import \
    GlobalAuroraTopologyMonitor
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

WRITER_HOME = HostInfo("writer.cluster-xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_HOME = HostInfo("reader1.xyz.us-west-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_OUT = HostInfo("reader2.xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)


def _bare_monitor(accessible_regions, initial_host=WRITER_HOME):
    # The base ClusterTopologyMonitorImpl constructor spins up background
    # threads; build a bare instance and set only the attributes the
    # accessible-regions methods touch.
    monitor = GlobalAuroraTopologyMonitor.__new__(GlobalAuroraTopologyMonitor)
    monitor._accessible_regions = accessible_regions
    monitor._rds_utils = RdsUtils()
    monitor._initial_host_info = initial_host
    return monitor


class TestFilterHostsForNodeMonitoring:
    def test_no_restriction_returns_all(self):
        monitor = _bare_monitor(None)
        hosts = (WRITER_HOME, READER_HOME, READER_OUT)
        assert monitor._filter_hosts_for_host_monitoring(hosts) is hosts

    def test_filters_inaccessible_hosts(self):
        monitor = _bare_monitor(frozenset({"us-west-1"}))
        hosts = (WRITER_HOME, READER_HOME, READER_OUT)

        filtered = monitor._filter_hosts_for_host_monitoring(hosts)

        assert filtered == (WRITER_HOME, READER_HOME)


class TestOpenAnyConnectionInitialHostValidation:
    def test_initial_host_inaccessible_raises(self):
        monitor = _bare_monitor(frozenset({"us-west-1"}), initial_host=READER_OUT)

        with pytest.raises(AwsWrapperError):
            monitor._open_any_connection_and_update_topology()

    def test_initial_host_accessible_delegates_to_super(self, monkeypatch):
        monitor = _bare_monitor(frozenset({"us-west-1"}), initial_host=WRITER_HOME)
        sentinel = (WRITER_HOME,)
        monkeypatch.setattr(
            "aws_advanced_python_wrapper.cluster_topology_monitor."
            "ClusterTopologyMonitorImpl._open_any_connection_and_update_topology",
            lambda self: sentinel,
        )

        assert monitor._open_any_connection_and_update_topology() is sentinel

    def test_no_restriction_delegates_to_super(self, monkeypatch):
        monitor = _bare_monitor(None, initial_host=READER_OUT)
        sentinel = (READER_OUT,)
        monkeypatch.setattr(
            "aws_advanced_python_wrapper.cluster_topology_monitor."
            "ClusterTopologyMonitorImpl._open_any_connection_and_update_topology",
            lambda self: sentinel,
        )

        assert monitor._open_any_connection_and_update_topology() is sentinel
