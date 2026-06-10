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

from unittest.mock import MagicMock

import pytest

from aws_advanced_python_wrapper.aurora_initial_connection_strategy_plugin import (
    AuroraInitialConnectionStrategyPlugin, InstanceSubstitutionStrategy)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType

WRITER_INSTANCE = "instance-1.xyz.us-east-1.rds.amazonaws.com"
READER_INSTANCE = "instance-2.xyz.us-east-1.rds.amazonaws.com"
WRITER_CLUSTER = "mycluster.cluster-xyz.us-east-1.rds.amazonaws.com"


def _plugin(props, all_hosts=()):
    plugin_service = MagicMock()
    plugin_service.all_hosts = all_hosts
    plugin_service.hosts = all_hosts
    plugin_service.accepts_strategy.return_value = True
    plugin_service.is_login_exception.return_value = False
    plugin_service.is_network_exception.return_value = False
    plugin_service.is_read_only_connection_exception.return_value = False

    plugin = AuroraInitialConnectionStrategyPlugin(plugin_service, props)
    plugin._host_list_provider_service = MagicMock()
    return plugin, plugin_service


def test_retry_deadline_uses_timeout_property():
    props = Properties({})
    # Zero total budget: the retry loop must not run even once despite the long interval.
    WrapperProperties.OPEN_CONNECTION_RETRY_TIMEOUT_MS.set(props, "0")
    WrapperProperties.OPEN_CONNECTION_RETRY_INTERVAL_MS.set(props, "600000")

    plugin, _ = _plugin(props)
    connect_func = MagicMock()

    with pytest.raises(AwsWrapperError):
        plugin.connect(
            MagicMock(), MagicMock(), HostInfo(WRITER_CLUSTER), props, True, connect_func)

    connect_func.assert_not_called()


def test_wait_for_initial_topology_disabled_by_default():
    props = Properties({})
    plugin, plugin_service = _plugin(props)
    # get_int returns -1 for an absent property; the plugin normalizes that to 0.
    assert plugin._wait_for_initial_topology_ms == 0

    fallback_conn = MagicMock()
    connect_func = MagicMock(return_value=fallback_conn)

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is fallback_conn
    assert host.host == WRITER_CLUSTER
    plugin_service.force_refresh_host_list.assert_called_once_with(fallback_conn)
    plugin_service.force_monitoring_refresh_host_list.assert_not_called()


def test_wait_for_initial_topology_connects_to_instance_after_wait():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    writer = HostInfo(WRITER_INSTANCE, role=HostRole.WRITER)
    plugin, plugin_service = _plugin(props, all_hosts=())
    plugin_service.force_monitoring_refresh_host_list.return_value = True

    fallback_conn = MagicMock()
    instance_conn = MagicMock()
    connect_func = MagicMock(return_value=fallback_conn)

    # Topology is empty on the first selection attempt and populated after the wait.
    def populate_topology(*_args, **_kwargs):
        plugin_service.all_hosts = (writer,)
        plugin_service.hosts = (writer,)
        return True

    plugin_service.force_monitoring_refresh_host_list.side_effect = populate_topology
    plugin_service.connect.return_value = instance_conn

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is instance_conn
    assert host.host == WRITER_INSTANCE
    # The timeout must reach the monitor in seconds.
    plugin_service.force_monitoring_refresh_host_list.assert_called_once_with(True, 5.0)
    fallback_conn.close.assert_called_once()


def test_wait_for_initial_topology_timeout_keeps_fallback():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    plugin, plugin_service = _plugin(props)
    plugin_service.force_monitoring_refresh_host_list.return_value = False

    fallback_conn = MagicMock()
    connect_func = MagicMock(return_value=fallback_conn)

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is fallback_conn
    assert host.host == WRITER_CLUSTER
    fallback_conn.close.assert_not_called()


def test_wait_for_initial_topology_survives_unsupported_provider():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    plugin, plugin_service = _plugin(props)
    plugin_service.force_monitoring_refresh_host_list.side_effect = AwsWrapperError(
        "Force monitoring refresh is not supported.")

    fallback_conn = MagicMock()
    connect_func = MagicMock(return_value=fallback_conn)

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is fallback_conn
    assert host.host == WRITER_CLUSTER


def test_wait_for_initial_topology_instance_connect_failure_keeps_fallback():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    writer = HostInfo(WRITER_INSTANCE, role=HostRole.WRITER)
    # Topology must be empty initially so the instance is only selected after the wait.
    plugin, plugin_service = _plugin(props, all_hosts=())
    plugin_service.connect.side_effect = AwsWrapperError("instance unreachable")

    def populate_topology(*_args, **_kwargs):
        plugin_service.all_hosts = (writer,)
        plugin_service.hosts = (writer,)
        return True

    plugin_service.force_monitoring_refresh_host_list.side_effect = populate_topology

    fallback_conn = MagicMock()
    connect_func = MagicMock(return_value=fallback_conn)

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is fallback_conn
    assert host.host == WRITER_CLUSTER
    fallback_conn.close.assert_not_called()


def test_do_not_substitute_skips_topology_wait_entirely():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    plugin, plugin_service = _plugin(props)
    original_conn = MagicMock()
    connect_func = MagicMock(return_value=original_conn)

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.DO_NOT_SUBSTITUTE,
        props,
        connect_func)

    assert conn is original_conn
    assert host.host == WRITER_CLUSTER
    plugin_service.force_monitoring_refresh_host_list.assert_not_called()
    plugin_service.force_refresh_host_list.assert_not_called()


def test_available_topology_connects_directly_without_wait():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    writer = HostInfo(WRITER_INSTANCE, role=HostRole.WRITER)
    plugin, plugin_service = _plugin(props, all_hosts=(writer,))
    instance_conn = MagicMock()
    plugin_service.connect.return_value = instance_conn
    connect_func = MagicMock()

    host, conn = plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        connect_func)

    assert conn is instance_conn
    assert host.host == WRITER_INSTANCE
    connect_func.assert_not_called()
    plugin_service.force_monitoring_refresh_host_list.assert_not_called()


def test_instance_connect_skips_this_plugin():
    props = Properties({})
    writer = HostInfo(WRITER_INSTANCE, role=HostRole.WRITER)
    plugin, plugin_service = _plugin(props, all_hosts=(writer,))
    plugin_service.connect.return_value = MagicMock()

    plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        MagicMock())

    plugin_service.connect.assert_called_once()
    assert plugin_service.connect.call_args.args[2] is plugin


def test_post_wait_instance_connect_skips_this_plugin():
    props = Properties({})
    WrapperProperties.WAIT_FOR_INITIAL_TOPOLOGY_MS.set(props, "5000")

    writer = HostInfo(WRITER_INSTANCE, role=HostRole.WRITER)
    plugin, plugin_service = _plugin(props, all_hosts=())
    plugin_service.connect.return_value = MagicMock()

    def populate_topology(*_args, **_kwargs):
        plugin_service.all_hosts = (writer,)
        plugin_service.hosts = (writer,)
        return True

    plugin_service.force_monitoring_refresh_host_list.side_effect = populate_topology

    plugin._open_candidate_connection(
        HostInfo(WRITER_CLUSTER),
        RdsUrlType.RDS_WRITER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_WRITER,
        props,
        MagicMock(return_value=MagicMock()))

    plugin_service.connect.assert_called_once()
    assert plugin_service.connect.call_args.args[2] is plugin


def test_substitute_with_any_raises_unsupported_strategy():
    props = Properties({})
    reader = HostInfo(READER_INSTANCE, role=HostRole.READER)
    plugin, plugin_service = _plugin(props, all_hosts=(reader,))

    with pytest.raises(AwsWrapperError):
        plugin._get_candidate_host(
            HostInfo("mycluster.cluster-custom-xyz.us-east-1.rds.amazonaws.com"),
            RdsUrlType.RDS_CUSTOM_CLUSTER,
            InstanceSubstitutionStrategy.SUBSTITUTE_WITH_ANY)

    # The selector must never be consulted for an unsupported role.
    plugin_service.get_host_info_by_strategy.assert_not_called()


def test_candidate_host_region_filter_uses_allow_block_filtered_hosts():
    props = Properties({})
    in_region = HostInfo(READER_INSTANCE, role=HostRole.READER)  # us-east-1
    blocked = HostInfo("blocked-inst.xyz.us-east-1.rds.amazonaws.com", role=HostRole.READER)
    plugin, plugin_service = _plugin(props, all_hosts=(in_region, blocked))
    # A custom endpoint is active: hosts excludes the blocked instance.
    plugin_service.hosts = (in_region,)
    plugin_service.get_host_info_by_strategy.return_value = in_region

    reader_cluster = HostInfo("mycluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com")
    plugin._get_candidate_host(
        reader_cluster,
        RdsUrlType.RDS_READER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER)

    passed_list = plugin_service.get_host_info_by_strategy.call_args.args[2]
    assert in_region in passed_list
    assert blocked not in passed_list


def test_candidate_host_substitute_with_reader_uses_reader_role():
    """The reader substitution passes HostRole.READER to the selector."""
    props = Properties({})
    reader = HostInfo(READER_INSTANCE, role=HostRole.READER)
    plugin, plugin_service = _plugin(props, all_hosts=(reader,))
    plugin_service.get_host_info_by_strategy.return_value = reader

    result = plugin._get_candidate_host(
        HostInfo("mycluster.cluster-ro-xyz.us-east-1.rds.amazonaws.com"),
        RdsUrlType.RDS_READER_CLUSTER,
        InstanceSubstitutionStrategy.SUBSTITUTE_WITH_READER)

    assert result is reader
    assert plugin_service.get_host_info_by_strategy.call_args.args[0] == HostRole.READER
