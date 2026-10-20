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
#  limitations under the License.s

from concurrent.futures import TimeoutError
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from aws_advanced_python_wrapper.database_dialect import (
    AuroraPgDialect, MultiAzClusterPgDialect, UnknownDatabaseDialect)
from aws_advanced_python_wrapper.errors import (AwsWrapperError,
                                                QueryTimeoutError,
                                                UnsupportedOperationError)
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.plugin_service import PluginServiceImpl
from aws_advanced_python_wrapper.utils.notifications import HostEvent
from aws_advanced_python_wrapper.utils.properties import Properties
from aws_advanced_python_wrapper.utils.utils import Utils


def test_get_host_role_unknown_dialect(mocker):
    mock_conn = MagicMock()
    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = UnknownDatabaseDialect()

    with pytest.raises(UnsupportedOperationError):
        plugin_service.get_host_role(mock_conn)


def test_identify_connection_unknown_dialect(mocker):
    mock_conn = MagicMock()
    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = UnknownDatabaseDialect()
    plugin_service._current_connection = mock_conn

    with pytest.raises(UnsupportedOperationError):
        plugin_service.identify_connection(mock_conn)


def test_get_host_role_reader(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (True,)
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert HostRole.READER == plugin_service.get_host_role(mock_conn)


def test_get_host_role_writer(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (False,)
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert HostRole.WRITER == plugin_service.get_host_role(mock_conn)


def test_get_host_role_error(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = ValueError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(AwsWrapperError):
        plugin_service.get_host_role(mock_conn)


def test_get_host_role_timeout(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = TimeoutError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(QueryTimeoutError):
        plugin_service.get_host_role(mock_conn)


def test_identify_connection_returns_none_when_the_host_id_query_reports_nothing(mocker):
    """Unidentifiable is not a failure: the connect hooks that call this would
    otherwise turn an unreported host id into a failed connect."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is None


def test_identify_connection_timeout(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = TimeoutError()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    with pytest.raises(QueryTimeoutError):
        plugin_service.identify_connection(mock_conn)


def test_identify_connection_no_match_in_topology(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("host-value", "non-matching-host")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = ()
    mock_host_list_provider.force_refresh.return_value = ()

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is None


def test_identify_connection_empty_topology(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("host-value", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = []

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is None


def test_identify_connection_host_in_topology_aurora(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("instance-1", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    expected_host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, HostRole.WRITER, host_id="instance-1")

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = (expected_host,)

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = AuroraPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    host_info = plugin_service.identify_connection(mock_conn)
    assert host_info is not None
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "instance-1" == host_info.host_id


def test_identify_connection_host_in_topology_multiaz(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Multi-AZ returns different values: (instanceId, instanceName). The
    # topology carries the id, which is what the first clause matches on.
    mock_cursor.fetchone.return_value = ("db-WQFQKBTL2LQUPIEFIFBGENS4ZQ", "instance-1")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    expected_host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, HostRole.WRITER,
                             host_id="db-WQFQKBTL2LQUPIEFIFBGENS4ZQ")

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = (expected_host,)

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com", "port": 5432}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = MultiAzClusterPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    # Mock preserve_transaction_status_with_timeout to execute directly
    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    host_info = plugin_service.identify_connection(mock_conn)
    assert host_info is not None
    assert "instance-1.xyz.us-east-2.rds.amazonaws.com" == host_info.host
    assert "db-WQFQKBTL2LQUPIEFIFBGENS4ZQ" == host_info.host_id


def test_identify_connection_matches_on_the_instance_name_column(mocker):
    """The second column is matched against the host, not the host id.

    A topology whose host ids come from somewhere other than host_id_query
    is still identifiable as long as the name column names the host.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (
        "unmatched-id", "instance-1.xyz.us-east-2.rds.amazonaws.com")
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor

    expected_host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, HostRole.WRITER,
                             host_id="db-SOMETHINGELSE")

    mock_container = mocker.MagicMock()
    mock_container.plugin_manager = mocker.MagicMock()
    mock_host_list_provider = mocker.MagicMock()
    mock_host_list_provider.refresh.return_value = (expected_host,)

    plugin_service = PluginServiceImpl(
        mock_container,
        Properties({"host": "test.com", "port": 5432}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock()
    )
    plugin_service._database_dialect = MultiAzClusterPgDialect()
    plugin_service._current_connection = mock_conn
    plugin_service._host_list_provider = mock_host_list_provider

    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert plugin_service.identify_connection(mock_conn) is expected_host


def _service_with_hosts(mocker, hosts, props=None):
    container = mocker.MagicMock()
    container.plugin_manager = mocker.MagicMock()
    service = PluginServiceImpl(
        container,
        Properties(props or {"host": "test.com"}),
        lambda: None,
        mocker.MagicMock(),
        mocker.MagicMock())
    service._all_hosts = tuple(hosts)
    return service, container


def test_identify_connection_returns_none_when_the_host_id_query_errors(mocker):
    """The connect hooks that identify a host call this. A host the database
    declines to name must not fail their connect."""
    mock_conn = MagicMock()
    mock_conn.cursor.side_effect = Exception("relation does not exist")

    service, _ = _service_with_hosts(mocker, ())
    service._database_dialect = AuroraPgDialect()
    service._current_connection = mock_conn
    service._host_list_provider = mocker.MagicMock()

    def mock_preserve(thread_pool, timeout, driver_dialect, conn):
        def decorator(func):
            return func
        return decorator

    mocker.patch('aws_advanced_python_wrapper.database_dialect.preserve_transaction_status_with_timeout', mock_preserve)

    assert service.identify_connection(mock_conn) is None


def test_set_availability_marks_the_host_and_caches_it(mocker):
    host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1")
    service, container = _service_with_hosts(mocker, (host,))

    service.set_availability(host, HostAvailability.UNAVAILABLE)

    assert host.get_raw_availability() == HostAvailability.UNAVAILABLE
    assert service.get_availability(host.url) == HostAvailability.UNAVAILABLE
    changes = container.plugin_manager.notify_host_list_changed.call_args.args[0]
    assert changes == {host.url: {HostEvent.WENT_DOWN, HostEvent.HOST_CHANGED}}


def test_set_availability_matches_by_host_id_and_is_case_insensitive_on_the_name(mocker):
    by_id = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1")
    by_name = HostInfo("Instance-2.XYZ.us-east-2.rds.amazonaws.com", 5432)
    untouched = HostInfo("instance-3.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-3")
    service, _ = _service_with_hosts(mocker, (by_id, by_name, untouched))

    service.set_availability(HostInfo("other.example", 5432, host_id="instance-1"),
                             HostAvailability.UNAVAILABLE)
    service.set_availability(HostInfo("instance-2.xyz.us-east-2.rds.amazonaws.com", 5432),
                             HostAvailability.UNAVAILABLE)

    assert by_id.get_raw_availability() == HostAvailability.UNAVAILABLE
    assert by_name.get_raw_availability() == HostAvailability.UNAVAILABLE
    # Bound the other side: a match is not a licence to mark the whole list.
    assert untouched.get_raw_availability() == HostAvailability.AVAILABLE


def test_set_availability_reports_a_host_coming_back_up(mocker):
    """Inverting the event ternary would otherwise pass the suite."""
    host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432,
                    availability=HostAvailability.UNAVAILABLE)
    service, container = _service_with_hosts(mocker, (host,))

    service.set_availability(host, HostAvailability.AVAILABLE)

    changes = container.plugin_manager.notify_host_list_changed.call_args.args[0]
    assert changes == {host.url: {HostEvent.WENT_UP, HostEvent.HOST_CHANGED}}


def test_set_availability_reports_every_matched_host(mocker):
    """One entry per matched host, so a subscriber that stops at the first
    one drops the rest."""
    first = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="shared")
    second = HostInfo("instance-2.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="shared")
    service, container = _service_with_hosts(mocker, (first, second))

    service.set_availability(HostInfo("other.example", 5432, host_id="shared"),
                             HostAvailability.UNAVAILABLE)

    changes = container.plugin_manager.notify_host_list_changed.call_args.args[0]
    assert set(changes) == {first.url, second.url}


def test_set_availability_reads_availability_through_the_backoff_strategy(mocker):
    """A host whose backoff window has elapsed reads as available again, so
    marking it down is a change the plugins have to hear about."""
    from aws_advanced_python_wrapper.host_availability import \
        ExponentialBackoffHostAvailabilityStrategy

    props = Properties({"host": "test.com",
                        "host_availability_strategy_initial_backoff_time": "1",
                        "host_availability_strategy_max_retries": "5"})
    strategy = ExponentialBackoffHostAvailabilityStrategy(props)
    host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432,
                    availability=HostAvailability.UNAVAILABLE,
                    host_availability_strategy=strategy)
    # Back-date the last change so the retry window has elapsed.
    strategy._not_available_count = 1
    strategy._last_changed = datetime.now() - timedelta(seconds=30)
    assert host.get_availability() == HostAvailability.AVAILABLE

    service, container = _service_with_hosts(mocker, (host,))
    service.set_availability(host, HostAvailability.UNAVAILABLE)

    changes = container.plugin_manager.notify_host_list_changed.call_args.args[0]
    assert changes == {host.url: {HostEvent.WENT_DOWN, HostEvent.HOST_CHANGED}}


def test_set_availability_does_not_notify_when_nothing_changed(mocker):
    host = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    service, container = _service_with_hosts(mocker, (host,))

    service.set_availability(host, HostAvailability.AVAILABLE)

    # The host did match, so this is "matched but unchanged" rather than the
    # early return for an unmatched host.
    assert service.get_availability(host.url) == HostAvailability.AVAILABLE
    container.plugin_manager.notify_host_list_changed.assert_not_called()


def test_set_availability_ignores_a_host_outside_the_host_list(mocker):
    in_list = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    service, container = _service_with_hosts(mocker, (in_list,))
    stranger = HostInfo("stranger.example", 5432)

    service.set_availability(stranger, HostAvailability.UNAVAILABLE)

    assert service.get_availability(stranger.url) is None
    # Bound both sides: neither the argument nor the host list is touched.
    assert stranger.get_raw_availability() == HostAvailability.AVAILABLE
    assert in_list.get_raw_availability() == HostAvailability.AVAILABLE
    container.plugin_manager.notify_host_list_changed.assert_not_called()


def test_connect_clears_a_routed_host_left_by_an_earlier_connect(mocker):
    """The routed host belongs to one connect. A value left behind would be
    read by the next one, which keys the connection tracker and picks the
    monitoring host off the previous connect's instance."""
    service, container = _service_with_hosts(mocker, ())
    stale = HostInfo("reader-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    service.routed_host_info = stale
    observed = {}

    def _record(*args, **kwargs):
        observed["routed"] = service.routed_host_info
        return MagicMock()

    container.plugin_manager.connect = _record
    service.connect(HostInfo("reader-2.xyz.us-east-2.rds.amazonaws.com", 5432), Properties())

    assert observed["routed"] is None


def test_take_routed_host_info_returns_it_once(mocker):
    service, _ = _service_with_hosts(mocker, ())
    routed = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432)
    service.routed_host_info = routed

    assert service.take_routed_host_info() is routed
    assert service.take_routed_host_info() is None


def test_find_host_does_not_match_an_instance_name_against_a_host_id():
    """A name-against-id clause picks the decoy that happens to carry the name
    as its id. On Multi-AZ the name and the id come from different columns, so
    such a clause would match the wrong row rather than be harmless."""
    decoy = HostInfo("decoy.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1")
    wanted = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="db-AAA")

    found = Utils.find_host((decoy, wanted), "db-AAA", "instance-1.xyz.us-east-2.rds.amazonaws.com")

    assert found is wanted


def test_find_host_matches_on_either_column():
    aurora = HostInfo("instance-1.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="instance-1")
    assert Utils.find_host((aurora,), "instance-1", "instance-1") is aurora

    by_name = HostInfo("instance-2.xyz.us-east-2.rds.amazonaws.com", 5432, host_id="db-BBB")
    assert Utils.find_host(
        (by_name,), "unmatched", "instance-2.xyz.us-east-2.rds.amazonaws.com") is by_name


def test_could_not_determine_current_host_raises_resolved_message():
    """Regression (parity review): the terminal current_host_info error
    previously raised the raw message KEY instead of the resolved text."""
    service = PluginServiceImpl.__new__(PluginServiceImpl)
    service._current_host_info = None
    service._initial_connection_host_info = None
    reader = MagicMock()
    reader.role = "reader-not-writer-sentinel"
    with patch.object(PluginServiceImpl, "all_hosts", (reader,)), \
            patch.object(PluginServiceImpl, "hosts", ()):
        with pytest.raises(AwsWrapperError) as exc_info:
            _ = service.current_host_info
    message = str(exc_info.value)
    assert "The current host could not be determined." in message
    assert "PluginServiceImpl.CouldNotDetermineCurrentHost" not in message
