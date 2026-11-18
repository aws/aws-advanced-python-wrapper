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

import datetime
import socket

import psycopg
import psycopg.errors
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from mysql.connector.errors import \
    ConnectionTimeoutError as MySQLConnectionTimeoutError
from mysql.connector.errors import InterfaceError, OperationalError

import tests.integration.container.utils.rds_test_utility as rds_test_utility
from aws_advanced_python_wrapper.errors import (AwsConnectError,
                                                FailoverFailedError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.test_timings import (
    FAILOVER_DEADLINE_SEC, FAILOVER_MAX_REISSUES,
    INITIAL_WRITER_POLL_INTERVAL_SEC, INITIAL_WRITER_REVERT_WINDOW_SEC,
    INSTANCE_STATUS_POLL_INTERVAL_SEC, INSTANCE_UP_TIMEOUT_SEC,
    WRITER_CHANGE_TIMEOUT_SEC)

_START_SEC = 1000.0


def _described_instances(instance_2_status):
    return {"DBInstances": [
        {"DBInstanceIdentifier": "instance-1", "DBInstanceStatus": "available"},
        {"DBInstanceIdentifier": "instance-2", "DBInstanceStatus": instance_2_status},
    ]}


@pytest.fixture
def clock(mocker):
    now = [_START_SEC]

    def _sleep(seconds):
        now[0] += seconds

    mocker.patch.object(rds_test_utility, "sleep", side_effect=_sleep)
    mocker.patch.object(rds_test_utility, "timeit").default_timer.side_effect = lambda: now[0]
    return now


@pytest.fixture
def utility(mocker, clock):
    mocker.patch.object(rds_test_utility.boto3, "client")
    env = mocker.patch.object(rds_test_utility, "TestEnvironment").get_current.return_value
    env.get_deployment.return_value = DatabaseEngineDeployment.AURORA
    env.get_engine.return_value = DatabaseEngine.PG
    mocker.patch.object(rds_test_utility, "socket").gethostbyname.side_effect = ["10.0.0.1", "10.0.0.2"]

    target = rds_test_utility.RdsTestUtility("us-east-2")
    target._client.describe_db_instances.return_value = _described_instances("available")
    mocker.patch.object(target, "writer_changed", return_value=True)
    mocker.patch.object(target, "make_sure_instances_up")
    mocker.patch.object(target, "failover_cluster")
    mocker.patch.object(target, "wait_until_cluster_has_desired_status")
    mocker.patch.object(target, "_open_connection")
    mocker.patch.object(target, "query_host_role")
    return target, env


def test_failover_waits_until_initial_writer_accepts_connections(utility, mocker):
    target, env = utility
    target._open_connection.side_effect = [ConnectionRefusedError("starting up"), mocker.MagicMock()]
    target.query_host_role.return_value = HostRole.READER

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1
    assert target._open_connection.call_count == 2
    env.get_database_info.return_value.get_instance.assert_called_with("instance-1")
    target._open_connection.assert_called_with(env.get_database_info.return_value.get_instance.return_value)


def test_failover_initial_writer_stays_writer_reissued(utility):
    target, _ = utility
    target.query_host_role.side_effect = lambda conn, engine: (
        HostRole.WRITER if target.failover_cluster.call_count == 1 else HostRole.READER)

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 2
    target.failover_cluster.assert_called_with("cluster", None, FAILOVER_DEADLINE_SEC - INITIAL_WRITER_REVERT_WINDOW_SEC)


def test_initial_writer_kept_writer_role_skips_non_aurora(utility, clock):
    target, env = utility
    env.get_deployment.return_value = DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER

    assert target._initial_writer_kept_writer_role("instance-1", clock[0] + FAILOVER_DEADLINE_SEC) is False
    target._open_connection.assert_not_called()


def test_failover_waits_through_short_writer_phase(utility, mocker):
    target, _ = utility
    target._open_connection.side_effect = [mocker.MagicMock()] * 55 + [ConnectionRefusedError("restarting"), mocker.MagicMock()]
    target.query_host_role.side_effect = [HostRole.WRITER] * 55 + [HostRole.READER]

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1
    assert target._open_connection.call_count == 57


def test_failover_waits_through_writer_phase_after_long_restart(utility, mocker):
    target, _ = utility
    target._open_connection.side_effect = [ConnectionRefusedError("restarting")] * 60 + [mocker.MagicMock()] * 2
    target.query_host_role.side_effect = [HostRole.WRITER, HostRole.READER]

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_failover_initial_writer_unreachable_returns_at_deadline(utility, clock, mocker):
    target, _ = utility

    def _slow_new_writer(initial_writer_id, cluster_id, timeout_sec):
        clock[0] += 200
        return True

    target.writer_changed.side_effect = _slow_new_writer
    target._open_connection.side_effect = ConnectionRefusedError("starting up")

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    target.failover_cluster.assert_called_once_with("cluster", None, FAILOVER_DEADLINE_SEC)
    target.make_sure_instances_up.assert_called_once_with(mocker.ANY, _START_SEC + FAILOVER_DEADLINE_SEC)
    assert clock[0] - _START_SEC <= FAILOVER_DEADLINE_SEC


def test_failover_no_new_writer_fails_at_deadline(utility, clock):
    target, _ = utility

    def _no_new_writer(initial_writer_id, cluster_id, timeout_sec):
        clock[0] += timeout_sec
        return False

    target.writer_changed.side_effect = _no_new_writer
    target._open_connection.side_effect = ConnectionRefusedError("unreachable")

    with pytest.raises(pytest.fail.Exception, match="ConnectionRefusedError: unreachable") as excinfo:
        target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1
    assert clock[0] - _START_SEC <= FAILOVER_DEADLINE_SEC
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is True


def test_failover_writer_unchanged_reissued(utility, clock):
    target, _ = utility

    def _new_writer_after_reissue(initial_writer_id, cluster_id, timeout_sec):
        if target.failover_cluster.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_reissue
    target.query_host_role.side_effect = lambda conn, engine: (
        HostRole.WRITER if target.failover_cluster.call_count == 1 else HostRole.READER)

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 2


def test_failover_near_deadline_not_reissued(utility, clock):
    target, _ = utility

    def _slow_new_writer(initial_writer_id, cluster_id, timeout_sec):
        clock[0] += 300
        return True

    target.writer_changed.side_effect = _slow_new_writer
    target.query_host_role.return_value = HostRole.WRITER

    with pytest.raises(Exception, match="not successful"):
        target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_make_sure_instances_up_tries_once_after_deadline(utility, clock):
    target, _ = utility
    target._open_connection.side_effect = ConnectionRefusedError("starting up")

    with pytest.raises(pytest.fail.Exception, match="instance-2") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] - 1)

    assert target._open_connection.call_count == 1
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is True


def test_failover_reissues_at_most_max_times(utility):
    target, _ = utility
    target.query_host_role.return_value = HostRole.WRITER

    with pytest.raises(Exception, match="not successful"):
        target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1 + FAILOVER_MAX_REISSUES


def test_failover_initial_writer_reader_not_reissued(utility, clock):
    target, _ = utility

    def _new_writer_after_lag(initial_writer_id, cluster_id, timeout_sec):
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_lag
    target.query_host_role.return_value = HostRole.READER

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_failover_writer_window_resets_after_restart(utility, mocker):
    target, _ = utility
    target._open_connection.side_effect = [mocker.MagicMock()] + [ConnectionRefusedError("restarting")] * 70 + [mocker.MagicMock()] * 2
    target.query_host_role.side_effect = [HostRole.WRITER, HostRole.WRITER, HostRole.READER]

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_failover_needs_writer_window_to_reissue(utility, clock):
    target, _ = utility

    def _new_writer_after_lag(initial_writer_id, cluster_id, timeout_sec):
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_lag
    target.query_host_role.side_effect = [HostRole.WRITER] * 10 + [HostRole.READER] * 5

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_failover_initial_writer_down_waits_for_new_writer(utility, clock):
    target, _ = utility
    checked_at = []

    def _new_writer_after_lag(initial_writer_id, cluster_id, timeout_sec):
        checked_at.append(clock[0])
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_lag
    target._open_connection.side_effect = ConnectionRefusedError("down")

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1
    assert checked_at[1] - checked_at[0] < WRITER_CHANGE_TIMEOUT_SEC + INITIAL_WRITER_POLL_INTERVAL_SEC


def test_failover_off_aurora_writer_unchanged_reissued(utility, clock):
    target, env = utility
    env.get_deployment.return_value = DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER

    def _new_writer_after_reissue(initial_writer_id, cluster_id, timeout_sec):
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_reissue

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 2
    target._open_connection.assert_not_called()


def test_failover_cluster_passes_timeout_to_status_wait(utility, mocker):
    target, _ = utility
    target._client.failover_db_cluster.return_value = {"DBCluster": {}, "ResponseMetadata": {"HTTPStatusCode": 200}}

    rds_test_utility.RdsTestUtility.failover_cluster(target, "cluster", None, 100)

    target.wait_until_cluster_has_desired_status.assert_called_once_with("cluster", "available", 100)


def test_wait_until_cluster_has_desired_status_stops_at_timeout(utility, clock, mocker):
    target, _ = utility

    class _ClockDatetime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.datetime(2026, 1, 1) + datetime.timedelta(seconds=clock[0])

    mocker.patch.object(rds_test_utility, "datetime", _ClockDatetime)
    mocker.patch.object(target, "get_db_cluster", return_value={"Status": "failing-over"})

    with pytest.raises(TimeoutError):
        rds_test_utility.RdsTestUtility.wait_until_cluster_has_desired_status(target, "cluster", "available", 100)

    assert clock[0] - _START_SEC <= 110


def test_failover_late_first_request_not_reissued(utility, clock):
    target, _ = utility

    def _new_writer_after_lag(initial_writer_id, cluster_id, timeout_sec):
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    target.writer_changed.side_effect = _new_writer_after_lag
    target.query_host_role.side_effect = lambda conn, engine: (
        HostRole.READER if target.wait_until_cluster_has_desired_status.called else HostRole.WRITER)

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1


def test_failover_initial_writer_restart_rechecks_new_writer(utility, clock, mocker):
    target, _ = utility

    def _new_writer_after_lag(initial_writer_id, cluster_id, timeout_sec):
        if target.writer_changed.call_count == 1:
            clock[0] += timeout_sec
            return False
        return True

    def _writer_then_restarting(instance_info):
        if target._open_connection.call_count == 1:
            return mocker.MagicMock()
        raise ConnectionRefusedError("restarting")

    target.writer_changed.side_effect = _new_writer_after_lag
    target._open_connection.side_effect = _writer_then_restarting
    target.query_host_role.return_value = HostRole.WRITER

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.writer_changed.call_count == 2
    assert target.failover_cluster.call_count == 1


def test_get_recent_events_merges_sources_oldest_first(utility):
    target, _ = utility
    paginator = target._client.get_paginator.return_value
    paginator.paginate.side_effect = [
        [{"Events": [{"Date": datetime.datetime(2026, 1, 1, 0, 2), "SourceIdentifier": "cluster", "Message": "second"}]}],
        [{"Events": [{"Date": datetime.datetime(2026, 1, 1, 0, 1), "SourceIdentifier": "instance-1", "Message": "first"}]}],
        [{"Events": []}],
    ]

    events = target.get_recent_events("cluster", ["instance-1", "instance-2"], 900, 20)

    assert [event["Message"] for event in events] == ["first", "second"]
    target._client.get_paginator.assert_called_once_with("describe_events")
    requests = [call.kwargs for call in paginator.paginate.call_args_list]
    assert [request["SourceType"] for request in requests] == ["db-cluster", "db-instance", "db-instance"]
    assert [request["SourceIdentifier"] for request in requests] == ["cluster", "instance-1", "instance-2"]
    expected_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=900)
    assert abs((requests[0]["StartTime"] - expected_start).total_seconds()) < 60


def _raised_from(error, cause):
    error.__cause__ = cause
    return error


def _raised_while_handling(error, context):
    error.__context__ = context
    return error


_MYSQL_CONNECT_TIMEOUT = "2003: Can't connect to MySQL server on 'instance-1:3306' (timed out)"
_PG_CONNECT_TIMEOUT = "connection timeout expired"


@pytest.mark.parametrize("error, after_failover, expected", [
    pytest.param(MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), False, True, id="mysql_connect_timeout"),
    pytest.param(psycopg.OperationalError(_PG_CONNECT_TIMEOUT), False, True, id="pg_connect_timeout"),
    pytest.param(
        pytest.fail.Exception("Failover from instance-1 did not finish within 420 s. Last connect error: None"),
        True, True, id="helper_deadline"),
    pytest.param(
        _raised_from(MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), socket.timeout("timed out")),
        False, True, id="connect_timeout_from_socket_timeout"),
    pytest.param(
        pytest.fail.Exception("Instance instance-2 did not accept a connection: OperationalError: timeout"),
        True, True, id="instance_down"),
    pytest.param(
        pytest.fail.Exception("Instances did not become available: instance-2 is 'rebooting'"),
        True, True, id="instance_not_available"),
    pytest.param(
        pytest.fail.Exception("Instances not in cluster 'cluster': instance-3"),
        False, False, id="instance_not_in_cluster"),
    pytest.param(
        pytest.fail.Exception(
            "The test setup step timed out while waiting for the test custom endpoint to become available: 'ep'."),
        False, False, id="custom_endpoint_not_available"),
    pytest.param(
        pytest.fail.Exception("Could not read the RDS status of the instances: EndpointConnectionError: timeout"),
        False, False, id="instance_status_unreadable"),
    pytest.param(None, False, False, id="no_error"),
    pytest.param(MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), True, False, id="connect_timeout_after_failover"),
    pytest.param(
        pytest.fail.Exception("Instance instance-2 did not accept a connection: OperationalError: too many connections"),
        True, False, id="instance_full"),
    pytest.param(
        InterfaceError("2003: Can't connect to MySQL server on 'instance-1:3306' (111 Connection refused)"),
        False, False, id="mysql_refused"),
    pytest.param(
        OperationalError("2013: Lost connection to MySQL server during query (timed out)"), False, False, id="mysql_lost"),
    pytest.param(
        psycopg.OperationalError('connection failed: connection to server at "1.2.3.4", port 5432 failed: Connection refused'),
        False, False, id="pg_refused"),
    pytest.param(psycopg.OperationalError("FATAL:  the database system is starting up"), False, False, id="pg_starting_up"),
    pytest.param(
        _raised_from(FailoverFailedError("writer failover failed"), MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT)),
        False, False, id="failover_failed"),
    pytest.param(
        _raised_from(AwsConnectError("connect failed"), psycopg.OperationalError(_PG_CONNECT_TIMEOUT)),
        False, False, id="wrapper_connect_error"),
    pytest.param(
        _raised_from(ReadWriteSplittingError("switch failed"), MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT)),
        False, False, id="read_write_splitting_error"),
    pytest.param(
        _raised_while_handling(
            ReferenceError("weakly-referenced object no longer exists"),
            _raised_from(FailoverFailedError("writer failover failed"), MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT))),
        False, False, id="with_exit_hides_failover_failed"),
    pytest.param(
        _raised_while_handling(KeyError("writer"), MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT)),
        False, False, id="key_error_in_except"),
    pytest.param(
        _raised_while_handling(
            RuntimeError("dictionary changed size during iteration"), psycopg.OperationalError(_PG_CONNECT_TIMEOUT)),
        False, False, id="runtime_error_in_except"),
    pytest.param(
        _raised_while_handling(AssertionError("wrong writer"), psycopg.OperationalError(_PG_CONNECT_TIMEOUT)),
        False, False, id="assert_in_except"),
    pytest.param(AssertionError("assert 'instance-1' != 'instance-1'"), False, False, id="wrong_writer"),
    pytest.param(
        AssertionError("Instance instance-2 did not accept a connection"), False, False, id="helper_text_in_assert"),
    pytest.param(
        _raised_while_handling(MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), FailoverFailedError("failover failed")),
        False, False, id="connect_timeout_while_failover_failed"),
    pytest.param(
        _raised_from(psycopg.OperationalError(_PG_CONNECT_TIMEOUT), AwsConnectError("connect failed")),
        False, False, id="connect_timeout_from_wrapper_error"),
    pytest.param(
        _raised_while_handling(MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), AssertionError("wrong writer")),
        False, False, id="cleanup_timeout_after_assert"),
    pytest.param(
        _raised_while_handling(psycopg.OperationalError(_PG_CONNECT_TIMEOUT), psycopg.ProgrammingError("bad query")),
        False, False, id="cleanup_timeout_after_driver_error"),
    pytest.param(AwsConnectError(_MYSQL_CONNECT_TIMEOUT), False, False, id="wrapper_error_with_timeout_text"),
    pytest.param(FailoverFailedError(_PG_CONNECT_TIMEOUT), False, False, id="failover_error_with_timeout_text"),
    pytest.param(
        pytest.fail.Exception("Failover from instance-1 did not finish within 420 s. "
                              "Last connect error: OperationalError: FATAL: sorry, too many clients already"),
        True, False, id="helper_deadline_server_full"),
    pytest.param(
        pytest.fail.Exception("Instance instance-2 did not accept a connection: OperationalError: FATAL: "
                              "remaining connection slots are reserved for roles with privileges of the rds_reserved role"),
        True, False, id="instance_down_slots_reserved"),
    pytest.param(
        _raised_from(psycopg.OperationalError(_PG_CONNECT_TIMEOUT),
                     psycopg.OperationalError("FATAL: remaining connection slots are reserved for non-replication "
                                              "superuser connections")),
        False, False, id="connect_timeout_from_slots_reserved"),
    pytest.param(
        _raised_while_handling(psycopg.OperationalError(_PG_CONNECT_TIMEOUT), psycopg.errors.QueryCanceled("canceled")),
        False, False, id="cleanup_timeout_after_query_canceled"),
    pytest.param(
        _raised_while_handling(
            MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT), OperationalError("MySQL Connection not available")),
        False, False, id="cleanup_timeout_after_connection_not_available"),
    pytest.param(
        _raised_while_handling(
            MySQLConnectionTimeoutError(_MYSQL_CONNECT_TIMEOUT),
            _raised_while_handling(socket.timeout("timed out"), AssertionError("wrong writer"))),
        False, False, id="assert_two_links_down"),
])
def test_is_cluster_outage(error, after_failover, expected):
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(error, after_failover) is expected


def test_get_recent_events_stops_at_budget(utility, clock):
    target, _ = utility
    paginator = target._client.get_paginator.return_value

    def _slow_pages(**kwargs):
        clock[0] += 15
        return [{"Events": []}]

    paginator.paginate.side_effect = _slow_pages

    target.get_recent_events("cluster", ["instance-1", "instance-2"], 900, 20)

    assert paginator.paginate.call_count == 2


def test_failover_cluster_marks_failover_requested(utility, mocker):
    target, _ = utility
    target._client.failover_db_cluster.return_value = {"DBCluster": {}, "ResponseMetadata": {"HTTPStatusCode": 200}}
    mocker.patch.object(rds_test_utility.RdsTestUtility, "failover_requested", False)

    rds_test_utility.RdsTestUtility.failover_cluster(target, "cluster", None, 100)

    assert rds_test_utility.RdsTestUtility.failover_requested is True


def test_make_sure_instances_up_after_one_refusal(utility, mocker):
    target, _ = utility
    target._open_connection.side_effect = [ConnectionRefusedError("starting up"), mocker.MagicMock()]

    rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"])

    assert target._open_connection.call_count == 2


def test_make_sure_instances_up_waits_until_available(utility, clock):
    target, env = utility
    env.get_cluster_name.return_value = "cluster"
    target._client.describe_db_instances.side_effect = [
        _described_instances("rebooting"), _described_instances("available")]

    rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-1", "instance-2"])

    target._client.describe_db_instances.assert_called_with(Filters=[{"Name": "db-cluster-id", "Values": ["cluster"]}])
    assert target._client.describe_db_instances.call_count == 2
    assert clock[0] == _START_SEC + INSTANCE_STATUS_POLL_INTERVAL_SEC
    assert target._open_connection.call_count == 2


def test_make_sure_instances_up_not_available_fails_at_timeout(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.return_value = _described_instances("rebooting")

    with pytest.raises(pytest.fail.Exception, match="instance-2 is 'rebooting'") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-1", "instance-2"])

    assert clock[0] == _START_SEC + INSTANCE_UP_TIMEOUT_SEC
    target._open_connection.assert_not_called()
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is True


def test_make_sure_instances_up_later_deadline_fails_at_timeout(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.return_value = _described_instances("modifying")

    with pytest.raises(pytest.fail.Exception, match="instance-2 is 'modifying'"):
        rds_test_utility.RdsTestUtility.make_sure_instances_up(
            target, ["instance-2"], clock[0] + INSTANCE_UP_TIMEOUT_SEC + 100)

    assert clock[0] == _START_SEC + INSTANCE_UP_TIMEOUT_SEC


def test_make_sure_instances_up_status_and_connect_share_timeout(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.side_effect = lambda **kwargs: _described_instances(
        "rebooting" if clock[0] < _START_SEC + 100 else "available")
    target._open_connection.side_effect = ConnectionRefusedError("starting up")

    with pytest.raises(pytest.fail.Exception, match="instance-2 did not accept a connection"):
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"])

    assert clock[0] == _START_SEC + INSTANCE_UP_TIMEOUT_SEC


def test_make_sure_instances_up_ignores_other_instances(utility):
    target, _ = utility
    target._client.describe_db_instances.return_value = _described_instances("rebooting")

    rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-1"])

    assert target._client.describe_db_instances.call_count == 1
    assert target._open_connection.call_count == 1


def test_make_sure_instances_up_checks_status_once_after_deadline(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.return_value = _described_instances("rebooting")

    with pytest.raises(pytest.fail.Exception, match="instance-2 is 'rebooting'"):
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] - 1)

    assert target._client.describe_db_instances.call_count == 1


def test_make_sure_instances_up_missing_instance_fails_at_once(utility, clock):
    target, _ = utility

    with pytest.raises(pytest.fail.Exception, match="not in cluster .*: instance-3$") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-3"])

    assert clock[0] == _START_SEC
    target._open_connection.assert_not_called()
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is False


def test_make_sure_instances_up_missing_and_not_available_fails_at_once(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.return_value = _described_instances("rebooting")

    with pytest.raises(pytest.fail.Exception, match="not in cluster .*: instance-3$") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2", "instance-3"])

    assert clock[0] == _START_SEC
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is False


def test_make_sure_instances_up_status_read_fails_at_deadline(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.side_effect = EndpointConnectionError(
        endpoint_url="https://rds.us-east-2.amazonaws.com")

    with pytest.raises(pytest.fail.Exception, match="Could not read the RDS status") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] + 10)

    assert clock[0] == _START_SEC + 10
    target._open_connection.assert_not_called()
    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is False


def test_make_sure_instances_up_last_read_not_available_reruns(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.side_effect = [
        EndpointConnectionError(endpoint_url="https://rds.us-east-2.amazonaws.com")] + [
        _described_instances("rebooting")] * 10

    with pytest.raises(pytest.fail.Exception, match="did not become available") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] + 10)

    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is True


def test_make_sure_instances_up_last_read_failed_not_rerun(utility, clock):
    target, _ = utility
    target._client.describe_db_instances.side_effect = [_described_instances("rebooting")] * 5 + [
        EndpointConnectionError(endpoint_url="https://rds.us-east-2.amazonaws.com")] * 5

    with pytest.raises(pytest.fail.Exception, match="Could not read the RDS status") as excinfo:
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] + 10)

    assert rds_test_utility.RdsTestUtility.is_cluster_outage(excinfo.value, after_failover=True) is False


def test_make_sure_instances_up_retries_status_after_api_error(utility):
    target, _ = utility
    throttled = ClientError({"Error": {"Code": "Throttling", "Message": "Rate exceeded"}}, "DescribeDBInstances")
    target._client.describe_db_instances.side_effect = [throttled, _described_instances("available")]

    rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"])

    assert target._client.describe_db_instances.call_count == 2
    assert target._open_connection.call_count == 1
