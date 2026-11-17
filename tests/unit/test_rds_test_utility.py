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

import pytest

import tests.integration.container.utils.rds_test_utility as rds_test_utility
from aws_advanced_python_wrapper.hostinfo import HostRole
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.test_timings import (
    FAILOVER_DEADLINE_SEC, FAILOVER_MAX_REISSUES,
    INITIAL_WRITER_POLL_INTERVAL_SEC, INITIAL_WRITER_REVERT_WINDOW_SEC,
    WRITER_CHANGE_TIMEOUT_SEC)

_START_SEC = 1000.0


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

    with pytest.raises(pytest.fail.Exception, match="instance-1"):
        target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 1
    assert clock[0] - _START_SEC <= FAILOVER_DEADLINE_SEC


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

    with pytest.raises(AssertionError):
        rds_test_utility.RdsTestUtility.make_sure_instances_up(target, ["instance-2"], clock[0] - 1)

    assert target._open_connection.call_count == 1


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
