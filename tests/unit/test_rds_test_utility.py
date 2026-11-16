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

import tests.integration.container.utils.rds_test_utility as rds_test_utility
from aws_advanced_python_wrapper.hostinfo import HostRole
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment


@pytest.fixture
def utility(mocker):
    now = [1000.0]

    def _sleep(seconds):
        now[0] += seconds

    mocker.patch.object(rds_test_utility, "sleep", side_effect=_sleep)
    mocker.patch.object(rds_test_utility.timeit, "default_timer", side_effect=lambda: now[0])
    mocker.patch.object(rds_test_utility.boto3, "client")
    env = mocker.patch.object(rds_test_utility, "TestEnvironment").get_current.return_value
    env.get_deployment.return_value = DatabaseEngineDeployment.AURORA
    env.get_engine.return_value = DatabaseEngine.PG
    mocker.patch.object(rds_test_utility.socket, "gethostbyname", side_effect=["10.0.0.1", "10.0.0.2"])

    target = rds_test_utility.RdsTestUtility("us-east-2")
    mocker.patch.object(target, "writer_changed", return_value=True)
    mocker.patch.object(target, "make_sure_instances_up")
    mocker.patch.object(target, "failover_cluster")
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


def test_failover_is_reissued_when_initial_writer_stays_writer(utility):
    target, _ = utility
    target.query_host_role.side_effect = lambda conn, engine: (
        HostRole.WRITER if target.failover_cluster.call_count == 1 else HostRole.READER)

    target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")

    assert target.failover_cluster.call_count == 2


def test_wait_until_initial_writer_is_reader_skips_non_aurora(utility):
    target, env = utility
    env.get_deployment.return_value = DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER

    assert target._wait_until_initial_writer_is_reader("instance-1") is True
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


def test_failover_fails_when_initial_writer_never_answers(utility):
    target, _ = utility
    target._open_connection.side_effect = ConnectionRefusedError("starting up")

    with pytest.raises(pytest.fail.Exception, match="instance-1"):
        target.failover_cluster_and_wait_until_writer_changed("instance-1", "cluster")
