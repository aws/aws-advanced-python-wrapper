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

from contextlib import closing
from threading import Thread
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, cast

import botocore.exceptions

from tests.integration.container.utils.proxy_helper import ProxyHelper

if TYPE_CHECKING:
    from botocore.config import Config

    from aws_advanced_python_wrapper.pep249 import Connection
    from .test_database_info import TestDatabaseInfo

import asyncio
import re
import socket
import timeit
from datetime import datetime, timedelta, timezone
from time import perf_counter_ns, sleep

import boto3
import psycopg
import psycopg.errors
import pytest
from mysql.connector.errors import \
    ConnectionTimeoutError as MySQLConnectionTimeoutError

from aws_advanced_python_wrapper.driver_info import DriverInfo
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from tests.integration.container.utils.test_timings import (
    CLUSTER_STATUS_TIMEOUT_SEC, FAILOVER_DEADLINE_SEC, FAILOVER_MAX_REISSUES,
    FAILOVER_REISSUE_MIN_REMAINING_SEC, INITIAL_WRITER_POLL_INTERVAL_SEC,
    INITIAL_WRITER_REVERT_WINDOW_SEC, INSTANCE_STATUS_POLL_INTERVAL_SEC,
    INSTANCE_UP_TIMEOUT_SEC, WRITER_CHANGE_TIMEOUT_SEC,
    WRITER_CHANGED_PROBE_CONNECT_TIMEOUT_SEC,
    WRITER_CHANGED_PROBE_POLL_INTERVAL_SEC)
from .database_engine import DatabaseEngine
from .database_engine_deployment import DatabaseEngineDeployment
from .driver_helper import DriverHelper
from .test_driver import TestDriver
from .test_environment import TestEnvironment
from .test_instance_info import TestInstanceInfo

# An instance, the cluster or the network is down: the failover helper ran out of time, an instance did
# not reach the RDS status available or accept a connection, or a connect timed out. The conftest
# reruns a test that fails this way once.
_HELPER_OUTAGE_PATTERNS = (
    re.compile(r"did not finish within"),
    re.compile(r"did not accept a connection"),
    re.compile(r"did not become available"),
)
_CONNECT_TIMEOUT_PATTERNS = (
    re.compile(r"Can't connect to MySQL server.*timed out"),
    re.compile(r"connection timeout expired"),
)
# A full server can come from leaked connections, so it is not an outage
_NOT_CLUSTER_OUTAGE_PATTERN = re.compile(
    r"[Tt]oo many (connections|clients)|remaining connection slots are reserved")
# The driver errors a connect timeout surfaces as
_CONNECT_TIMEOUT_ERRORS = (MySQLConnectionTimeoutError, psycopg.OperationalError)
# What a connect timeout is raised from. Anything else in the chain, such as an assertion whose cleanup then
# failed to connect, or a query the test ran, is the test's own failure.
_CONNECT_TIMEOUT_CAUSES = (OSError, asyncio.TimeoutError, MySQLConnectionTimeoutError, psycopg.errors.ConnectionTimeout)


class RdsTestUtility:
    logger = Logger(__name__)
    # Set when a test requests a failover, so that a connect timeout after it is not rerun as an outage
    failover_requested: ClassVar[bool] = False

    _client: Any
    _last_connect_error: Optional[str] = None

    def __init__(self, region: str, endpoint: Optional[str] = None, config: Optional[Config] = None):
        if endpoint:
            self._client = boto3.client(service_name='rds', region_name=region, endpoint_url=endpoint, config=config)
        else:
            self._client = boto3.client(service_name='rds', region_name=region, config=config)

    @staticmethod
    def get_utility() -> RdsTestUtility:
        test_info = TestEnvironment.get_current().get_info()
        return RdsTestUtility(test_info.get_region(), test_info.get_rds_endpoint())

    def get_db_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        filters = [{'Name': "db-instance-id", 'Values': [f"{instance_id}"]}]
        response = self._client.describe_db_instances(DBInstanceIdentifier=instance_id,
                                                      Filters=filters)
        instances = response.get("DBInstances")
        if instances is None or len(instances) == 0:
            return None
        return instances[0]

    def get_rds_client(self):
        test_info = TestEnvironment.get_current().get_info()
        endpoint = test_info.get_rds_endpoint()
        if endpoint:
            return boto3.client(service_name='rds', region_name=test_info.get_region(), endpoint_url=endpoint)
        else:
            return boto3.client(service_name='rds', region_name=test_info.get_region())

    def does_db_instance_exist(self, instance_id: str) -> bool:
        try:
            instance = self.get_db_instance(instance_id)
            return instance is not None
        except self._client.exceptions.DBInstanceNotFoundFault:
            return False

    def create_db_instance(self, instance_id: str) -> TestInstanceInfo:
        environment = TestEnvironment.get_current()
        deployment = environment.get_deployment()
        if DatabaseEngineDeployment.AURORA != deployment:
            raise Exception(Messages.get_formatted(
                "RdsTestUtility.MethodNotSupportedForDeployment", "create_db_instance", deployment))

        if self.does_db_instance_exist(instance_id):
            self.delete_db_instance(instance_id)

        self._client.create_db_instance(
            DBClusterIdentifier=environment.get_info().get_db_name(),
            DBInstanceIdentifier=instance_id,
            DBInstanceClass="db.r5.large",
            Engine=self.get_aurora_engine_name(environment.get_engine()),
            PubliclyAccessible=True)

        instance = self.wait_until_instance_has_desired_status(instance_id, 15, "available")
        if instance is None:
            raise Exception(Messages.get_formatted("RdsTestUtility.CreateDBInstanceFailed", instance_id))

        return TestInstanceInfo(instance)

    def delete_db_instance(self, instance_id: str):
        self._client.delete_db_instance(DBInstanceIdentifier=instance_id)
        self.wait_until_instance_has_desired_status(instance_id, 15, "deleted")

    def wait_until_instance_has_desired_status(
            self, instance_id: str, wait_time_mins: float, desired_status: str) -> Optional[Dict[str, Any]]:
        stop_time = datetime.now() + timedelta(minutes=wait_time_mins)
        while datetime.now() <= stop_time:
            try:
                instance = self.get_db_instance(instance_id)
                if instance is not None and instance.get("DBInstanceStatus") == desired_status:
                    return instance
            except self._client.exceptions.DBInstanceNotFoundFault:
                if desired_status == "deleted":
                    return None
            except Exception:
                pass
            sleep(1)

        raise InterruptedError(Messages.get_formatted(
            "RdsTestUtility.InstanceDescriptionTimeout", instance_id, desired_status, wait_time_mins))

    def wait_until_cluster_has_desired_status(
            self, cluster_id: str, desired_status: str, timeout_sec: float = CLUSTER_STATUS_TIMEOUT_SEC) -> None:
        stop_time = datetime.now() + timedelta(seconds=timeout_sec)
        cluster_info = self.get_db_cluster(cluster_id)
        status = cluster_info.get("Status")
        while status != desired_status:
            if datetime.now() > stop_time:
                raise TimeoutError(f"Cluster {cluster_id} did not reach status '{desired_status}' within {timeout_sec:.0f} s.")
            sleep(10)
            cluster_info = self.get_db_cluster(cluster_id)
            status = cluster_info.get("Status")

    def get_recent_events(
            self, cluster_id: str, instance_ids: List[str], lookback_sec: float, budget_sec: float) -> List[Dict[str, Any]]:
        """Read the RDS events of a cluster and its instances.

        :param cluster_id: the cluster's identifier.
        :param instance_ids: the identifiers of the cluster's instances.
        :param lookback_sec: how far back to read, in seconds.
        :param budget_sec: how long to keep reading, in seconds. Sources not read by then are skipped.
        :return: the events read, oldest first.
        """
        stop_time = timeit.default_timer() + budget_sec
        start_time = datetime.now(timezone.utc) - timedelta(seconds=lookback_sec)
        sources = [("db-cluster", cluster_id)] + [("db-instance", instance_id) for instance_id in instance_ids]
        paginator = self._client.get_paginator("describe_events")
        events: List[Dict[str, Any]] = []
        for source_type, source_id in sources:
            if timeit.default_timer() >= stop_time:
                break
            for page in paginator.paginate(SourceType=source_type, SourceIdentifier=source_id, StartTime=start_time):
                events.extend(page.get("Events", []))
        return sorted(events, key=lambda event: event.get("Date", datetime.min.replace(tzinfo=timezone.utc)))

    @staticmethod
    def is_cluster_outage(error: Optional[BaseException], after_failover: bool = False) -> bool:
        """Tell whether a test failed because the cluster stopped accepting connections.

        :param error: the exception the test failed with, or None.
        :param after_failover: whether the test requested a failover before it failed. A connect
            timeout is then not an outage, because the wrapper may have connected to the old writer.
        :return: True if both of these hold. First, the exception itself is one of the helper's
            outage failures (the failover did not finish in time, or an instance did not reach the
            RDS status available or accept a connection) or, before any failover, a connect that
            timed out. Second, every exception it was raised from or while handling is part of a
            connect timeout.
        """
        if error is None:
            return False

        message = f"{type(error).__name__}: {error}"
        if _NOT_CLUSTER_OUTAGE_PATTERN.search(message):
            return False
        if isinstance(error, pytest.fail.Exception):
            outage = any(pattern.search(message) for pattern in _HELPER_OUTAGE_PATTERNS)
        elif after_failover or not isinstance(error, _CONNECT_TIMEOUT_ERRORS):
            outage = False
        else:
            outage = any(pattern.search(message) for pattern in _CONNECT_TIMEOUT_PATTERNS)
        if not outage:
            return False

        chain: List[BaseException] = []
        pending: List[BaseException] = [error]
        while pending:
            current = pending.pop()
            if any(current is seen for seen in chain):
                continue
            chain.append(current)
            pending.extend(linked for linked in (current.__cause__, current.__context__) if linked is not None)
        # Every exception the outermost one was raised from or while handling must be part of a connect timeout
        return all(isinstance(exception, _CONNECT_TIMEOUT_CAUSES) for exception in chain[1:])

    def get_db_cluster(self, cluster_id: str) -> Any:
        response: Any = self._client.describe_db_clusters(DBClusterIdentifier=cluster_id)
        clusters = response.get("DBClusters")
        if clusters is None or len(clusters) == 0:
            return None
        return clusters[0]

    def failover_cluster_and_wait_until_writer_changed(
            self,
            initial_writer_id: Optional[str] = None,
            cluster_id: Optional[str] = None,
            target_id: Optional[str] = None) -> None:
        deployment = TestEnvironment.get_current().get_deployment()
        if DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER == deployment and target_id is not None:
            raise Exception(Messages.get_formatted("RdsTestUtility.FailoverToTargetNotSupported", target_id, deployment))

        start = perf_counter_ns()
        deadline = timeit.default_timer() + FAILOVER_DEADLINE_SEC
        self._last_connect_error = None
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_db_name()

        if initial_writer_id is None:
            initial_writer_id = self.get_cluster_writer_instance_id(cluster_id)

        database_info = TestEnvironment.get_current().get_database_info()
        cluster_endpoint = database_info.get_cluster_endpoint()
        initial_cluster_address = socket.gethostbyname(cluster_endpoint)

        self.failover_cluster(cluster_id, target_id, deadline - timeit.default_timer())
        remaining_reissues = FAILOVER_MAX_REISSUES
        while True:
            remaining_sec = deadline - timeit.default_timer()
            if remaining_sec <= 0:
                # The conftest reruns a test that fails with this message once
                pytest.fail(f"Failover from {initial_writer_id} did not finish within {FAILOVER_DEADLINE_SEC} s. "
                            f"Last connect error: {self._last_connect_error}")

            if self.writer_changed(initial_writer_id, cluster_id, min(WRITER_CHANGE_TIMEOUT_SEC, remaining_sec)):
                if not self._initial_writer_kept_writer_role(initial_writer_id, deadline):
                    if timeit.default_timer() >= deadline:
                        self.logger.warning(
                            f"Initial writer {initial_writer_id} did not answer as a reader before the failover deadline")
                    break
            elif DatabaseEngineDeployment.AURORA == deployment:
                # No new writer yet. If the initial writer doesn't answer, or stops answering as the writer,
                # a second failover would not help, and on two instances it hands the writer role back.
                # Check for a new writer again once the initial writer's window can't complete.
                if self._query_instance_role(initial_writer_id) != HostRole.WRITER:
                    continue
                window_deadline = min(deadline, timeit.default_timer() + 2 * INITIAL_WRITER_REVERT_WINDOW_SEC)
                if not self._initial_writer_kept_writer_role(initial_writer_id, window_deadline):
                    continue

            # The failover did not take, or the initial writer is the writer again: request it again
            if remaining_reissues == 0 or deadline - timeit.default_timer() < FAILOVER_REISSUE_MIN_REMAINING_SEC:
                raise Exception(Messages.get("RdsTestUtility.FailoverRequestNotSuccessful"))
            self.wait_until_cluster_has_desired_status(cluster_id, "available", deadline - timeit.default_timer())
            if (DatabaseEngineDeployment.AURORA == deployment
                    and self._query_instance_role(initial_writer_id) != HostRole.WRITER):
                # A failover that was still running has finished while the cluster was busy
                continue
            remaining_reissues -= 1
            self.failover_cluster(cluster_id, target_id, deadline - timeit.default_timer())

        # Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the new writer instance.
        cluster_address = socket.gethostbyname(cluster_endpoint)
        while cluster_address == initial_cluster_address and timeit.default_timer() < deadline:
            sleep(1)
            cluster_address = socket.gethostbyname(cluster_endpoint)

        # wait until all instances except initial writer instance to be available
        instances = [x.get_instance_id() for x in TestEnvironment.get_current().get_instances() if x.get_instance_id() != initial_writer_id]
        self.make_sure_instances_up(instances, deadline)
        self.logger.debug("Testing.FinishedFailover", initial_writer_id, str((perf_counter_ns() - start) / 1_000_000))

    def failover_cluster(
            self,
            cluster_id: Optional[str] = None,
            target_id: Optional[str] = None,
            timeout_sec: float = CLUSTER_STATUS_TIMEOUT_SEC) -> None:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_db_name()

        RdsTestUtility.failover_requested = True
        self.wait_until_cluster_has_desired_status(cluster_id, "available", timeout_sec)

        remaining_attempts = 10
        while remaining_attempts > 0:
            remaining_attempts -= 1
            try:
                if not target_id:
                    result = self._client.failover_db_cluster(DBClusterIdentifier=cluster_id)
                else:
                    result = self._client.failover_db_cluster(
                        DBClusterIdentifier=cluster_id, TargetDBInstanceIdentifier=target_id)
                http_status_code = result.get("ResponseMetadata").get("HTTPStatusCode")
                if result.get("DBCluster") is not None and http_status_code == 200:
                    return
                sleep(1)
            except Exception:
                sleep(1)

        raise Exception(Messages.get_formatted("RdsTestUtility.FailoverClusterFailed", cluster_id))

    def writer_changed(self, initial_writer_id: str, cluster_id: str, timeout_sec: float) -> bool:
        """Detect whether Aurora has promoted a new writer instance.

        For Aurora deployments, queries the data-plane SQL catalog through the
        cluster endpoint (which always routes to the current writer). The data
        plane converges within seconds of a failover event, whereas the RDS API
        ``DescribeDBClusters.IsClusterWriter`` field can lag for minutes -- long
        enough to bust pytest-timeout budgets on multi-instance failover tests.
        Falls back to the boto3-based polling for non-Aurora deployments where
        ``aurora_db_instance_identifier()`` / ``@@aurora_server_id`` aren't
        defined.
        """
        deployment = TestEnvironment.get_current().get_deployment()
        engine = TestEnvironment.get_current().get_engine()
        if (DatabaseEngineDeployment.AURORA != deployment
                or engine not in (DatabaseEngine.PG, DatabaseEngine.MYSQL)):
            return self._writer_changed_via_rds_api(initial_writer_id, cluster_id, timeout_sec)

        info = TestEnvironment.get_current().get_database_info()
        cluster_endpoint = info.get_cluster_endpoint()
        port = TestEnvironment.get_current().get_writer().get_port()
        connect_params = DriverHelper.get_connect_params(
            cluster_endpoint, port, info.get_username(), info.get_password(),
            info.get_default_db_name(),
            test_driver=TestDriver.PG if engine == DatabaseEngine.PG else TestDriver.MYSQL)
        connect_params["connect_timeout" if engine == DatabaseEngine.PG else "connection_timeout"] = \
            WRITER_CHANGED_PROBE_CONNECT_TIMEOUT_SEC
        connect_func = DriverHelper.get_connect_func(
            TestDriver.PG if engine == DatabaseEngine.PG else TestDriver.MYSQL)
        instance_id_query = self.get_instance_id_query(engine)

        # The data plane converges within seconds, but downstream test
        # assertions can call ``get_cluster_writer_instance_id`` (RDS
        # DescribeDBClusters) and ``is_db_instance_writer``, which read the
        # control plane and lag the data plane by tens of seconds on
        # multi-instance Aurora topologies. Returning as soon as the data
        # plane flips lets those assertions race the control plane. Wait
        # for control-plane convergence too -- within the same ``timeout_sec``
        # budget -- so callers see a consistent view across both planes.
        data_plane_changed = False
        wait_until = timeit.default_timer() + timeout_sec
        while timeit.default_timer() < wait_until:
            try:
                conn = connect_func(**connect_params)
                try:
                    with closing(conn.cursor()) as cursor:
                        cursor.execute(instance_id_query)
                        row = cursor.fetchone()
                        if row is not None and row[0] != initial_writer_id:
                            data_plane_changed = True
                finally:
                    conn.close()
            except Exception as ex:
                # Aurora may briefly reject connections mid-failover; keep polling.
                self._last_connect_error = f"{type(ex).__name__}: {ex}"
                self.logger.debug("writer_changed SQL probe failed: " + str(ex))
            if data_plane_changed:
                try:
                    cp_writer = self.get_cluster_writer_instance_id(cluster_id)
                    if cp_writer is not None and cp_writer != initial_writer_id:
                        return True
                except Exception as ex:
                    self.logger.debug(
                        "writer_changed control-plane probe failed: " + str(ex))
            sleep(WRITER_CHANGED_PROBE_POLL_INTERVAL_SEC)
        return False

    def _writer_changed_via_rds_api(self, initial_writer_id: str, cluster_id: str, timeout_sec: float) -> bool:
        wait_until = timeit.default_timer() + timeout_sec

        current_writer_id = self.get_cluster_writer_instance_id(cluster_id)
        while initial_writer_id == current_writer_id and timeit.default_timer() < wait_until:
            sleep(3)
            current_writer_id = self.get_cluster_writer_instance_id(cluster_id)
        return initial_writer_id != current_writer_id

    def _initial_writer_kept_writer_role(self, initial_writer_id: str, deadline: float) -> bool:
        """Tell whether the initial writer is still, or again, the writer after the failover.

        Polls the initial writer until it answers as a reader, or keeps answering as the writer
        for ``INITIAL_WRITER_REVERT_WINDOW_SEC`` without a break.

        :param initial_writer_id: the instance that was the writer before the failover.
        :param deadline: the ``timeit.default_timer()`` value at which to stop waiting.
        :return: True if it kept answering as the writer for the whole window.
            False if it answered as a reader, off Aurora, or at the deadline.
        """
        if DatabaseEngineDeployment.AURORA != TestEnvironment.get_current().get_deployment():
            return False

        first_writer_reading: Optional[float] = None
        while timeit.default_timer() < deadline:
            role = self._query_instance_role(initial_writer_id)
            if role == HostRole.READER:
                return False
            if role is None:
                # It restarts: a writer reading after the restart starts a new window
                first_writer_reading = None
            elif role == HostRole.WRITER:
                now = timeit.default_timer()
                if first_writer_reading is None:
                    first_writer_reading = now
                elif now - first_writer_reading >= INITIAL_WRITER_REVERT_WINDOW_SEC:
                    self.logger.debug(f"Initial writer {initial_writer_id} is still the writer")
                    return True
            sleep(INITIAL_WRITER_POLL_INTERVAL_SEC)

        return False

    def _query_instance_role(self, instance_id: str) -> Optional[HostRole]:
        """Connect to one instance and ask for its role.

        :param instance_id: the instance to ask.
        :return: its role, or None if it can't be reached, for example while it restarts.
        """
        env = TestEnvironment.get_current()
        instance_info = env.get_database_info().get_instance(instance_id)
        try:
            conn = self._open_connection(instance_info)
            try:
                return self.query_host_role(conn, env.get_engine())
            finally:
                conn.close()
        except Exception as e:
            self._last_connect_error = f"{type(e).__name__}: {e}"
            self.logger.debug(f"Instance {instance_id} did not answer: {e}")
            return None

    def assert_first_query_throws(
            self,
            conn,
            exception_cls,
            database_deployment: Optional[DatabaseEngineDeployment] = None,
            database_engine: Optional[DatabaseEngine] = None) -> None:
        if database_deployment is None:
            database_deployment = TestEnvironment.get_current().get_deployment()
        if database_engine is None:
            database_engine = TestEnvironment.get_current().get_engine()

        with pytest.raises(exception_cls):
            self.query_instance_id(conn, database_deployment, database_engine)

    def query_instance_id(
            self,
            conn,
            database_deployment: Optional[DatabaseEngineDeployment] = None,
            database_engine: Optional[DatabaseEngine] = None) -> str:
        if database_deployment is None:
            database_deployment = TestEnvironment.get_current().get_deployment()
        if database_engine is None:
            database_engine = TestEnvironment.get_current().get_engine()

        if DatabaseEngineDeployment.AURORA == database_deployment:
            return self._query_aurora_instance_id(conn, database_engine)
        elif DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER == database_deployment:
            return self._query_multi_az_instance_id(conn, database_engine)
        else:
            raise RuntimeError(Messages.get_formatted(
                "RdsTestUtility.MethodNotSupportedForDeployment", "query_instance_id", database_deployment))

    def query_host_role(
            self,
            conn,
            database_engine: DatabaseEngine) -> HostRole:
        if database_engine == DatabaseEngine.MYSQL:
            is_reader_query = "SELECT @@innodb_read_only"
        elif database_engine == DatabaseEngine.PG:
            is_reader_query = "SELECT pg_catalog.pg_is_in_recovery()"

        with closing(conn.cursor()) as cursor:
            cursor.execute(is_reader_query)
            record = cursor.fetchone()
            is_reader = record[0]

            if is_reader in (1, True):
                return HostRole.READER
            else:
                return HostRole.WRITER

    @staticmethod
    def get_instance_id_query(engine: Optional[DatabaseEngine] = None) -> str:
        """Get the SQL query to retrieve the Aurora instance ID based on the database engine.

        Args:
            engine: The database engine. If None, uses the current test environment's engine.

        Returns:
            The SQL query string to get the instance ID.
        """
        if engine is None:
            engine = TestEnvironment.get_current().get_engine()

        if engine == DatabaseEngine.MYSQL:
            return "SELECT @@aurora_server_id"
        elif engine == DatabaseEngine.PG:
            return "SELECT pg_catalog.aurora_db_instance_identifier()"
        else:
            raise UnsupportedOperationError(engine.value)

    def _query_aurora_instance_id(self, conn: Connection, engine: DatabaseEngine) -> str:
        sql = self.get_instance_id_query(engine)
        with closing(conn.cursor()) as cursor:
            cursor.execute(sql)
            record = cursor.fetchone()
            return record[0]

    def _query_multi_az_instance_id(self, conn: Connection, engine: DatabaseEngine):
        # You cannot directly query the instance name with multi-az instances. You must find the endpoint whose ID
        # matches the current instance and parse the instance name from the endpoint.
        if engine == DatabaseEngine.MYSQL:
            # Gets the id, e.g. '0123456789'
            id_sql = "SELECT @@server_id"
            endpoint_sql = f"SELECT endpoint FROM mysql.rds_topology WHERE id=({id_sql})"
        elif engine == DatabaseEngine.PG:
            # Gets the id, e.g. 'db-ABC1DE2FGHI'
            id_sql = "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()"
            endpoint_sql = f"SELECT endpoint FROM rds_tools.show_topology() WHERE id=({id_sql})"
        else:
            raise UnsupportedOperationError(engine.value)

        cursor = conn.cursor()
        cursor.execute(endpoint_sql)
        endpoint = cursor.fetchone()[0]
        instance_id = endpoint[:endpoint.find(".")]
        return instance_id

    def is_db_instance_writer(self, instance_id: str, cluster_id: Optional[str] = None) -> bool:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_db_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if m.get("DBInstanceIdentifier") == instance_id:
                return cast('bool', m.get("IsClusterWriter"))
        raise Exception(Messages.get_formatted("RdsTestUtility.ClusterMemberNotFound", instance_id))

    def get_cluster_writer_instance_id(self, cluster_id: Optional[str] = None) -> str:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_db_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if cast('bool', m.get("IsClusterWriter")):
                return cast('str', m.get("DBInstanceIdentifier"))
        raise Exception(Messages.get_formatted("RdsTestUtility.WriterInstanceNotFound", cluster_id))

    def get_instance_ids(self, host: Optional[str] = None) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        deployment: DatabaseEngineDeployment = test_environment.get_deployment()
        if DatabaseEngineDeployment.AURORA == deployment:
            return self._get_aurora_instance_ids(host)
        elif DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER == deployment:
            return self._get_multi_az_instance_ids(host)
        else:
            raise RuntimeError("RdsTestUtility.MethodNotSupportedForDeployment", "get_instance_ids", deployment)

    def _get_aurora_instance_ids(self, host: Optional[str] = None) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        engine: DatabaseEngine = test_environment.get_engine()
        instance_info: TestInstanceInfo = test_environment.get_writer()

        sql = self._get_aurora_topology_sql(engine)
        with self._open_connection(instance_info, host) as conn, conn.cursor() as cursor:
            cursor.execute(sql)
            records = cursor.fetchall()

            result: List[str] = list()
            for r in records:
                result.append(r[0])

            return result

    def _get_multi_az_instance_ids(self, host: Optional[str] = None) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        engine: DatabaseEngine = test_environment.get_engine()
        cluster_endpoint_instance_info: TestInstanceInfo = TestInstanceInfo({
            "host": test_environment.get_database_info().get_cluster_endpoint(),
            "port": test_environment.get_database_info().get_cluster_endpoint_port()
        })

        self.logger.debug("Testing._get_multi_az_instance_ids_connecting", cluster_endpoint_instance_info.get_host())

        conn = self._open_connection(cluster_endpoint_instance_info, host)
        cursor = conn.cursor()
        get_writer_id_query = self._get_multi_az_writer_sql(engine)
        cursor.execute(get_writer_id_query)
        record = cursor.fetchone()
        if record is not None:
            writer_id = record[0]
        else:
            # The writer ID query returns None if we are connected to the MySQL writer.
            host_id_query = self._get_multi_az_host_id_sql(engine)
            cursor.execute(host_id_query)
            writer_id = cursor.fetchone()[0]

        topology_sql = self._get_multi_az_topology_sql(engine, writer_id)
        cursor.execute(topology_sql)
        records = cursor.fetchall()

        result: List[str] = list()
        for r in records:
            endpoint = r[1]
            instance_id = endpoint[:endpoint.find(".")]
            result.append(instance_id)
        conn.close()

        self.logger.debug("Testing._get_multi_az_instance_ids", result)

        return result

    def _open_connection(self, instance_info: TestInstanceInfo, host: Optional[str] = None) -> Any:

        env: TestEnvironment = TestEnvironment.get_current()
        database_engine: DatabaseEngine = env.get_engine()
        test_driver = self._get_driver_for_database_engine(database_engine)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)

        host = host if host is not None else instance_info.get_host()
        user = env.get_database_info().get_username()
        password = env.get_database_info().get_password()
        db = env.get_database_info().get_default_db_name()

        conn_params = DriverHelper.get_connect_params(host, instance_info.get_port(), user, password, db, test_driver)

        conn = target_driver_connect(**conn_params, connect_timeout=10)
        return conn

    def _get_aurora_topology_sql(self, engine: DatabaseEngine) -> str:
        if engine == DatabaseEngine.MYSQL:
            return ("SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status "
                    "ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)")
        elif engine == DatabaseEngine.PG:
            return ("SELECT SERVER_ID, SESSION_ID FROM pg_catalog.aurora_replica_status() "
                    "ORDER BY CASE WHEN SESSION_ID OPERATOR(pg_catalog.=) 'MASTER_SESSION_ID' THEN 0 ELSE 1 END")
        else:
            raise UnsupportedOperationError(engine.value)

    def _get_multi_az_topology_sql(self, engine: DatabaseEngine, writer_id) -> str:
        if engine == DatabaseEngine.MYSQL:
            return f"SELECT id, endpoint, port FROM mysql.rds_topology ORDER BY id = '{writer_id}' DESC"
        elif engine == DatabaseEngine.PG:
            return (f"SELECT id, endpoint, port "
                    f"FROM rds_tools.show_topology('aws_python_driver-{DriverInfo.DRIVER_VERSION}')"
                    f"ORDER BY id OPERATOR(pg_catalog.=) '{writer_id}' DESC")
        else:
            raise UnsupportedOperationError(engine.value)

    def _get_multi_az_writer_sql(self, engine: DatabaseEngine) -> str:
        if engine == DatabaseEngine.MYSQL:
            return "SHOW REPLICA STATUS"
        elif engine == DatabaseEngine.PG:
            return ("SELECT multi_az_db_cluster_source_dbi_resource_id "
                    "FROM rds_tools.multi_az_db_cluster_source_dbi_resource_id()")
        else:
            raise UnsupportedOperationError(engine.value)

    def _get_multi_az_host_id_sql(self, engine: DatabaseEngine) -> str:
        if engine == DatabaseEngine.MYSQL:
            return "SELECT @@server_id"
        elif engine == DatabaseEngine.PG:
            return "SELECT dbi_resource_id FROM rds_tools.dbi_resource_id()"
        else:
            raise UnsupportedOperationError(engine.value)

    def _get_driver_for_database_engine(self, database_engine: DatabaseEngine) -> TestDriver:
        if database_engine == DatabaseEngine.MYSQL:
            return TestDriver.MYSQL
        elif database_engine == DatabaseEngine.PG:
            return TestDriver.PG
        else:
            raise UnsupportedOperationError(database_engine.value)

    def make_sure_instances_up(self, instances: List[str], deadline: Optional[float] = None) -> None:
        """Wait until the instances have the RDS status ``available``, then until each accepts a connection.

        All the waits together last up to ``INSTANCE_UP_TIMEOUT_SEC``, or until ``deadline`` if that is
        sooner. The status is read at least once and, if every instance is available, each one gets at
        least one connection attempt, even after that time.

        :param instances: the ids of the instances to check.
        :param deadline: a ``timeit.default_timer()`` value that ends the waits sooner.
        """
        stop_time = timeit.default_timer() + INSTANCE_UP_TIMEOUT_SEC
        if deadline is not None:
            stop_time = min(stop_time, deadline)
        self._wait_until_instances_available(instances, stop_time)

        database_info: TestDatabaseInfo = TestEnvironment.get_current().get_database_info()
        for i in instances:
            instance_info: TestInstanceInfo = database_info.get_instance(i)
            last_error: Optional[Exception] = None
            # Try each instance at least once, even when the deadline has already passed
            while True:
                try:
                    conn = self._open_connection(instance_info)
                    conn.close()
                    last_error = None
                    break
                except Exception as e:
                    last_error = e
                    if timeit.default_timer() >= stop_time:
                        break
                    sleep(1)
            if last_error is not None:
                # The conftest reruns a test that fails with this message once
                pytest.fail(f"Instance {i} did not accept a connection: {type(last_error).__name__}: {last_error}")

    def _wait_until_instances_available(self, instance_ids: List[str], stop_time: float) -> None:
        cluster_id = TestEnvironment.get_current().get_cluster_name()
        filters = [{"Name": "db-cluster-id", "Values": [cluster_id]}]
        last_detail = ""
        # Read the status at least once, even when the deadline has already passed
        while True:
            try:
                response = self._client.describe_db_instances(Filters=filters)
            except Exception as e:
                detail = f"{type(e).__name__}: {e}"
                # Not a cluster outage, so the conftest doesn't rerun a test that fails with this message
                failure = f"Could not read the RDS status of the instances: {detail}"
            else:
                statuses = {
                    instance["DBInstanceIdentifier"]: instance["DBInstanceStatus"] for instance in response["DBInstances"]}
                missing = [instance_id for instance_id in instance_ids if instance_id not in statuses]
                if missing:
                    # RDS lists an instance from its creation until its deletion, so waiting doesn't help
                    pytest.fail(f"Instances not in cluster '{cluster_id}': {', '.join(missing)}")
                not_available = [instance_id for instance_id in instance_ids if statuses[instance_id] != "available"]
                if not not_available:
                    return
                detail = ", ".join(f"{instance_id} is '{statuses[instance_id]}'" for instance_id in not_available)
                # The conftest reruns a test that fails with this message once
                failure = f"Instances did not become available: {detail}"
            if detail != last_detail:
                self.logger.debug(f"Waiting for the RDS status available: {detail}")
                last_detail = detail
            if timeit.default_timer() >= stop_time:
                pytest.fail(failure)
            sleep(INSTANCE_STATUS_POLL_INTERVAL_SEC)

    @staticmethod
    def create_user(conn, username, password):
        engine = TestEnvironment.get_current().get_engine()
        if engine == DatabaseEngine.PG:
            sql = f"CREATE USER {username} WITH PASSWORD '{password}'"
        elif engine == DatabaseEngine.MYSQL:
            sql = f"CREATE USER {username} IDENTIFIED BY '{password}'"
        else:
            raise RuntimeError(Messages.get_formatted("RdsTestUtility.InvalidDatabaseEngine", engine.value))
        cursor = conn.cursor()
        cursor.execute(sql)

    @staticmethod
    def get_aurora_engine_name(engine: DatabaseEngine):
        if engine == DatabaseEngine.PG:
            return "aurora-postgresql"
        elif engine == DatabaseEngine.MYSQL:
            return "aurora-mysql"

        raise RuntimeError(Messages.get_formatted("RdsTestUtility.InvalidDatabaseEngine", engine.value))

    def get_cluster_by_arn(self, cluster_arn: str) -> Optional[Any]:
        response = self._client.describe_db_clusters(Filters=[{'Name': 'db-cluster-id', 'Values': [cluster_arn]}])
        clusters = response["DBClusters"]
        if len(clusters) < 1:
            return None

        return clusters[0]

    def get_rds_instance_info_by_arn(self, instance_arn: str) -> Optional[Any]:
        response = self._client.describe_db_instances(Filters=[{'Name': 'db-instance-id', 'Values': [instance_arn]}])
        instances = response["DBInstances"]
        if len(instances) < 1:
            return None

        return instances[0]

    def get_blue_green_deployment(self, bg_id: str) -> Optional[Any]:
        try:
            response: Any = self._client.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bg_id)
            deployments = response.get("BlueGreenDeployments")
            if len(deployments) < 1:
                return None

            return deployments[0]
        except self._client.exceptions.BlueGreenDeploymentNotFoundFault:
            return None

    def switchover_blue_green_deployment(self, bg_id: str):
        try:
            self._client.switchover_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            self.logger.debug("switchover_blue_green_deployment request is sent.")
        except botocore.exceptions.ClientError as e:
            error_info = e.response['Error']
            self.logger.debug(
                f"switchover_blue_green_deployment error: code={error_info['Code']}, message={error_info['Message']}")

            if error_info['Message']:
                error_message = error_info['Message']
            else:
                error_message = (f"The switchover_blue_green_deployment request for the blue/green deployment with "
                                 f"ID '{bg_id}'failed for an unspecified reason")

            raise Exception(error_message)

    def simulate_temporary_failure(self, instance_name: str, delay_ms: int, failure_duration_ms: int) -> Thread:
        def temporary_failure():
            if delay_ms > 0:
                sleep(delay_ms / 1_000)

            if instance_name == "*":
                ProxyHelper.disable_all_connectivity()
            else:
                ProxyHelper.disable_connectivity(instance_name)
            sleep(failure_duration_ms / 1_000)

            if instance_name == "*":
                ProxyHelper.enable_all_connectivity()
            else:
                ProxyHelper.enable_connectivity(instance_name)

        simulate_temporary_failure_thread: Thread = Thread(daemon=True, name="SimulateTemporaryFailureThread",
                                                           target=temporary_failure)
        return simulate_temporary_failure_thread

    def get_sleep_sql(self, seconds: float) -> str:
        engine = TestEnvironment.get_current().get_engine()
        if engine == DatabaseEngine.PG:
            return f"SELECT pg_catalog.pg_sleep({seconds})"
        elif engine == DatabaseEngine.MYSQL:
            return f"SELECT SLEEP({seconds})"
        else:
            raise UnsupportedOperationError(Messages.get_formatted("RdsTestUtility.InvalidDatabaseEngine", engine.value))
