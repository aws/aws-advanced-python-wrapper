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

import typing
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from .test_database_info import TestDatabaseInfo
    from .test_instance_info import TestInstanceInfo

import socket
import timeit
from logging import getLogger
from time import perf_counter_ns, sleep
from typing import Any, List, Optional

import boto3
from botocore.config import Config

from .database_engine import DatabaseEngine
from .driver_helper import DriverHelper
from .test_driver import TestDriver
from .test_environment import TestEnvironment


class AuroraTestUtility:
    logger = getLogger(__name__)

    _client: Any

    def __init__(self, region: str):
        config = Config(region_name=region)
        self._client = boto3.client('rds', config=config)

    def wait_until_cluster_has_right_state(self, cluster_id: str) -> None:
        cluster_info = self.get_db_cluster(cluster_id)
        status = cluster_info.get("Status")
        while status != "available":
            sleep(1)
            cluster_info = self.get_db_cluster(cluster_id)
            status = cluster_info.get("Status")

    def get_db_cluster(self, cluster_id: str) -> Any:
        response: Any = self._client.describe_db_clusters(DBClusterIdentifier=cluster_id)
        clusters = response.get("DBClusters")
        if clusters is None or len(clusters) == 0:
            return None
        return clusters[0]

    def failover_cluster_and_wait_until_writer_changed(
            self, initial_writer_id: Optional[str] = None, cluster_id: Optional[str] = None) -> None:
        start = perf_counter_ns()
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_aurora_cluster_name()

        if initial_writer_id is None:
            initial_writer_id = self.get_cluster_writer_instance_id(cluster_id)

        database_info = TestEnvironment.get_current().get_database_info()
        cluster_endpoint = database_info.get_cluster_endpoint()
        initial_cluster_address = socket.gethostbyname(cluster_endpoint)

        self.failover_cluster(cluster_id)
        remaining_attempts = 5
        while not self.writer_changed(initial_writer_id, cluster_id, 300):
            # if writer is not changed, try triggering failover again
            remaining_attempts -= 1
            if remaining_attempts == 0:
                raise Exception("Failover cluster request was not successful.")
            self.failover_cluster(cluster_id)

        # Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the new writer instance.
        cluster_address = socket.gethostbyname(cluster_endpoint)
        start_time = timeit.default_timer()
        while cluster_address == initial_cluster_address and (timeit.default_timer() - start_time) < 300:  # 5 min
            sleep(1)
            cluster_address = socket.gethostbyname(cluster_endpoint)

        self.logger.debug(
            f"Finished failover from {initial_writer_id} in {(perf_counter_ns() - start) / 1_000_000}ms\n")

    def failover_cluster(self, cluster_id: Optional[str] = None) -> None:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_aurora_cluster_name()

        self.wait_until_cluster_has_right_state(cluster_id)

        remaining_attempts = 10
        while remaining_attempts > 0:
            remaining_attempts -= 1
            try:
                result = self._client.failover_db_cluster(DBClusterIdentifier=cluster_id)
                http_status_code = result.get("ResponseMetadata").get("HTTPStatusCode")
                if result.get("DBCluster") is not None and http_status_code == 200:
                    return
                sleep(1)
            except Exception:
                sleep(1)

    def writer_changed(self, initial_writer_id: str, cluster_id: str, timeout: int) -> bool:
        wait_until = timeit.default_timer() + timeout

        current_writer_id = self.get_cluster_writer_instance_id(cluster_id)
        while initial_writer_id == current_writer_id and timeit.default_timer() < wait_until:
            sleep(3)
            current_writer_id = self.get_cluster_writer_instance_id(cluster_id)
        return (initial_writer_id != current_writer_id)

    def assert_first_query_throws(
            self,
            conn,
            exception_cls,
            database_engine: Optional[DatabaseEngine] = None) -> None:
        if database_engine is None:
            database_engine = TestEnvironment.get_current().get_engine()
        with pytest.raises(exception_cls):
            cursor = conn.cursor()
            cursor.execute(self._get_instance_id_sql(database_engine))
            cursor.fetchone()

    def _get_instance_id_sql(self, database_engine: DatabaseEngine) -> str:
        if database_engine == DatabaseEngine.MYSQL:
            return "SELECT @@aurora_server_id as id"
        elif database_engine == DatabaseEngine.PG:
            return "SELECT aurora_db_instance_identifier()"
        else:
            raise NotImplementedError(database_engine)

    def query_instance_id(
            self,
            conn,
            database_engine: Optional[DatabaseEngine] = None) -> str:
        if database_engine is None:
            database_engine = TestEnvironment.get_current().get_engine()

        cursor = conn.cursor()
        cursor.execute(self._get_instance_id_sql(database_engine))
        record = cursor.fetchone()
        return record[0]

    def is_db_instance_writer(self, instance_id: str, cluster_id: Optional[str] = None) -> bool:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_aurora_cluster_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if m.get("DBInstanceIdentifier") == instance_id:
                return typing.cast('bool', m.get("IsClusterWriter"))
        raise Exception("Cannot find cluster member whose db instance identifier is " + instance_id)

    def get_cluster_writer_instance_id(self, cluster_id: Optional[str] = None) -> str:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_aurora_cluster_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if typing.cast('bool', m.get("IsClusterWriter")):
                return typing.cast('str', m.get("DBInstanceIdentifier"))
        raise Exception("Cannot find writer instance for cluster " + cluster_id)

    def get_aurora_instance_ids(self) -> List[str]:

        test_environment: TestEnvironment = TestEnvironment.get_current()
        database_engine: DatabaseEngine = test_environment.get_engine()
        instance_info: TestInstanceInfo = test_environment.get_writer()

        conn = self._open_connection(instance_info)
        sql: str = self._get_topology_sql(database_engine)
        cursor = conn.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()

        result: List[str] = list()
        for r in records:
            result.append(r[0])
        conn.close()

        return result

    def _open_connection(self, instance_info: TestInstanceInfo) -> Any:

        test_environment: TestEnvironment = TestEnvironment.get_current()
        database_engine: DatabaseEngine = test_environment.get_engine()
        test_driver = self._get_driver_for_database_engine(database_engine)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)

        db_name: str = test_environment.get_database_info().get_default_db_name()
        user: str = test_environment.get_database_info().get_username()
        password: str = test_environment.get_database_info().get_password()
        # TODO: connection params should be driver specific
        connect_params: str = "host={0} port={1} dbname={2} user={3} password={4} connect_timeout=3".format(
            instance_info.get_host(), instance_info.get_port(), db_name, user, password)

        conn = target_driver_connect(connect_params)
        return conn

    def _get_topology_sql(self, database_engine: DatabaseEngine) -> str:
        if database_engine == DatabaseEngine.MYSQL:
            return "SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status \
                ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)"
        elif database_engine == DatabaseEngine.PG:
            return "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() \
                ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END"
        else:
            raise NotImplementedError(database_engine)

    def _get_driver_for_database_engine(self, database_engine: DatabaseEngine) -> TestDriver:
        if database_engine == DatabaseEngine.MYSQL:
            return TestDriver.MYSQL
        elif database_engine == DatabaseEngine.MARIADB:
            return TestDriver.MARIADB
        elif database_engine == DatabaseEngine.PG:
            return TestDriver.PG
        else:
            raise NotImplementedError(database_engine)

    def make_sure_instances_up(self, instances: List[str]) -> None:
        database_info: TestDatabaseInfo = TestEnvironment.get_current().get_database_info()
        for i in instances:
            instance_info: TestInstanceInfo = database_info.get_instance(i)
            success: bool = False
            start_time = timeit.default_timer()
            while (timeit.default_timer() - start_time) < 300:  # 5 min
                try:
                    conn = self._open_connection(instance_info)
                    conn.close()
                    success = True
                    break
                except Exception:
                    sleep(1)
            assert success
