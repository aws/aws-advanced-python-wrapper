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

from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.pep249 import Connection
    from .test_database_info import TestDatabaseInfo

import socket
import timeit
from datetime import datetime, timedelta
from time import perf_counter_ns, sleep

import boto3
import pytest
from botocore.config import Config

from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.driver_info import DriverInfo
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from .database_engine import DatabaseEngine
from .database_engine_deployment import DatabaseEngineDeployment
from .driver_helper import DriverHelper
from .test_driver import TestDriver
from .test_environment import TestEnvironment
from .test_instance_info import TestInstanceInfo


class RdsTestUtility:
    logger = Logger(__name__)

    _client: Any

    def __init__(self, region: str):
        config = Config(region_name=region)
        self._client = boto3.client('rds', config=config)

    def get_db_instance(self, instance_id: str) -> Optional[Dict[str, Any]]:
        filters = [{'Name': "db-instance-id", 'Values': [f"{instance_id}"]}]
        response = self._client.describe_db_instances(DBInstanceIdentifier=instance_id,
                                                      Filters=filters)
        instances = response.get("DBInstances")
        if instances is None or len(instances) == 0:
            return None
        return instances[0]

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
            DBClusterIdentifier=environment.get_info().get_cluster_name(),
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

    def wait_until_cluster_has_desired_status(self, cluster_id: str, desired_status: str) -> None:
        cluster_info = self.get_db_cluster(cluster_id)
        status = cluster_info.get("Status")
        while status != desired_status:
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
            cluster_id = TestEnvironment.get_current().get_info().get_cluster_name()

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
                raise Exception(Messages.get("RdsTestUtility.FailoverRequestNotSuccessful"))
            self.failover_cluster(cluster_id)

        # Failover has finished, wait for DNS to be updated so cluster endpoint resolves to the new writer instance.
        cluster_address = socket.gethostbyname(cluster_endpoint)
        start_time = timeit.default_timer()
        while cluster_address == initial_cluster_address and (timeit.default_timer() - start_time) < 300:  # 5 min
            sleep(1)
            cluster_address = socket.gethostbyname(cluster_endpoint)

        self.logger.debug("Testing.FinishedFailover", initial_writer_id, str((perf_counter_ns() - start) / 1_000_000))

    def failover_cluster(self, cluster_id: Optional[str] = None) -> None:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_cluster_name()

        self.wait_until_cluster_has_desired_status(cluster_id, "available")

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
        return initial_writer_id != current_writer_id

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
        elif DatabaseEngineDeployment.MULTI_AZ == database_deployment:
            return self._query_multi_az_instance_id(conn, database_engine)
        else:
            raise RuntimeError(Messages.get_formatted(
                "RdsTestUtility.MethodNotSupportedForDeployment", "query_instance_id", database_deployment))

    def _query_aurora_instance_id(self, conn: Connection, engine: DatabaseEngine) -> str:
        if engine == DatabaseEngine.MYSQL:
            sql = "SELECT @@aurora_server_id"
        elif engine == DatabaseEngine.PG:
            sql = "SELECT aurora_db_instance_identifier()"
        else:
            raise UnsupportedOperationError(engine.value)

        cursor = conn.cursor()
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
            cluster_id = TestEnvironment.get_current().get_info().get_cluster_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if m.get("DBInstanceIdentifier") == instance_id:
                return cast('bool', m.get("IsClusterWriter"))
        raise Exception(Messages.get_formatted("RdsTestUtility.ClusterMemberNotFound", instance_id))

    def get_cluster_writer_instance_id(self, cluster_id: Optional[str] = None) -> str:
        if cluster_id is None:
            cluster_id = TestEnvironment.get_current().get_info().get_cluster_name()
        cluster_info = self.get_db_cluster(cluster_id)
        members = cluster_info.get("DBClusterMembers")
        for m in members:
            if cast('bool', m.get("IsClusterWriter")):
                return cast('str', m.get("DBInstanceIdentifier"))
        raise Exception(Messages.get_formatted("RdsTestUtility.WriterInstanceNotFound", cluster_id))

    def get_instance_ids(self) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        deployment: DatabaseEngineDeployment = test_environment.get_deployment()
        if DatabaseEngineDeployment.AURORA == deployment:
            return self._get_aurora_instance_ids()
        elif DatabaseEngineDeployment.MULTI_AZ == deployment:
            return self._get_multi_az_instance_ids()
        else:
            raise RuntimeError("RdsTestUtility.MethodNotSupportedForDeployment", "get_instance_ids", deployment)

    def _get_aurora_instance_ids(self) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        engine: DatabaseEngine = test_environment.get_engine()
        instance_info: TestInstanceInfo = test_environment.get_writer()

        sql = self._get_aurora_topology_sql(engine)
        conn = self._open_connection(instance_info)
        cursor = conn.cursor()
        cursor.execute(sql)
        records = cursor.fetchall()

        result: List[str] = list()
        for r in records:
            result.append(r[0])
        conn.close()

        return result

    def _get_multi_az_instance_ids(self) -> List[str]:
        test_environment: TestEnvironment = TestEnvironment.get_current()
        engine: DatabaseEngine = test_environment.get_engine()
        instance_info: TestInstanceInfo = test_environment.get_writer()

        conn = self._open_connection(instance_info)
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

        return result

    def _open_connection(self, instance_info: TestInstanceInfo) -> Any:

        env: TestEnvironment = TestEnvironment.get_current()
        database_engine: DatabaseEngine = env.get_engine()
        test_driver = self._get_driver_for_database_engine(database_engine)

        target_driver_connect = DriverHelper.get_connect_func(test_driver)

        user = env.get_database_info().get_username()
        password = env.get_database_info().get_password()
        db = env.get_database_info().get_default_db_name()

        print(f"asdfasdf Connecting using db: {db}")
        conn_params = DriverHelper.get_connect_params(
            instance_info.get_host(), instance_info.get_port(), user, password, db, test_driver)

        conn = target_driver_connect(**conn_params, connect_timeout=3)
        return conn

    def _get_aurora_topology_sql(self, engine: DatabaseEngine) -> str:
        if engine == DatabaseEngine.MYSQL:
            return ("SELECT SERVER_ID, SESSION_ID FROM information_schema.replica_host_status "
                    "ORDER BY IF(SESSION_ID = 'MASTER_SESSION_ID', 0, 1)")
        elif engine == DatabaseEngine.PG:
            return ("SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
                    "ORDER BY CASE WHEN SESSION_ID = 'MASTER_SESSION_ID' THEN 0 ELSE 1 END")
        else:
            raise UnsupportedOperationError(engine.value)

    def _get_multi_az_topology_sql(self, engine: DatabaseEngine, writer_id) -> str:
        if engine == DatabaseEngine.MYSQL:
            return f"SELECT id, endpoint, port FROM mysql.rds_topology ORDER BY id='{writer_id}' DESC"
        elif engine == DatabaseEngine.PG:
            return (f"SELECT id, endpoint, port "
                    f"FROM rds_tools.show_topology('aws_python_driver-{DriverInfo.DRIVER_VERSION}')"
                    f"ORDER BY id='{writer_id}' DESC")
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
