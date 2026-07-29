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

"""Async twin of test_custom_endpoint.py.

Exercises the custom_endpoint plugin on AsyncAwsWrapperConnection: endpoint
member list refresh, connection routing to custom-endpoint members, and
behavior when endpoint membership changes mid-connection.

Translation notes vs the sync file:
- ``AwsWrapperConnection.connect(...)`` → ``await connect_async(...)``
- ``conn.read_only = value`` → ``await conn.set_read_only(value)``
- ``rds_utils.query_instance_id(conn)`` → ``await query_instance_id_async(conn, rds_utils)``
- ``rds_utils.query_host_role(conn, engine)`` → ``await _query_host_role_async(conn, rds_utils)``
- ``rds_utils.assert_first_query_throws(conn, exc)`` → ``await assert_first_query_throws_async(conn, rds_utils, exc)``
- boto3 RDS management calls (create/modify/delete endpoints) stay synchronous
- ``with AwsWrapperConnection.connect(...) as conn:`` → manual open + try/finally close
"""

from __future__ import annotations

import asyncio
import socket
from time import perf_counter_ns, sleep
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Set
from uuid import uuid4

import pytest
from boto3 import client
from botocore.exceptions import ClientError

from aws_advanced_python_wrapper.errors import (FailoverSuccessError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.async_connection_helpers import (
    assert_first_query_throws_async, cleanup_async, connect_async,
    query_instance_id_async)
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployments, enable_on_num_instances)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.aio.wrapper import \
        AsyncAwsWrapperConnection
    from tests.integration.container.utils.connection_utils import \
        ConnectionUtils
    from tests.integration.container.utils.test_driver import TestDriver


# ---------------------------------------------------------------------------
# Module-level async helpers (inlined from sync rds_test_utility to avoid
# calling sync cursor methods on an AsyncAwsWrapperConnection).
# ---------------------------------------------------------------------------

async def _query_host_role_async(
        conn: AsyncAwsWrapperConnection,
        rds_utils: RdsTestUtility) -> HostRole:
    """Async counterpart of ``rds_utils.query_host_role(conn, engine)``.

    ``rds_test_utility.query_host_role`` calls ``conn.cursor()`` and uses
    sync cursor methods. We replicate the logic here for async connections.
    """
    engine = TestEnvironment.get_current().get_engine()
    if engine == DatabaseEngine.MYSQL:
        is_reader_query = "SELECT @@innodb_read_only"
    else:
        is_reader_query = "SELECT pg_catalog.pg_is_in_recovery()"

    async with conn.cursor() as cur:
        await cur.execute(is_reader_query)
        record = await cur.fetchone()
        is_reader = record[0]

    if is_reader in (1, True):
        return HostRole.READER
    else:
        return HostRole.WRITER


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------

@enable_on_num_instances(min_instances=3)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestCustomEndpointAsync:
    logger: ClassVar[Logger] = Logger(__name__)
    endpoint_id: ClassVar[str] = f"test-endpoint-1-async-{uuid4()}"
    endpoint_info: ClassVar[Dict[str, Any]] = {}
    reuse_existing_endpoint: ClassVar[bool] = False

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def default_props(self):
        p: Properties = Properties(
            {"connect_timeout": 10_000, "autocommit": True, "cluster_id": "cluster1"})

        features = TestEnvironment.get_current().get_features()
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features \
                or TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            WrapperProperties.ENABLE_TELEMETRY.set(p, True)
            WrapperProperties.TELEMETRY_SUBMIT_TOPLEVEL.set(p, True)
        if TestEnvironmentFeatures.TELEMETRY_TRACES_ENABLED in features:
            WrapperProperties.TELEMETRY_TRACES_BACKEND.set(p, "XRAY")
        if TestEnvironmentFeatures.TELEMETRY_METRICS_ENABLED in features:
            WrapperProperties.TELEMETRY_METRICS_BACKEND.set(p, "OTLP")

        return p

    @pytest.fixture(scope='class')
    def props_with_failover(self, default_props):
        p = default_props.copy()
        p["plugins"] = "custom_endpoint,read_write_splitting,failover_v2"
        return p

    @pytest.fixture(scope='class')
    def props(self, default_props):
        p = default_props.copy()
        p["plugins"] = "custom_endpoint,read_write_splitting"
        return p

    @pytest.fixture(scope='class', autouse=True)
    def setup_and_teardown(self):
        env_info = TestEnvironment.get_current().get_info()
        region = env_info.get_region()

        rds_client = client('rds', region_name=region)
        if not self.reuse_existing_endpoint:
            instances = env_info.get_database_info().get_instances()
            self._create_endpoint(rds_client, instances[0:1])

        self.wait_until_endpoint_available(rds_client)

        yield

        if not self.reuse_existing_endpoint:
            self.delete_endpoint(rds_client)

        rds_client.close()

    def wait_until_endpoint_available(self, rds_client):
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        available = False

        while perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(
                DBClusterEndpointIdentifier=self.endpoint_id,
                Filters=[
                    {
                        "Name": "db-cluster-endpoint-type",
                        "Values": ["custom"]
                    }
                ]
            )

            response_endpoints = response["DBClusterEndpoints"]
            if len(response_endpoints) != 1:
                sleep(3)  # Endpoint needs more time to get created.
                continue

            response_endpoint = response_endpoints[0]
            TestCustomEndpointAsync.endpoint_info = response_endpoint
            available = "available" == response_endpoint["Status"]
            if available:
                break

            sleep(3)

        if not available:
            pytest.fail(f"The test setup step timed out while waiting for the test custom endpoint to become available: "
                        f"'{TestCustomEndpointAsync.endpoint_id}'.")

        # The RDS API flips the endpoint to "available" before its DNS record
        # has propagated to the resolver this host uses. Connecting in that
        # window fails the test setup with "[Errno -2] Name or service not
        # known" -- observed deterministically on the multi-instance Aurora
        # axes (these tests only run with >=3 instances). Wait for the endpoint
        # hostname to actually resolve before any test connects through it.
        self._wait_until_endpoint_dns_resolves(TestCustomEndpointAsync.endpoint_info["Endpoint"])

    def _wait_until_endpoint_dns_resolves(self, hostname: str):
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        last_error: Optional[BaseException] = None
        while perf_counter_ns() < end_ns:
            try:
                socket.getaddrinfo(hostname, None)
                return
            except OSError as ex:  # name resolution not propagated yet
                last_error = ex
                sleep(3)
        pytest.fail(
            "The test setup step timed out while waiting for the custom "
            f"endpoint DNS to resolve: '{hostname}' (last error: {last_error}).")

    def _create_endpoint(self, rds_client, instances):
        instance_ids = [instance.get_instance_id() for instance in instances]
        rds_client.create_db_cluster_endpoint(
            DBClusterEndpointIdentifier=self.endpoint_id,
            DBClusterIdentifier=TestEnvironment.get_current().get_cluster_name(),
            EndpointType="ANY",
            StaticMembers=instance_ids
        )

    def delete_endpoint(self, rds_client):
        try:
            rds_client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=self.endpoint_id)
            # Wait for the endpoint to be deleted
            self._wait_until_endpoint_deleted(rds_client)
        except ClientError as e:
            # If the custom endpoint already does not exist, we can continue. Otherwise, fail the test.
            if e.response['Error']['Code'] != 'DBClusterEndpointNotFoundFault':
                pytest.fail(e)

    def _wait_until_endpoint_deleted(self, rds_client):
        """Wait until the custom endpoint is deleted (max 3 minutes)"""
        end_ns = perf_counter_ns() + 3 * 60 * 1_000_000_000  # 3 minutes
        deleted = False

        while perf_counter_ns() < end_ns:
            try:
                response = rds_client.describe_db_cluster_endpoints(
                    DBClusterEndpointIdentifier=self.endpoint_id,
                    Filters=[
                        {
                            "Name": "db-cluster-endpoint-type",
                            "Values": ["custom"]
                        }
                    ]
                )

                response_endpoints = response["DBClusterEndpoints"]
                if len(response_endpoints) == 0:
                    deleted = True
                    break

                # Check if endpoint is in deleting state
                endpoint_status = response_endpoints[0]["Status"]
                if endpoint_status == "deleting":
                    sleep(3)
                    continue

            except ClientError as e:
                # If we get DBClusterEndpointNotFoundFault, the endpoint is deleted
                if e.response['Error']['Code'] == 'DBClusterEndpointNotFoundFault':
                    deleted = True
                    break
                else:
                    # Some other error occurred
                    sleep(3)
                    continue

            sleep(3)

        if not deleted:
            self.logger.warning(f"Timed out waiting for custom endpoint to be deleted: '{self.endpoint_id}'. "
                                f"The endpoint may still be in the process of being deleted.")
        else:
            self.logger.debug(f"Custom endpoint '{self.endpoint_id}' successfully deleted.")

    def wait_until_endpoint_has_members(self, rds_client, expected_members: Set[str], rds_utils):
        start_ns = perf_counter_ns()
        end_ns = perf_counter_ns() + 20 * 60 * 1_000_000_000  # 20 minutes
        has_correct_state = False
        while perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(DBClusterEndpointIdentifier=self.endpoint_id)
            response_endpoints = response["DBClusterEndpoints"]
            if len(response_endpoints) != 1:
                response_ids = [endpoint["DBClusterEndpointIdentifier"] for endpoint in response_endpoints]
                pytest.fail("Unexpected number of endpoints returned while waiting for custom endpoint to have the "
                            f"specified list of members. Expected 1, got {len(response_endpoints)}. "
                            f"Endpoint IDs: {response_ids}.")

            endpoint = response_endpoints[0]
            response_members = set(endpoint["StaticMembers"])
            has_correct_state = response_members == expected_members and "available" == endpoint["Status"]
            if has_correct_state:
                break

            sleep(3)

        if not has_correct_state:
            pytest.fail(f"Timed out while waiting for the custom endpoint to stabilize: "
                        f"'{TestCustomEndpointAsync.endpoint_id}'.")

        rds_utils.make_sure_instances_up(list(expected_members))
        duration_sec = (perf_counter_ns() - start_ns) / 1_000_000_000
        self.logger.debug(f"wait_until_endpoint_has_specified_members took {duration_sec} seconds.")

    def test_custom_endpoint_failover_async(self, test_driver: TestDriver, conn_utils: ConnectionUtils,
                                            props_with_failover, rds_utils):
        props_with_failover["failover_mode"] = "reader_or_writer"
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]

        async def inner():
            conn = await connect_async(
                test_driver=test_driver,
                connect_params=kwargs,
                **dict(props_with_failover)
            )
            try:
                endpoint_members = self.endpoint_info["StaticMembers"]
                instance_id = await query_instance_id_async(conn, rds_utils)
                assert instance_id in endpoint_members

                # Use failover API to break connection.
                target_id = None if instance_id == rds_utils.get_cluster_writer_instance_id() else instance_id
                rds_utils.failover_cluster_and_wait_until_writer_changed(target_id=target_id)

                await assert_first_query_throws_async(conn, rds_utils, FailoverSuccessError)

                instance_id = await query_instance_id_async(conn, rds_utils)
                assert instance_id in endpoint_members
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    async def _setup_custom_endpoint_role_async(
            self,
            test_driver: TestDriver,
            conn_kwargs: Dict[str, Any],
            rds_utils: RdsTestUtility,
            host_role: HostRole) -> None:
        self.logger.debug("Setting up custom endpoint instance with role: " + host_role.name)
        props = {'plugins': ''}
        original_writer = rds_utils.get_cluster_writer_instance_id()
        failover_target = None

        conn = await connect_async(
            test_driver=test_driver,
            connect_params=conn_kwargs,
            **props
        )
        try:
            endpoint_members = self.endpoint_info["StaticMembers"]
            original_instance_id = await query_instance_id_async(conn, rds_utils)
            self.logger.debug("Original instance id: " + original_instance_id)
            assert original_instance_id in endpoint_members

            if host_role == HostRole.WRITER:
                if original_instance_id == original_writer:
                    self.logger.debug("Role is already " + host_role.name + ", no failover needed.")
                    return  # Do nothing, no need to failover.
                failover_target = original_instance_id
                self.logger.debug("Failing over to get writer role...")
            elif host_role == HostRole.READER:
                if original_instance_id != original_writer:
                    self.logger.debug("Role is already " + host_role.name + ", no failover needed.")
                    return  # Do nothing, no need to failover.
                self.logger.debug("Failing over to get reader role...")
        finally:
            await conn.close()

        rds_utils.failover_cluster_and_wait_until_writer_changed(target_id=failover_target)

        self.logger.debug("Verifying that new connection has role: " + host_role.name)
        # Verify that new connection is now the correct role
        conn = await connect_async(
            test_driver=test_driver,
            connect_params=conn_kwargs,
            **props
        )
        try:
            endpoint_members = self.endpoint_info["StaticMembers"]
            original_instance_id = await query_instance_id_async(conn, rds_utils)
            assert original_instance_id in endpoint_members

            new_role = await _query_host_role_async(conn, rds_utils)
            assert new_role == host_role
        finally:
            await conn.close()
        self.logger.debug("Custom endpoint instance successfully set to role: " + host_role.name)

    def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes__with_reader_as_init_conn_async(
            self, test_driver: TestDriver, conn_utils: ConnectionUtils, props_with_failover, rds_utils):
        '''
        Will test for the following scenario:
        1. Initially connect to a reader instance via the custom endpoint.
        2. Attempt to switch to writer instance - should fail since the custom endpoint only has the reader instance.
        3. Modify the custom endpoint to add the writer instance as a static member.
        4. Switch to writer instance - should succeed.
        5. Switch back to reader instance - should succeed.
        6. Modify the custom endpoint to remove the writer instance as a static member.
        7. Attempt to switch to writer instance - should fail since the custom endpoint no longer has the writer instance.
        '''
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        # This setting is not required for the test, but it allows us to also test re-creation of expired monitors since
        # it takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
        props_with_failover["custom_endpoint_idle_monitor_expiration_ms"] = 30_000
        props_with_failover["wait_for_custom_endpoint_info_timeout_ms"] = 30_000

        async def inner():
            # Ensure that we are starting with a reader connection
            await self._setup_custom_endpoint_role_async(test_driver, kwargs, rds_utils, HostRole.READER)

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=kwargs,
                **dict(props_with_failover)
            )
            try:
                endpoint_members = self.endpoint_info["StaticMembers"]
                original_reader_id = await query_instance_id_async(conn, rds_utils)
                assert original_reader_id in endpoint_members

                # Attempt to switch to an instance of the opposite role. This should fail since the custom endpoint
                # consists only of the current host.
                self.logger.debug("Initial connection is to a reader. Attempting to switch to writer...")
                with pytest.raises(ReadWriteSplittingError):
                    await conn.set_read_only(False)

                writer_id = rds_utils.get_cluster_writer_instance_id()

                rds_client = client('rds', region_name=TestEnvironment.get_current().get_aurora_region())
                rds_client.modify_db_cluster_endpoint(
                    DBClusterEndpointIdentifier=self.endpoint_id,
                    StaticMembers=[original_reader_id, writer_id]
                )

                try:
                    self.wait_until_endpoint_has_members(rds_client, {original_reader_id, writer_id}, rds_utils)

                    # We should now be able to switch to writer.
                    await conn.set_read_only(False)
                    new_instance_id = await query_instance_id_async(conn, rds_utils)
                    assert new_instance_id == writer_id

                    # Switch back to original instance
                    await conn.set_read_only(True)
                    new_instance_id = await query_instance_id_async(conn, rds_utils)
                    assert new_instance_id == original_reader_id
                finally:
                    # Remove the writer from the custom endpoint.
                    rds_client.modify_db_cluster_endpoint(
                        DBClusterEndpointIdentifier=self.endpoint_id,
                        StaticMembers=[original_reader_id])
                    self.wait_until_endpoint_has_members(rds_client, {original_reader_id}, rds_utils)

                # We should not be able to switch again because new_member was removed from the custom endpoint.
                # We are connected to the reader. Attempting to switch to the writer will throw an exception.
                with pytest.raises(ReadWriteSplittingError):
                    await conn.set_read_only(False)
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())

    def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes__with_writer_as_init_conn_async(
            self, test_driver: TestDriver, conn_utils: ConnectionUtils, props, rds_utils):
        """
        Will test for the following scenario:
        1. Initially connect to the writer instance via the custom endpoint.
        2. Attempt to switch to reader instance - should succeed, but will still use writer instance as reader.
        3. Modify the custom endpoint to add a reader instance as a static member.
        4. Switch to reader instance - should succeed.
        5. Switch back to writer instance - should succeed.
        6. Modify the custom endpoint to remove the reader instance as a static member.
        7. Attempt to switch to reader instance - should fail since the custom endpoint no longer has the reader instance.
        """
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        # This setting is not required for the test, but it allows us to also test re-creation of expired monitors since
        # it takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
        props["custom_endpoint_idle_monitor_expiration_ms"] = 30_000
        props["wait_for_custom_endpoint_info_timeout_ms"] = 30_000

        async def inner():
            # Ensure that we are starting with a writer connection
            await self._setup_custom_endpoint_role_async(test_driver, kwargs, rds_utils, HostRole.WRITER)

            conn = await connect_async(
                test_driver=test_driver,
                connect_params=kwargs,
                **dict(props)
            )
            try:
                endpoint_members = self.endpoint_info["StaticMembers"]
                original_writer_id = str(await query_instance_id_async(conn, rds_utils))
                assert original_writer_id in endpoint_members

                # We are connected to the writer. Attempting to switch to the reader will not work but will
                # intentionally not throw an exception. In this scenario we log a warning and purposefully stick
                # with the writer.
                self.logger.debug("Initial connection is to the writer. Attempting to switch to reader...")
                await conn.set_read_only(True)
                new_instance_id = await query_instance_id_async(conn, rds_utils)
                assert new_instance_id == original_writer_id

                instances = TestEnvironment.get_current().get_instances()
                writer_id = str(rds_utils.get_cluster_writer_instance_id())

                reader_id_to_add = ""
                # Get any reader id that is neither the AWS-truth writer nor the
                # wrapper's currently-observed writer. After a failover, the
                # wrapper's SQL-queried ``original_writer_id`` may briefly lag
                # AWS's view (cluster topology refresh hasn't completed), so we
                # must exclude both to avoid emitting a duplicate-id StaticMembers
                # list which RDS rejects with InvalidParameterValueException.
                for instance in instances:
                    instance_id = instance.get_instance_id()
                    if instance_id != writer_id and instance_id != original_writer_id:
                        reader_id_to_add = instance_id
                        break

                rds_client = client('rds', region_name=TestEnvironment.get_current().get_aurora_region())
                rds_client.modify_db_cluster_endpoint(
                    DBClusterEndpointIdentifier=self.endpoint_id,
                    StaticMembers=[original_writer_id, reader_id_to_add]
                )

                try:
                    self.wait_until_endpoint_has_members(rds_client, {original_writer_id, reader_id_to_add}, rds_utils)
                    # We should now be able to switch to new_member.
                    await conn.set_read_only(True)
                    new_instance_id = await query_instance_id_async(conn, rds_utils)
                    assert new_instance_id == reader_id_to_add

                    # Switch back to original instance
                    await conn.set_read_only(False)
                finally:
                    # Remove the reader from the custom endpoint.
                    rds_client.modify_db_cluster_endpoint(
                        DBClusterEndpointIdentifier=self.endpoint_id,
                        StaticMembers=[original_writer_id])
                    self.wait_until_endpoint_has_members(rds_client, {original_writer_id}, rds_utils)

                # We should not be able to switch again because new_member was removed from the custom endpoint.
                # We are connected to the writer. Attempting to switch to the reader will not work but will
                # intentionally not throw an exception. In this scenario we log a warning and fallback to the writer.
                await conn.set_read_only(True)
                new_instance_id = await query_instance_id_async(conn, rds_utils)
                assert new_instance_id == original_writer_id
            finally:
                await conn.close()
                await cleanup_async()

        asyncio.run(inner())
