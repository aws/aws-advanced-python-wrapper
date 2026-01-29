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

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Set

if TYPE_CHECKING:
    from tests.integration.container.utils.test_driver import TestDriver

from time import perf_counter_ns, sleep
from uuid import uuid4

import pytest
from boto3 import client
from botocore.exceptions import ClientError

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import (FailoverSuccessError,
                                                ReadWriteSplittingError)
from aws_advanced_python_wrapper.hostinfo import HostRole
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from tests.integration.container.utils.conditions import (
    disable_on_features, enable_on_deployments, enable_on_num_instances)
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@enable_on_num_instances(min_instances=3)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestCustomEndpoint:
    logger: ClassVar[Logger] = Logger(__name__)
    endpoint_id: ClassVar[str] = f"test-endpoint-1-{uuid4()}"
    endpoint_info: ClassVar[Dict[str, Any]] = {}
    reuse_existing_endpoint: ClassVar[bool] = False

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def props(self):
        p: Properties = Properties(
            {"plugins": "custom_endpoint,read_write_splitting,failover", "connect_timeout": 10_000, "autocommit": True})

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
            TestCustomEndpoint.endpoint_info = response_endpoint
            available = "available" == response_endpoint["Status"]
            if available:
                break

            sleep(3)

        if not available:
            pytest.fail(f"The test setup step timed out while waiting for the test custom endpoint to become available: "
                        f"'{TestCustomEndpoint.endpoint_id}'.")

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

    def wait_until_endpoint_has_members(self, rds_client, expected_members: Set[str]):
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
                        f"'{TestCustomEndpoint.endpoint_id}'.")

        duration_sec = (perf_counter_ns() - start_ns) / 1_000_000_000
        self.logger.debug(f"wait_until_endpoint_has_specified_members took {duration_sec} seconds.")

    def test_custom_endpoint_failover(self, test_driver: TestDriver, conn_utils, props, rds_utils):
        props["failover_mode"] = "reader_or_writer"

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        conn = AwsWrapperConnection.connect(target_driver_connect, **kwargs, **props)

        endpoint_members = self.endpoint_info["StaticMembers"]
        instance_id = rds_utils.query_instance_id(conn)
        assert instance_id in endpoint_members

        # Use failover API to break connection.
        target_id = None if instance_id == rds_utils.get_cluster_writer_instance_id() else instance_id
        rds_utils.failover_cluster_and_wait_until_writer_changed(target_id=target_id)

        rds_utils.assert_first_query_throws(conn, FailoverSuccessError)

        instance_id = rds_utils.query_instance_id(conn)
        assert instance_id in endpoint_members

        conn.close()

    def _setup_custom_endpoint_role(self, target_driver_connect, conn_kwargs, rds_utils, host_role: HostRole):
        self.logger.debug("Setting up custom endpoint instance with role: " + host_role.name)
        props = {'plugins': ''}
        original_writer = rds_utils.get_cluster_writer_instance_id()
        failover_target = None
        with AwsWrapperConnection.connect(target_driver_connect, **conn_kwargs, **props) as conn:
            endpoint_members = self.endpoint_info["StaticMembers"]
            original_instance_id = rds_utils.query_instance_id(conn)
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

        rds_utils.failover_cluster_and_wait_until_writer_changed(target_id=failover_target)

        self.logger.debug("Verifying that new connection has role: " + host_role.name)
        # Verify that new connection is now the correct role
        with AwsWrapperConnection.connect(target_driver_connect, **conn_kwargs, **props) as conn:
            endpoint_members = self.endpoint_info["StaticMembers"]
            original_instance_id = rds_utils.query_instance_id(conn)
            assert original_instance_id in endpoint_members

            new_role = rds_utils.query_host_role(conn, TestEnvironment.get_current().get_engine())
            assert new_role == host_role
        self.logger.debug("Custom endpoint instance successfully set to role: " + host_role.name)

    def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes__with_reader_as_init_conn(
            self, test_driver: TestDriver, conn_utils, props, rds_utils):
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
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        # This setting is not required for the test, but it allows us to also test re-creation of expired monitors since
        # it takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
        props["custom_endpoint_idle_monitor_expiration_ms"] = 30_000
        props["wait_for_custom_endpoint_info_timeout_ms"] = 30_000

        # Ensure that we are starting with a reader connection
        self._setup_custom_endpoint_role(target_driver_connect, kwargs, rds_utils, HostRole.READER)

        conn = AwsWrapperConnection.connect(target_driver_connect, **kwargs, **props)
        endpoint_members = self.endpoint_info["StaticMembers"]
        original_reader_id = rds_utils.query_instance_id(conn)
        assert original_reader_id in endpoint_members

        # Attempt to switch to an instance of the opposite role. This should fail since the custom endpoint consists
        # only of the current host.
        self.logger.debug("Initial connection is to a reader. Attempting to switch to writer...")
        with pytest.raises(ReadWriteSplittingError):
            conn.read_only = False

        writer_id = rds_utils.get_cluster_writer_instance_id()

        rds_client = client('rds', region_name=TestEnvironment.get_current().get_aurora_region())
        rds_client.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=self.endpoint_id,
            StaticMembers=[original_reader_id, writer_id]
        )

        try:
            self.wait_until_endpoint_has_members(rds_client, {original_reader_id, writer_id})

            # We should now be able to switch to writer.
            conn.read_only = False
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == writer_id

            # Switch back to original instance
            conn.read_only = True
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == original_reader_id
        finally:
            # Remove the writer from the custom endpoint.
            rds_client.modify_db_cluster_endpoint(
                DBClusterEndpointIdentifier=self.endpoint_id,
                StaticMembers=[original_reader_id])
            self.wait_until_endpoint_has_members(rds_client, {original_reader_id})

        # We should not be able to switch again because new_member was removed from the custom endpoint.
        # We are connected to the reader. Attempting to switch to the writer will throw an exception.
        with pytest.raises(ReadWriteSplittingError):
            conn.read_only = False

        conn.close()

    def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes__with_writer_as_init_conn(
            self, test_driver: TestDriver, conn_utils, props, rds_utils):
        '''
        Will test for the following scenario:
        1. Iniitially connect to the writer instance via the custom endpoint.
        2. Attempt to switch to reader instance - should succeed, but will still use writer instance as reader.
        3. Modify the custom endpoint to add a reader instance as a static member.
        4. Switch to reader instance - should succeed.
        5. Switch back to writer instance - should succeed.
        6. Modify the custom endpoint to remove the reader instance as a static member.
        7. Attempt to switch to reader instance - should fail since the custom endpoint no longer has the reader instance.
        '''

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        # This setting is not required for the test, but it allows us to also test re-creation of expired monitors since
        # it takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
        props["custom_endpoint_idle_monitor_expiration_ms"] = 30_000
        props["wait_for_custom_endpoint_info_timeout_ms"] = 30_000

        # Ensure that we are starting with a writer connection
        self._setup_custom_endpoint_role(target_driver_connect, kwargs, rds_utils, HostRole.WRITER)
        conn = AwsWrapperConnection.connect(target_driver_connect, **kwargs, **props)

        endpoint_members = self.endpoint_info["StaticMembers"]
        original_writer_id = str(rds_utils.query_instance_id(conn))
        assert original_writer_id in endpoint_members

        # We are connected to the writer. Attempting to switch to the reader will not work but will intentionally
        # not throw an exception. In this scenario we log a warning and purposefully stick with the writer.
        self.logger.debug("Initial connection is to the writer. Attempting to switch to reader...")
        conn.read_only = True
        new_instance_id = rds_utils.query_instance_id(conn)
        assert new_instance_id == original_writer_id

        instances = TestEnvironment.get_current().get_instances()
        writer_id = str(rds_utils.get_cluster_writer_instance_id())

        reader_id_to_add = ""
        # Get any reader id
        for instance in instances:
            if instance.get_instance_id() != writer_id:
                reader_id_to_add = instance.get_instance_id()
                break

        rds_client = client('rds', region_name=TestEnvironment.get_current().get_aurora_region())
        rds_client.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=self.endpoint_id,
            StaticMembers=[original_writer_id, reader_id_to_add]
        )

        try:
            self.wait_until_endpoint_has_members(rds_client, {original_writer_id, reader_id_to_add})
            # We should now be able to switch to new_member.
            conn.read_only = True
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == reader_id_to_add

            # Switch back to original instance
            conn.read_only = False
        finally:
            # Remove the reader from the custom endpoint.
            rds_client.modify_db_cluster_endpoint(
                DBClusterEndpointIdentifier=self.endpoint_id,
                StaticMembers=[original_writer_id])
            self.wait_until_endpoint_has_members(rds_client, {original_writer_id})

        # We should not be able to switch again because new_member was removed from the custom endpoint.
        # We are connected to the writer. Attempting to switch to the reader will not work but will intentionally
        # not throw an exception. In this scenario we log a warning and fallback to the writer.
        conn.read_only = True
        new_instance_id = rds_utils.query_instance_id(conn)
        assert new_instance_id == original_writer_id

        conn.close()
