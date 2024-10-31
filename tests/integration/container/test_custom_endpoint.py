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
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY, TestEnvironmentFeatures.PERFORMANCE])
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

        while not available and perf_counter_ns() < end_ns:
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
            else:
                sleep(3)

        if not available:
            pytest.fail("The test setup step timed out while waiting for the test custom endpoint to become available.")

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
        except ClientError as e:
            # If the custom endpoint already does not exist, we can continue. Otherwise, fail the test.
            if e.response['Error']['Code'] != 'DBClusterEndpointNotFoundFault':
                pytest.fail(e)

    def wait_until_endpoint_has_members(self, rds_client, expected_members: Set[str]):
        start_ns = perf_counter_ns()
        end_ns = perf_counter_ns() + 20 * 60 * 1_000_000_000  # 20 minutes
        has_correct_state = False
        while not has_correct_state and perf_counter_ns() < end_ns:
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
            else:
                sleep(3)

        if not has_correct_state:
            pytest.fail("Timed out while waiting for the custom endpoint to stabilize.")

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

    def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes(
            self, test_driver: TestDriver, conn_utils, props, rds_utils):
        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = self.endpoint_info["Endpoint"]
        # This setting is not required for the test, but it allows us to also test re-creation of expired monitors since
        # it takes more than 30 seconds to modify the cluster endpoint (usually around 140s).
        props["custom_endpoint_idle_monitor_expiration_ms"] = 30_000
        conn = AwsWrapperConnection.connect(target_driver_connect, **kwargs, **props)

        endpoint_members = self.endpoint_info["StaticMembers"]
        original_instance_id = rds_utils.query_instance_id(conn)
        assert original_instance_id in endpoint_members

        # Attempt to switch to an instance of the opposite role. This should fail since the custom endpoint consists
        # only of the current host.
        new_read_only_value = original_instance_id == rds_utils.get_cluster_writer_instance_id()
        if new_read_only_value:
            # We are connected to the writer. Attempting to switch to the reader will not work but will intentionally
            # not throw an exception. In this scenario we log a warning and purposefully stick with the writer.
            self.logger.debug("Initial connection is to the writer. Attempting to switch to reader...")
            conn.read_only = new_read_only_value
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == original_instance_id
        else:
            # We are connected to the reader. Attempting to switch to the writer will throw an exception.
            self.logger.debug("Initial connection is to a reader. Attempting to switch to writer...")
            with pytest.raises(ReadWriteSplittingError):
                conn.read_only = new_read_only_value

        instances = TestEnvironment.get_current().get_instances()
        writer_id = rds_utils.get_cluster_writer_instance_id()
        if original_instance_id == writer_id:
            new_member = instances[1].get_instance_id()
        else:
            new_member = writer_id

        rds_client = client('rds', region_name=TestEnvironment.get_current().get_aurora_region())
        rds_client.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=self.endpoint_id,
            StaticMembers=[original_instance_id, new_member]
        )

        try:
            self.wait_until_endpoint_has_members(rds_client, {original_instance_id, new_member})

            # We should now be able to switch to new_member.
            conn.read_only = new_read_only_value
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == new_member

            # Switch back to original instance
            conn.read_only = not new_read_only_value
        finally:
            rds_client.modify_db_cluster_endpoint(
                DBClusterEndpointIdentifier=self.endpoint_id,
                StaticMembers=[original_instance_id])
            self.wait_until_endpoint_has_members(rds_client, {original_instance_id})

        # We should not be able to switch again because new_member was removed from the custom endpoint.
        if new_read_only_value:
            # We are connected to the writer. Attempting to switch to the reader will not work but will intentionally
            # not throw an exception. In this scenario we log a warning and purposefully stick with the writer.
            conn.read_only = new_read_only_value
            new_instance_id = rds_utils.query_instance_id(conn)
            assert new_instance_id == original_instance_id
        else:
            # We are connected to the reader. Attempting to switch to the writer will throw an exception.
            with pytest.raises(ReadWriteSplittingError):
                conn.read_only = new_read_only_value

        conn.close()
