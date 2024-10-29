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

from time import perf_counter_ns
from typing import Dict
from uuid import uuid4

import pytest
from boto3 import client
from botocore.exceptions import ClientError

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import FailoverSuccessError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.properties import Properties, WrapperProperties
from tests.integration.container.utils.conditions import enable_on_num_instances, enable_on_deployments, \
    disable_on_features
from tests.integration.container.utils.database_engine_deployment import DatabaseEngineDeployment
from tests.integration.container.utils.driver_helper import DriverHelper
from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from tests.integration.container.utils.test_driver import TestDriver
from tests.integration.container.utils.test_environment import TestEnvironment
from tests.integration.container.utils.test_environment_features import TestEnvironmentFeatures


@enable_on_num_instances(min_instances=3)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY, TestEnvironmentFeatures.PERFORMANCE])
class TestCustomEndpoint:
    logger = Logger(__name__)
    one_instance_endpoint_id = f"test-endpoint-1-{uuid4()}"
    two_instance_endpoint_id = f"test-endpoint-2-{uuid4()}"
    endpoints: Dict[str, Dict] = {one_instance_endpoint_id: None, two_instance_endpoint_id: None}
    reuse_existing_endpoints = False
    current_writer = None

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

    @pytest.fixture(autouse=True)
    def create_endpoints(self):
        env_info = TestEnvironment.get_current().get_info()
        cluster_id = env_info.get_cluster_name()
        region = env_info.get_region()

        rds_client = client('rds', region_name=region)
        if self.reuse_existing_endpoints:
            self.wait_until_endpoints_available(rds_client, cluster_id)
            return

        instances = env_info.get_database_info().get_instances()
        self.create_endpoint(rds_client, cluster_id, self.one_instance_endpoint_id, instances[0:1])
        self.create_endpoint(rds_client, cluster_id, self.two_instance_endpoint_id, instances[0:2])
        self.wait_until_endpoints_available(rds_client, cluster_id)

        yield

        if self.reuse_existing_endpoints:
            return

        self.delete_endpoints(rds_client)
        rds_client.close()

    def wait_until_endpoints_available(self, rds_client, cluster_id):
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        all_available = False

        while not all_available and perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(
                DBClusterIdentifier=cluster_id,
                Filters=[
                    {
                        "Name": "db-cluster-endpoint-type",
                        "Values": ["custom"]
                    }
                ]
            )

            response_endpoints = response["DBClusterEndpoints"]
            num_available = 0
            for response_endpoint in response_endpoints:
                endpoint_id = response_endpoint["DBClusterEndpointIdentifier"]
                if endpoint_id not in self.endpoints:
                    continue

                self.endpoints[endpoint_id] = response_endpoint
                if "available" == response_endpoint["Status"]:
                    num_available = num_available + 1

            all_available = num_available == len(self.endpoints)

        if not all_available:
            pytest.fail("The test setup step timed out while waiting for the new custom endpoints to become available.")

    def create_endpoint(self, rds_client, cluster_id, endpoint_id, instances):
        instance_ids = [instance.get_instance_id() for instance in instances]
        rds_client.create_db_cluster_endpoint(
            DBClusterEndpointIdentifier=endpoint_id,
            DBClusterIdentifier=cluster_id,
            EndpointType="ANY",
            StaticMembers=instance_ids
        )

    def delete_endpoints(self, rds_client):
        for endpoint_id in self.endpoints:
            try:
                rds_client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=endpoint_id)
            except ClientError as e:
                if e.response['Error']['Code'] == 'DBClusterEndpointNotFoundFault':
                    continue  # Custom endpoint already does not exist - do nothing.
                else:
                    pytest.fail(e)

        # TODO: Is this necessary if the endpoints have unique IDs so are unlikely to be used elsewhere? We could save
        #  some time by skipping this step.
        self.wait_until_endpoints_deleted(rds_client)

    def wait_until_endpoints_deleted(self, rds_client):
        cluster_id = TestEnvironment.get_current().get_info().get_cluster_name()
        end_ns = perf_counter_ns() + 5 * 60 * 1_000_000_000  # 5 minutes
        all_deleted = False

        while not all_deleted and perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(
                DBClusterIdentifier=cluster_id,
                Filters=[
                    {
                        "Name": "db-cluster-endpoint-type",
                        "Values": ["custom"]
                    }
                ]
            )

            response_ids = [endpoint["DBClusterEndpointIdentifier"] for endpoint in response["DBClusterEndpoints"]]
            all_deleted = all(endpoint_id not in self.endpoints for endpoint_id in response_ids)

        if not all_deleted:
            pytest.fail("The test setup step timed out while attempting to delete pre-existing test custom endpoints.")

    def wait_until_endpoint_has_specified_members(self, rds_client, endpoint_id, members_list):
        start_ns = perf_counter_ns()

        # Convert to set for later comparison.
        expected_members = set(members_list)
        end_ns = perf_counter_ns() + 20 * 60 * 1_000_000_000  # 20 minutes
        has_members = False
        while not has_members and perf_counter_ns() < end_ns:
            response = rds_client.describe_db_cluster_endpoints(DBClusterEndpointIdentifier=endpoint_id)
            response_endpoints = response["DBClusterEndpoints"]
            if len(response_endpoints) != 1:
                response_ids = [endpoint["DBClusterEndpointIdentifier"] for endpoint in response_endpoints]
                pytest.fail("Unexpected number of endpoints returned while waiting for custom endpoint to have the "
                            f"specified list of members. Expected 1, got {len(response_endpoints)}. "
                            f"Endpoint IDs: {response_ids}.")

            endpoint = response_endpoints[0]
            response_members = set(endpoint["StaticMembers"])
            has_members = response_members == expected_members

        if not has_members:
            pytest.fail("Timed out while waiting for the custom endpoint to stabilize.")

        duration_sec = (perf_counter_ns() - start_ns) / 1_000_000_000
        self.logger.debug(f"wait_until_endpoint_has_specified_members took {duration_sec} seconds.")

    def test_custom_endpoint_failover(self, test_driver: TestDriver, conn_utils, props, rds_utils):
        endpoint = self.endpoints.get(self.one_instance_endpoint_id)
        props["failover_mode"] = "reader-or-writer"

        target_driver_connect = DriverHelper.get_connect_func(test_driver)
        kwargs = conn_utils.get_connect_params()
        kwargs["host"] = endpoint["Endpoint"]
        conn = AwsWrapperConnection.connect(target_driver_connect, **kwargs, **props)

        endpoint_members = endpoint["StaticMembers"]
        instance_id = rds_utils.query_instance_id(conn)
        assert instance_id in endpoint_members

        # Use failover API to break connection.
        target_id = None if instance_id == rds_utils.get_cluster_writer_instance_id() else instance_id
        rds_utils.failover_cluster_and_wait_until_writer_changed(target_id=target_id)

        rds_utils.assert_first_query_throws(conn, FailoverSuccessError)

        instance_id = rds_utils.query_instance_id(conn)
        assert instance_id in endpoint_members

    # def test_custom_endpoint_read_write_splitting__with_custom_endpoint_changes(
    #         self, test_driver: TestDriver, conn_utils, props, rds_utils):
