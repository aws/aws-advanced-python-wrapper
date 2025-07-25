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

import typing
from typing import Any, Dict

from .test_database_info import TestDatabaseInfo
from .test_environment_request import TestEnvironmentRequest
from .test_proxy_database_info import TestProxyDatabaseInfo
from .test_telemetry_info import TestTelemetryInfo


class TestEnvironmentInfo:
    __test__ = False

    _request: TestEnvironmentRequest
    _aws_access_key_id: str
    _aws_secret_access_key: str
    _aws_session_token: str
    _region: str
    _rds_endpoint: str
    _db_name: str
    _iam_user_name: str
    _bg_deployment_id: str
    _cluster_parameter_group: str
    _random_base: str
    _database_info: TestDatabaseInfo
    _proxy_database_info: TestProxyDatabaseInfo
    _traces_telemetry_info: TestTelemetryInfo
    _metrics_telemetry_info: TestTelemetryInfo

    def __init__(self, test_info: Dict[str, Any]) -> None:
        if test_info is None:
            return

        request_dict: Dict[str, Any] = typing.cast('Dict[str, Any]', test_info.get("request"))
        if request_dict is not None:
            self._request = TestEnvironmentRequest(request_dict)

        self._aws_access_key_id = typing.cast('str', test_info.get("awsAccessKeyId"))
        self._aws_secret_access_key = typing.cast('str', test_info.get("awsSecretAccessKey"))
        self._aws_session_token = typing.cast('str', test_info.get("awsSessionToken"))
        self._region = typing.cast('str', test_info.get("region"))
        self._rds_endpoint = typing.cast('str', test_info.get("rdsEndpoint"))
        self._db_name = typing.cast('str', test_info.get("rdsDbName"))
        self._iam_user_name = typing.cast('str', test_info.get("iamUsername"))
        self._bg_deployment_id = typing.cast('str', test_info.get("blueGreenDeploymentId"))
        self._cluster_parameter_group = typing.cast('str', test_info.get("clusterParameterGroupName"))
        self._random_base = typing.cast('str', test_info.get("randomBase"))

        database_info_dict: Dict[str, Any] = typing.cast('Dict[str, Any]', test_info.get("databaseInfo"))
        if database_info_dict is not None:
            self._database_info = TestDatabaseInfo(database_info_dict)

        proxy_database_info_dict: Dict[str, Any] = typing.cast('Dict[str, Any]', test_info.get("proxyDatabaseInfo"))
        if proxy_database_info_dict is not None:
            self._proxy_database_info = TestProxyDatabaseInfo(proxy_database_info_dict)

        traces_telemetry_info_dict: Dict[str, Any] = (
            typing.cast('Dict[str, Any]', test_info.get("tracesTelemetryInfo")))
        if traces_telemetry_info_dict is not None:
            self._traces_telemetry_info = TestTelemetryInfo(traces_telemetry_info_dict)

        metrics_telemetry_info_dict: Dict[str, Any] = (
            typing.cast('Dict[str, Any]', test_info.get("metricsTelemetryInfo")))
        if metrics_telemetry_info_dict is not None:
            self._metrics_telemetry_info = TestTelemetryInfo(metrics_telemetry_info_dict)

    def get_database_info(self) -> TestDatabaseInfo:
        return self._database_info

    def get_proxy_database_info(self) -> TestProxyDatabaseInfo:
        return self._proxy_database_info

    def get_request(self) -> TestEnvironmentRequest:
        return self._request

    def get_aws_access_key_id(self) -> str:
        return self._aws_access_key_id

    def get_aws_secret_access_key(self) -> str:
        return self._aws_secret_access_key

    def get_aws_session_token(self) -> str:
        return self._aws_session_token

    def get_region(self) -> str:
        return self._region

    def get_rds_endpoint(self) -> str:
        return self._rds_endpoint

    def get_db_name(self) -> str:
        return self._db_name

    def get_iam_user_name(self) -> str:
        return self._iam_user_name

    def get_bg_deployment_id(self) -> str:
        return self._bg_deployment_id

    def get_cluster_parameter_group(self) -> str:
        return self._cluster_parameter_group

    def get_random_base(self) -> str:
        return self._random_base

    def get_traces_telemetry_info(self) -> TestTelemetryInfo:
        return self._traces_telemetry_info

    def get_metrics_telemetry_info(self) -> TestTelemetryInfo:
        return self._metrics_telemetry_info
