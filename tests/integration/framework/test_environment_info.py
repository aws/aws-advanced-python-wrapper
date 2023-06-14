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


class TestEnvironmentInfo:
    __test__ = False

    _request: TestEnvironmentRequest
    _aws_access_key_id: str
    _aws_secret_access_key: str
    _aws_session_token: str
    _aurora_region: str
    _aurora_cluster_name: str
    _iam_user_name: str
    _database_info: TestDatabaseInfo
    _proxy_database_info: TestProxyDatabaseInfo

    def __init__(self, dict: Dict[str, Any]) -> None:
        if dict is None:
            return

        request_dict: Dict[str, Any] = typing.cast(Dict[str, Any],
                                                   dict.get("request"))
        if request_dict is not None:
            self._request = TestEnvironmentRequest(request_dict)

        self._aws_access_key_id = typing.cast(str, dict.get("awsAccessKeyId"))
        self._aws_secret_access_key = typing.cast(
            str, dict.get("awsSecretAccessKey"))
        self._aws_session_token = typing.cast(str, dict.get("awsSessionToken"))
        self._aurora_region = typing.cast(str, dict.get("auroraRegion"))
        self._aurora_cluster_name = typing.cast(str,
                                                dict.get("auroraClusterName"))
        self._iam_user_name = typing.cast(str, dict.get("iamUsername"))

        database_info_dict: Dict[str,
                                 Any] = typing.cast(Dict[str, Any],
                                                    dict.get("databaseInfo"))
        if database_info_dict is not None:
            self._database_info = TestDatabaseInfo(database_info_dict)

        proxy_database_info_dict: Dict[str, Any] = typing.cast(
            Dict[str, Any], dict.get("proxyDatabaseInfo"))
        if proxy_database_info_dict is not None:
            self._proxy_database_info = TestProxyDatabaseInfo(
                proxy_database_info_dict)

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

    def get_aurora_region(self) -> str:
        return self._aurora_region

    def get_aurora_cluster_name(self) -> str:
        return self._aurora_cluster_name

    def get_iam_user_name(self) -> str:
        return self._iam_user_name
