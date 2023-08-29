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

from typing import Optional

from .test_environment import TestEnvironment


class ConnectionUtils:

    @property
    def user(self) -> str:
        return TestEnvironment.get_current().get_database_info().get_username()

    @property
    def iam_user(self) -> str:
        return TestEnvironment.get_current().get_info().get_iam_user_name()

    @property
    def password(self) -> str:
        return TestEnvironment.get_current().get_database_info().get_password()

    @property
    def dbname(self) -> str:
        return TestEnvironment.get_current().get_database_info().get_default_db_name()

    @property
    def port(self) -> int:
        return TestEnvironment.get_current().get_writer().get_port()

    @property
    def writer_host(self) -> str:
        return TestEnvironment.get_current().get_writer().get_host()

    @property
    def writer_cluster_host(self) -> str:
        return TestEnvironment.get_current().get_database_info().get_cluster_endpoint()

    @property
    def reader_cluster_host(self) -> str:
        return TestEnvironment.get_current().get_database_info().get_cluster_read_only_endpoint()

    def get_conn_string(self, host: Optional[str] = None) -> str:
        host = self.writer_host if host is None else host
        return f"host={host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}"

    @property
    def proxy_port(self) -> int:
        return TestEnvironment.get_current().get_proxy_writer().get_port()

    @property
    def proxy_writer_host(self) -> str:
        return TestEnvironment.get_current().get_proxy_writer().get_host()

    @property
    def proxy_writer_cluster_host(self) -> str:
        return TestEnvironment.get_current().get_proxy_database_info().get_cluster_endpoint()

    @property
    def proxy_reader_cluster_host(self) -> str:
        return TestEnvironment.get_current().get_proxy_database_info().get_cluster_read_only_endpoint()

    def get_proxy_conn_string(self, host: Optional[str] = None):
        host = self.proxy_writer_host if host is None else host
        return f"host={host} port={self.proxy_port} dbname={self.dbname} user={self.user} password={self.password}"
