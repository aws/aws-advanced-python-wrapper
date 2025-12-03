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

from typing import Any, Dict, Optional

from .database_engine import DatabaseEngine
from .driver_helper import DriverHelper
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

    def get_connect_params(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            dbname: Optional[str] = None) -> Dict[str, Any]:
        host = self.writer_host if host is None else host
        port = self.port if port is None else port
        user = self.user if user is None else user
        password = self.password if password is None else password
        dbname = self.dbname if dbname is None else dbname
        return DriverHelper.get_connect_params(host, port, user, password, dbname)

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

    def get_proxy_connect_params(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            dbname: Optional[str] = None) -> Dict[str, Any]:
        host = self.proxy_writer_host if host is None else host
        port = self.proxy_port if port is None else port
        user = self.user if user is None else user
        password = self.password if password is None else password
        dbname = self.dbname if dbname is None else dbname
        return DriverHelper.get_connect_params(host, port, user, password, dbname)
    
    def get_aws_tortoise_url(
            self,
            db_engine: DatabaseEngine,
            host: Optional[str] = None,
            port: Optional[int] = None,
            user: Optional[str] = None,
            password: Optional[str] = None,
            dbname: Optional[str] = None,
            **kwargs) -> str:
        """Build AWS MySQL connection URL for Tortoise ORM with query parameters."""
        host = self.writer_cluster_host if host is None else host
        port = self.port if port is None else port
        user = self.user if user is None else user
        password = self.password if password is None else password
        dbname = self.dbname if dbname is None else dbname
        
        # Build base URL
        protocol = "aws-pg" if db_engine == DatabaseEngine.PG else "aws-mysql"
        url = f"{protocol}://{user}:{password}@{host}:{port}/{dbname}"
        
        # Add all kwargs as query parameters
        if kwargs:
            params = [f"{key}={value}" for key, value in kwargs.items()]
            url += "?" + "&".join(params)
        
        return url
