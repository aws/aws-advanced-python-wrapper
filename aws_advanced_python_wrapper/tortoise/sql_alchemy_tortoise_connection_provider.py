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
from types import ModuleType
from typing import Callable, Dict

from sqlalchemy import Dialect
from sqlalchemy.dialects.mysql import mysqlconnector
from sqlalchemy.dialects.postgresql import psycopg

from aws_advanced_python_wrapper.database_dialect import DatabaseDialect
from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.sql_alchemy_connection_provider import SqlAlchemyPooledConnectionProvider
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)



class SqlAlchemyTortoisePooledConnectionProvider(SqlAlchemyPooledConnectionProvider):
    """
    Tortoise-specific pooled connection provider that handles failover by disposing pools.
    """

    _sqlalchemy_dialect_map : Dict[str, ModuleType] = {
        "MySQLDriverDialect": mysqlconnector,
        "PostgresDriverDialect": psycopg
    }

    def accepts_host_info(self, host_info: HostInfo, props: Properties) -> bool:
        if self._accept_url_func:
            return self._accept_url_func(host_info, props)
        url_type = SqlAlchemyPooledConnectionProvider._rds_utils.identify_rds_type(host_info.host)
        return url_type.is_rds

    def _create_pool(
            self,
            target_func: Callable,
            driver_dialect: DriverDialect,
            database_dialect: DatabaseDialect,
            host_info: HostInfo,
            props: Properties):
        kwargs = dict() if self._pool_configurator is None else self._pool_configurator(host_info, props)
        prepared_properties = driver_dialect.prepare_connect_info(host_info, props)
        database_dialect.prepare_conn_props(prepared_properties)
        kwargs["creator"] = self._get_connection_func(target_func, prepared_properties)
        dialect = self._get_pool_dialect(driver_dialect)
        if not dialect:
            raise AwsWrapperError(Messages.get_formatted("SqlAlchemyTortoisePooledConnectionProvider.NoDialect", driver_dialect.__class__.__name__))

        '''
        We need to pass in pre_ping and dialect to QueuePool the queue pool to enable health checks.
        Without this health check, we could be using dead connections after a failover.
        '''
        kwargs["pre_ping"] = True
        kwargs["dialect"] = dialect
        return self._create_sql_alchemy_pool(**kwargs)
    
    def _get_pool_dialect(self, driver_dialect: DriverDialect) -> Dialect:
        dialect = None
        driver_dialect_class_name = driver_dialect.__class__.__name__
        if driver_dialect_class_name == "SqlAlchemyDriverDialect":
            driver_dialect_class_name = driver_dialect_class_name._underlying_driver_dialect.__class__.__name__
        module = self._sqlalchemy_dialect_map.get(driver_dialect_class_name)

        if not module:
            return dialect
        dialect = module.dialect()
        dialect.dbapi = driver_dialect.get_driver_module()

        return dialect
