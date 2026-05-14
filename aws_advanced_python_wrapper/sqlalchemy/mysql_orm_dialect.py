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

from typing import Optional
from typing import TYPE_CHECKING

from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector

import mysql.connector
from mysql.connector import CMySQLConnection
from mysql.connector.errors import Error
from sqlalchemy.engine import default

from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.properties import Properties, PropertiesUtils

if TYPE_CHECKING:
    from sqlalchemy import Connection
    from aws_advanced_python_wrapper.hostinfo import HostInfo

class SqlAlchemyOrmMysqlDialect(MySQLDialect_mysqlconnector):
    """
    SQLAlchemy dialect for AWS Advanced Python Wrapper with mysqlconnector. Extends the SQLAlchemy MySQL mysqlconnector dialect.
    This dialect is not related to the DriverDialect or DatabaseDialect classes used by our driver. Instead, it is used
    directly by SQLAlchemy. This dialect is registered in pyproject.toml and is selected by prefixing the connection
    string passed to create_engine with "mysql+aws_wrapper_mysqlconnector://" ("[name]+[driver]").
    """

    name = 'mysql'
    driver = 'aws_wrapper_mysqlconnector'

    @classmethod
    def import_dbapi(cls):
        """
        Return the DB-API 2.0 module.
        SQLAlchemy calls this to get the driver module.
        """
        import aws_advanced_python_wrapper
        return aws_advanced_python_wrapper

    def create_connect_args(self, url):
        """
        Transform SQLAlchemy URL into connection arguments.
        Must include the 'target' parameter for our wrapper driver.
        """
        # Extract standard connection parameters
        opts = url.translate_connect_args(username='user')

        # Add query string parameters
        opts.update(url.query)

        # Add the required 'target' parameter for our wrapper
        if 'target' not in opts:
            opts['target'] = mysql.connector.Connect
        if 'wrapper_plugins' not in opts:
            opts['plugins'] = "aurora_connection_tracker,failover"
        else:
            opts['plugins'] = opts['wrapper_plugins']
            opts.pop('wrapper_plugins', None)

        # Return empty args list and kwargs dict
        return [], opts

    def _detect_charset(self, connection: Connection) -> str:
        if isinstance(connection, CMySQLConnection):
            return connection.charset
        else:
            raise Exception("Could not detect charset because connection was not a CMySQLConnection.")

    def _extract_error_code(self, exception: BaseException) -> int:
        if isinstance(exception, AwsWrapperError):
            err = exception.driver_error
            if err and isinstance(err, Error):
                return err.errno
            else:
                raise Exception("Could not extract error code because driver_error was not a BaseException.")
        else:
            raise Exception("Could not extract error code because exception was not an AwsWrapperError.")

    def initialize(self, connection):
        """
        Override initialization to handle type introspection.
        The parent class tries to use TypeInfo.fetch() which requires
        a native SQLAlchemy connection, not AwsWrapperConnection.
        """

        # Unwrap SQLAlchemy's connection object
        wrapper_conn, wrapper_parent = self._get_wrapper_connection_and_parent(connection)

        # this is driver-based, does not need server version info
        # and is fairly critical for even basic SQL operations
        self._connection_charset: Optional[str] = self._detect_charset(
            wrapper_conn.target_connection
        )

        # call super().initialize() because we need to have
        # server_version_info set up.  in 1.4 under python 2 only this does the
        # "check unicode returns" thing, which is the one area that some
        # SQL gets compiled within initialize() currently
        default.DefaultDialect.initialize(self, connection)

        self._detect_sql_mode(connection)
        self._detect_ansiquotes(connection)  # depends on sql mode
        self._detect_casing(connection)
        if self._server_ansiquotes:
            # if ansiquotes == True, build a new IdentifierPreparer
            # with the new setting
            self.identifier_preparer = self.preparer(
                self, server_ansiquotes=self._server_ansiquotes
            )

        self.supports_sequences = (
                self.is_mariadb and self.server_version_info >= (10, 3)
        )

        self.supports_for_update_of = (
                self._is_mysql and self.server_version_info >= (8,)
        )

        self.use_mysql_for_share = (
                self._is_mysql and self.server_version_info >= (8, 0, 1)
        )

        self._needs_correct_for_88718_96365 = (
                not self.is_mariadb and self.server_version_info >= (8,)
        )

        self.delete_returning = (
                self.is_mariadb and self.server_version_info >= (10, 0, 5)
        )

        self.insert_returning = (
                self.is_mariadb and self.server_version_info >= (10, 5)
        )

        self._requires_alias_for_on_duplicate_key = (
                self._is_mysql and self.server_version_info >= (8, 0, 20)
        )

        self._warn_for_known_db_issues()

    def _get_wrapper_connection_and_parent(self, connection):
        """
        Traverse the connection chain to find AwsWrapperConnection and its parent connection.

        Args:
            connection: SQLAlchemy Connection object

        Returns:
            AwsWrapperConnection instance or None, parent connection of AwsWrapperConnection or None
        """
        # Start with the DBAPI connection
        parent = connection
        child = connection.connection

        # Traverse up to 5 levels deep (reasonable limit)
        for _ in range(5):
            if isinstance(child, AwsWrapperConnection):
                return child, parent

            # Try to go deeper if there's a .connection attribute
            if hasattr(child, 'connection'):
                parent = child
                child = child.connection
            else:
                break

        return None

    def prepare_connect_info(self, host_info: HostInfo, props: Properties) -> Properties:
        prop_copy: Properties = Properties(props.copy())

        prop_copy["host"] = host_info.host

        if host_info.is_port_specified():
            prop_copy["port"] = str(host_info.port)

        PropertiesUtils.remove_wrapper_props(prop_copy)
        return prop_copy
