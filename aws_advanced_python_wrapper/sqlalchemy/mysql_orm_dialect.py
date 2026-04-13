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

# aws_advanced_python_wrapper/sqlalchemy/sqlalchemy_mysqlconnector_dialect.py
from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector


class SqlAlchemyOrmMysqlDialect(MySQLDialect_mysqlconnector):
    """
    SQLAlchemy dialect for AWS Advanced Python Wrapper with mysqlconnector. Extends the SQLAlchemy MySQL mysqlconnector dialect.
    This dialect is not related to the DriverDialect or DatabaseDialect classes used by our driver. Instead, it is used
    directly by SQLAlchemy. This dialect is registered in pyproject.toml and is selected by prefixing the connection
    string passed to create_engine with "mysql+aws_wrapper_mysqlconnector://" ("[name]+[driver]").
    """

    name = 'mysql'
    driver = 'aws_wrapper_mysqlconnector'
