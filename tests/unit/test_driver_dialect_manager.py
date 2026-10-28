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


import psycopg
import pytest
from mysql.connector import MySQLConnection

from aws_advanced_python_wrapper.driver_dialect_codes import DriverDialectCodes
from aws_advanced_python_wrapper.driver_dialect_manager import \
    DriverDialectManager
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.mysql_driver_dialect import MySQLDriverDialect
from aws_advanced_python_wrapper.pg_driver_dialect import PgDriverDialect
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.utils import Utils


@pytest.fixture
def manager():
    return DriverDialectManager()


@pytest.fixture
def pg_not_installed(mocker):
    pg_path = DriverDialectManager.known_dialects_by_code[DriverDialectCodes.PSYCOPG]
    initialize = Utils.initialize_class
    mocker.patch.object(Utils, "initialize_class",
                        side_effect=lambda path, *args: None if path == pg_path else initialize(path, *args))


def code_props(code):
    return Properties({WrapperProperties.DRIVER_DIALECT.name: code})


def test_get_dialect_code_selects_dialect(manager, mocker):
    assert isinstance(manager.get_dialect(mocker.MagicMock(), code_props(DriverDialectCodes.PSYCOPG)), PgDriverDialect)


def test_get_dialect_unknown_code_raises(manager, mocker):
    with pytest.raises(AwsWrapperError, match="Unknown target driver dialect code: oracle"):
        manager.get_dialect(mocker.MagicMock(), code_props("oracle"))


def test_get_dialect_for_connection_custom_dialect_not_used(manager, mocker):
    DriverDialectManager.set_custom_dialect(mocker.MagicMock())
    assert manager.get_dialect_for_connection(object(), Properties()) is None


def test_get_dialect_for_connection_code_wins_over_detection(manager, mocker):
    pg_conn = mocker.MagicMock(spec=psycopg.Connection)
    dialect = manager.get_dialect_for_connection(pg_conn, code_props(DriverDialectCodes.MYSQL_CONNECTOR_PYTHON))
    assert isinstance(dialect, MySQLDriverDialect)


def test_get_dialect_for_connection_unknown_code_raises(manager):
    with pytest.raises(AwsWrapperError, match="Unknown target driver dialect code: pg"):
        manager.get_dialect_for_connection(object(), code_props("pg"))


def test_get_dialect_for_connection_code_driver_not_installed_raises(manager, pg_not_installed):
    with pytest.raises(AwsWrapperError, match="unable to initialize the target driver dialect"):
        manager.get_dialect_for_connection(object(), code_props(DriverDialectCodes.PSYCOPG))


@pytest.mark.parametrize("spec, dialect_class", [(psycopg.Connection, PgDriverDialect), (MySQLConnection, MySQLDriverDialect)])
def test_get_dialect_for_connection_detects_driver(manager, mocker, spec, dialect_class):
    assert isinstance(manager.get_dialect_for_connection(mocker.MagicMock(spec=spec), Properties()), dialect_class)


def test_get_dialect_for_connection_skips_driver_not_installed(manager, mocker, pg_not_installed):
    assert manager.get_dialect_for_connection(mocker.MagicMock(spec=psycopg.Connection), Properties()) is None


def test_get_dialect_for_connection_unknown_object_returns_none(manager):
    assert manager.get_dialect_for_connection(object(), Properties()) is None
