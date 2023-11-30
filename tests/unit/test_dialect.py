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

from unittest.mock import patch

import psycopg
import pytest

from aws_advanced_python_wrapper.database_dialect import (
    AuroraMysqlDialect, AuroraPgDialect, DatabaseDialectManager, DialectCode,
    MysqlDatabaseDialect, PgDatabaseDialect, RdsMysqlDialect, RdsPgDialect,
    TargetDriverType, UnknownDatabaseDialect, MultiAzMysqlDialect)
from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.driver_info import DriverInfo
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def mock_conn(mocker):
    return mocker.MagicMock(spec=psycopg.Connection)


@pytest.fixture
def mock_session(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_cursor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_custom_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_candidate(mocker):
    return mocker.MagicMock(spec=DialectCode)


@pytest.fixture
def mock_fetchone_row(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_driver_dialect(mocker):
    return mocker.MagicMock()


@pytest.fixture
def pg_dialect():
    return PgDatabaseDialect()


@pytest.fixture
def mysql_dialect():
    return MysqlDatabaseDialect()


@pytest.fixture
def rds_mysql_dialect():
    return RdsMysqlDialect()


@pytest.fixture
def rds_pg_dialect():
    return RdsPgDialect()


@pytest.fixture
def aurora_pg_dialect():
    return AuroraPgDialect()


@pytest.fixture(autouse=True)
def mock_default_behavior(mock_conn, mock_cursor, mock_fetchone_row):
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = mock_fetchone_row


def test_pg_is_dialect(mock_conn, mock_cursor, mock_session, pg_dialect):
    assert pg_dialect.is_dialect(mock_conn)

    mock_cursor.fetchone.return_value = None
    assert not pg_dialect.is_dialect(mock_conn)


def test_mysql_is_dialect(mock_conn, mock_cursor, mock_session, mysql_dialect):
    records = [("some_value", "some_value"), ("some_value", "mysql")]
    mock_cursor.__iter__.return_value = records

    assert mysql_dialect.is_dialect(mock_conn)

    records = [("some_value", "some_value"), ("some_value", "some_value")]
    mock_cursor.__iter__.return_value = records

    assert not mysql_dialect.is_dialect(mock_conn)


@patch('aws_advanced_python_wrapper.database_dialect.super')
def test_rds_mysql_is_dialect(mock_super, mock_cursor, mock_conn, rds_mysql_dialect):
    mock_super().is_dialect.return_value = True

    records = [("some_value", "some_value"), ("some_value", "source distribution")]
    mock_cursor.__iter__.return_value = records

    assert rds_mysql_dialect.is_dialect(mock_conn)

    records = [("some_value", "some_value"), ("some_value", "some_value")]
    mock_cursor.__iter__.return_value = records

    assert not rds_mysql_dialect.is_dialect(mock_conn)

    mock_super().is_dialect.return_value = False

    assert not rds_mysql_dialect.is_dialect(mock_conn)


def test_aurora_mysql_is_dialect(mock_conn, mock_cursor):
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = None

    dialect = AuroraMysqlDialect()
    assert dialect.is_dialect(mock_conn) is False

    mock_cursor.fetchone.return_value = ('aurora_version', '3.0.0')
    assert dialect.is_dialect(mock_conn) is True


@patch('aws_advanced_python_wrapper.database_dialect.super')
def test_aurora_pg_is_dialect(mock_super, mock_conn, mock_cursor):
    aurora_pg_dialect = AuroraPgDialect()
    mock_conn.cursor.return_value = mock_cursor
    mock_super().is_dialect.return_value = True

    records = [("aurora_stat_utils", "aurora_stat_utils"), ("some_value", "source distribution")]
    mock_cursor.__iter__.return_value = records

    assert aurora_pg_dialect.is_dialect(mock_conn)

    mock_cursor.fetchone.return_value = None

    assert not aurora_pg_dialect.is_dialect(mock_conn)

    mock_cursor.fetchone.return_value = records
    mock_super().is_dialect.return_value = False

    assert not aurora_pg_dialect.is_dialect(mock_conn)


@patch('aws_advanced_python_wrapper.database_dialect.super')
def test_rds_pg_is_dialect(mock_super, mock_cursor, mock_conn, rds_pg_dialect):
    mock_super().is_dialect.return_value = True

    mock_cursor.__iter__.return_value = [(False, False), (True, False)]

    assert rds_pg_dialect.is_dialect(mock_conn)

    mock_cursor.__iter__.return_value = [(False, False), (False, False)]

    assert not rds_pg_dialect.is_dialect(mock_conn)

    mock_cursor.__iter__.return_value = []

    assert not rds_pg_dialect.is_dialect(mock_conn)

    mock_cursor.fetchone.return_value = [(False, False), (True, False)]
    mock_super().is_dialect.return_value = False

    assert not rds_pg_dialect.is_dialect(mock_conn)


def test_get_dialect_custom_dialect(mock_custom_dialect, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    manager._custom_dialect = mock_custom_dialect

    assert mock_custom_dialect == manager.get_dialect(mock_driver_dialect, Properties())


def test_get_dialect_user_setting(mock_driver_dialect):
    props = Properties({"host": "localhost", WrapperProperties.DIALECT.name: "custom"})
    manager = DatabaseDialectManager(props)

    with pytest.raises(AwsWrapperError):
        manager.get_dialect(mock_driver_dialect, props)

    props[WrapperProperties.DIALECT.name] = "invalid_dialect"
    with pytest.raises(AwsWrapperError):
        manager.get_dialect(mock_driver_dialect, props)

    props[WrapperProperties.DIALECT.name] = "aurora-pg"
    assert isinstance(manager.get_dialect(mock_driver_dialect, props), AuroraPgDialect)
    assert isinstance(manager._dialect, AuroraPgDialect)
    assert manager._dialect_code == DialectCode.AURORA_PG


def test_prepare_conn_props__multi_az_mysql():
    dialect = MultiAzMysqlDialect()
    props = Properties({"host": "some_host"})
    expected = Properties({
        "host": "some_host",
        "conn_attrs": {
            "python_wrapper_name": "aws_python_driver",
            "python_wrapper_version": DriverInfo.DRIVER_VERSION
        }
    })

    dialect.add_conn_props(props)
    assert props == expected

    props = Properties({"conn_attrs": {"some_attr": "some_val"}})
    expected = Properties({
        "conn_attrs": {
            "some_attr": "some_val",
            "python_wrapper_name": "aws_python_driver",
            "python_wrapper_version": DriverInfo.DRIVER_VERSION
        }
    })

    dialect.add_conn_props(props)
    assert props == expected


def test_get_dialect_aurora_mysql(mock_driver_dialect):
    props = Properties({"host": "my-database.cluster-xyz.us-east-2.rds.amazonaws.com"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.MYSQL):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), AuroraMysqlDialect)
        assert isinstance(manager._dialect, AuroraMysqlDialect)
        assert DialectCode.AURORA_MYSQL == manager._dialect_code
        assert manager._can_update is False


def test_get_dialect_rds_mysql(mock_driver_dialect):
    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.MYSQL):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), RdsMysqlDialect)
        assert isinstance(manager._dialect, RdsMysqlDialect)
        assert DialectCode.RDS_MYSQL == manager._dialect_code
        assert manager._can_update is True


def test_get_dialect_mysql(mock_driver_dialect):
    props = Properties({"host": "localhost"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.MYSQL):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), MysqlDatabaseDialect)
        assert isinstance(manager._dialect, MysqlDatabaseDialect)
        assert DialectCode.MYSQL == manager._dialect_code
        assert manager._can_update is True


def test_get_dialect_aurora_pg(mock_driver_dialect):
    props = Properties({"host": "my-database.cluster-xyz.us-east-2.rds.amazonaws.com"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.POSTGRES):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), AuroraPgDialect)
        assert isinstance(manager._dialect, AuroraPgDialect)
        assert DialectCode.AURORA_PG == manager._dialect_code
        assert manager._can_update is True


def test_get_dialect_rds_pg(mock_driver_dialect):
    props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.POSTGRES):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), RdsPgDialect)
        assert isinstance(manager._dialect, RdsPgDialect)
        assert DialectCode.RDS_PG == manager._dialect_code
        assert manager._can_update is True


def test_get_dialect_pg(mock_driver_dialect):
    props = Properties({"host": "localhost"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=TargetDriverType.POSTGRES):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), PgDatabaseDialect)
        assert isinstance(manager._dialect, PgDatabaseDialect)
        assert DialectCode.PG == manager._dialect_code
        assert manager._can_update is True


def test_get_dialect_unknown_dialect(mock_driver_dialect):
    props = Properties({"host": "localhost"})
    manager = DatabaseDialectManager(props)

    with patch.object(manager, '_get_target_driver_type', return_value=None):
        assert isinstance(manager.get_dialect(mock_driver_dialect, props), UnknownDatabaseDialect)
        assert isinstance(manager._dialect, UnknownDatabaseDialect)
        assert DialectCode.UNKNOWN == manager._dialect_code
        assert manager._can_update is True


def test_query_for_dialect_cannot_update(mock_dialect, mock_conn, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    manager._dialect = mock_dialect

    assert mock_dialect == manager.query_for_dialect("", None, mock_conn, mock_driver_dialect)
    mock_dialect.dialect_update_candidates.assert_not_called()


def test_query_for_dialect_errors(mock_conn, mock_dialect, mock_candidate, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    manager._can_update = True
    mock_dialect.dialect_update_candidates = frozenset({mock_candidate})
    manager._dialect = mock_dialect

    with pytest.raises(AwsWrapperError):
        manager.query_for_dialect("", None, mock_conn, mock_driver_dialect)

    mock_dialect.dialect_update_candidates = frozenset()
    with pytest.raises(AwsWrapperError):
        manager.query_for_dialect("", None, mock_conn, mock_driver_dialect)


def test_query_for_dialect_no_update_candidates(mock_dialect, mock_conn, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    mock_dialect.dialect_update_candidates = None
    manager._can_update = True
    manager._dialect_code = DialectCode.PG
    manager._dialect = mock_dialect

    assert mock_dialect == manager.query_for_dialect("url", HostInfo("host"), mock_conn, mock_driver_dialect)
    assert DialectCode.PG == manager._known_endpoint_dialects.get("url")
    assert DialectCode.PG == manager._known_endpoint_dialects.get("host")


def test_query_for_dialect_pg(mock_conn, mock_cursor, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    manager._can_update = True
    manager._dialect = PgDatabaseDialect()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__iter__.return_value = [(True, True)]
    mock_cursor.fetch_one.return_value = (True,)

    result = manager.query_for_dialect("url", HostInfo("host"), mock_conn, mock_driver_dialect)
    assert isinstance(result, AuroraPgDialect)
    assert DialectCode.AURORA_PG == manager._known_endpoint_dialects.get("url")
    assert DialectCode.AURORA_PG == manager._known_endpoint_dialects.get("host")


def test_query_for_dialect_mysql(mock_conn, mock_cursor, mock_driver_dialect):
    manager = DatabaseDialectManager(Properties())
    manager._can_update = True
    manager._dialect = MysqlDatabaseDialect()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.__iter__.return_value = [("version_comment", "Source distribution")]
    mock_cursor.fetch_one.return_value = ("aurora_version", "3.0.0")

    result = manager.query_for_dialect("url", HostInfo("host"), mock_conn, mock_driver_dialect)
    assert isinstance(result, AuroraMysqlDialect)
    assert DialectCode.AURORA_MYSQL == manager._known_endpoint_dialects.get("url")
    assert DialectCode.AURORA_MYSQL == manager._known_endpoint_dialects.get("host")
