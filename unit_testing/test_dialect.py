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

from unittest import TestCase
from unittest.mock import MagicMock, patch

import pytest

from aws_wrapper.dialect import (AuroraMysqlDialect, AuroraPgDialect,
                                 DatabaseType, DialectCodes, DialectManager,
                                 MariaDbDialect, MysqlDialect, PgDialect,
                                 RdsMysqlDialect, RdsPgDialect, UnknownDialect)
from aws_wrapper.errors import AwsWrapperError
from aws_wrapper.hostinfo import HostInfo
from aws_wrapper.utils.properties import Properties, WrapperProperties


class TestDialect(TestCase):

    def test_pg_is_dialect(self):
        pg_dialect = PgDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_fetchone_row = MagicMock()

        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = mock_fetchone_row

        assert pg_dialect.is_dialect(mock_conn)

        mock_cursor.fetchone.return_value = None
        assert not pg_dialect.is_dialect(mock_conn)

    def test_mysql_is_dialect(self):
        mysql_dialect = MysqlDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        records = [("some_value", "some_value"), ("some_value", "mysql")]
        mock_cursor.__iter__.return_value = records

        assert mysql_dialect.is_dialect(mock_conn)

        records = [("some_value", "some_value"), ("some_value", "some_value")]
        mock_cursor.__iter__.return_value = records

        assert not mysql_dialect.is_dialect(mock_conn)

    def test_mariadb_is_dialect(self):
        mariadb_dialect = MariaDbDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        records = [("some_value", "some_value"), ("mariadb", "some_value")]
        mock_cursor.__iter__.return_value = records

        assert mariadb_dialect.is_dialect(mock_conn)

        records = [("some_value", "some_value"), ("some_value", "some_value")]
        mock_cursor.__iter__.return_value = records

        assert not mariadb_dialect.is_dialect(mock_conn)

    @patch('aws_wrapper.dialect.super')
    def test_rds_mysql_is_dialect(self, mock_super):
        rds_mysql_dialect = RdsMysqlDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_super().is_dialect.return_value = True

        records = [("some_value", "some_value"), ("some_value", "source distribution")]
        mock_cursor.__iter__.return_value = records

        assert rds_mysql_dialect.is_dialect(mock_conn)

        records = [("some_value", "some_value"), ("some_value", "some_value")]
        mock_cursor.__iter__.return_value = records

        assert not rds_mysql_dialect.is_dialect(mock_conn)

        mock_super().is_dialect.return_value = False

        assert not rds_mysql_dialect.is_dialect(mock_conn)

    def test_aurora_mysql_is_dialect(self):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None

        dialect = AuroraMysqlDialect()
        assert dialect.is_dialect(mock_conn) is False

        mock_cursor.fetchone.return_value = ('aurora_version', '3.0.0')
        assert dialect.is_dialect(mock_conn) is True

    @patch('aws_wrapper.dialect.super')
    def test_aurora_pg_is_dialect(self, mock_super):
        aurora_pg_dialect = AuroraPgDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
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

    @patch('aws_wrapper.dialect.super')
    def test_rds_pg_is_dialect(self, mock_super):
        rds_pg_dialect = RdsPgDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
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

    def test_get_dialect_custom_dialect(self):
        manager = DialectManager()
        mock_custom_dialect = MagicMock()
        manager._custom_dialect = mock_custom_dialect

        assert mock_custom_dialect == manager.get_dialect(Properties())

    def test_get_dialect_user_setting(self):
        manager = DialectManager()
        props = Properties({"host": "localhost", WrapperProperties.DIALECT.name: "custom"})

        with pytest.raises(AwsWrapperError):
            manager.get_dialect(props)

        props = Properties({"host": "localhost", WrapperProperties.DIALECT.name: "aurora-pg"})
        assert isinstance(manager.get_dialect(props), AuroraPgDialect)
        assert isinstance(manager._dialect, AuroraPgDialect)
        assert manager._dialect_code == DialectCodes.AURORA_PG

    def test_get_dialect_aurora_mysql(self):
        manager = DialectManager()
        props = Properties({"host": "my-database.cluster-xyz.us-east-2.rds.amazonaws.com"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.MYSQL):
            assert isinstance(manager.get_dialect(props), AuroraMysqlDialect)
            assert isinstance(manager._dialect, AuroraMysqlDialect)
            assert DialectCodes.AURORA_MYSQL == manager._dialect_code
            assert manager._can_update is False

    def test_get_dialect_rds_mysql(self):
        manager = DialectManager()
        props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.MYSQL):
            assert isinstance(manager.get_dialect(props), RdsMysqlDialect)
            assert isinstance(manager._dialect, RdsMysqlDialect)
            assert DialectCodes.RDS_MYSQL == manager._dialect_code
            assert manager._can_update is True

    def test_get_dialect_mysql(self):
        manager = DialectManager()
        props = Properties({"host": "localhost"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.MYSQL):
            assert isinstance(manager.get_dialect(props), MysqlDialect)
            assert isinstance(manager._dialect, MysqlDialect)
            assert DialectCodes.MYSQL == manager._dialect_code
            assert manager._can_update is True

    def test_get_dialect_aurora_pg(self):
        manager = DialectManager()
        props = Properties({"host": "my-database.cluster-xyz.us-east-2.rds.amazonaws.com"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.POSTGRES):
            assert isinstance(manager.get_dialect(props), AuroraPgDialect)
            assert isinstance(manager._dialect, AuroraPgDialect)
            assert DialectCodes.AURORA_PG == manager._dialect_code
            assert manager._can_update is False

    def test_get_dialect_mysql_pg(self):
        manager = DialectManager()
        props = Properties({"host": "instance-1.xyz.us-east-2.rds.amazonaws.com"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.POSTGRES):
            assert isinstance(manager.get_dialect(props), RdsPgDialect)
            assert isinstance(manager._dialect, RdsPgDialect)
            assert DialectCodes.RDS_PG == manager._dialect_code
            assert manager._can_update is True

    def test_get_dialect_pg(self):
        manager = DialectManager()
        props = Properties({"host": "localhost"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.POSTGRES):
            assert isinstance(manager.get_dialect(props), PgDialect)
            assert isinstance(manager._dialect, PgDialect)
            assert DialectCodes.PG == manager._dialect_code
            assert manager._can_update is True

    def test_get_dialect_mariadb(self):
        manager = DialectManager()
        props = Properties({"host": "localhost"})

        with patch.object(manager, '_get_database_type', return_value=DatabaseType.MARIADB):
            assert isinstance(manager.get_dialect(props), MariaDbDialect)
            assert isinstance(manager._dialect, MariaDbDialect)
            assert DialectCodes.MARIADB == manager._dialect_code
            assert manager._can_update is True

    def test_get_dialect_unknown_dialect(self):
        manager = DialectManager()
        props = Properties({"host": "localhost"})

        with patch.object(manager, '_get_database_type', return_value=None):
            assert isinstance(manager.get_dialect(props), UnknownDialect)
            assert isinstance(manager._dialect, UnknownDialect)
            assert DialectCodes.UNKNOWN == manager._dialect_code
            assert manager._can_update is True

    def test_query_for_dialect_cannot_update(self):
        manager = DialectManager()
        mock_dialect = MagicMock()
        manager._dialect = mock_dialect

        assert mock_dialect == manager.query_for_dialect("", None, MagicMock())
        mock_dialect.dialect_update_candidates.assert_not_called()

    def test_query_for_dialect_errors(self):
        manager = DialectManager()
        manager._can_update = True
        mock_dialect = MagicMock()
        mock_candidate = MagicMock(spec=DialectCodes)
        mock_dialect.dialect_update_candidates = frozenset({mock_candidate})
        manager._dialect = mock_dialect

        with pytest.raises(AwsWrapperError):
            manager.query_for_dialect("", None, MagicMock())

        mock_dialect.dialect_update_candidates = frozenset()
        with pytest.raises(AwsWrapperError):
            manager.query_for_dialect("", None, MagicMock())

    def test_query_for_dialect_no_update_candidates(self):
        manager = DialectManager()
        mock_dialect = MagicMock()
        mock_dialect.dialect_update_candidates = None
        manager._can_update = True
        manager._dialect_code = DialectCodes.MARIADB
        manager._dialect = mock_dialect

        assert mock_dialect == manager.query_for_dialect("url", HostInfo("host"), MagicMock())
        assert DialectCodes.MARIADB == manager._known_endpoint_dialects.get("url")
        assert DialectCodes.MARIADB == manager._known_endpoint_dialects.get("host")

    def test_query_for_dialect_pg(self):
        manager = DialectManager()
        manager._can_update = True
        manager._dialect = PgDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__iter__.return_value = [(True, True)]
        mock_cursor.fetch_one.return_value = (True,)

        result = manager.query_for_dialect("url", HostInfo("host"), mock_conn)
        assert isinstance(result, AuroraPgDialect)
        assert DialectCodes.AURORA_PG == manager._known_endpoint_dialects.get("url")
        assert DialectCodes.AURORA_PG == manager._known_endpoint_dialects.get("host")

    def test_query_for_dialect_mysql(self):
        manager = DialectManager()
        manager._can_update = True
        manager._dialect = MysqlDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.__iter__.return_value = [("version_comment", "Source distribution")]
        mock_cursor.fetch_one.return_value = ("aurora_version", "3.0.0")

        result = manager.query_for_dialect("url", HostInfo("host"), mock_conn)
        assert isinstance(result, AuroraMysqlDialect)
        assert DialectCodes.AURORA_MYSQL == manager._known_endpoint_dialects.get("url")
        assert DialectCodes.AURORA_MYSQL == manager._known_endpoint_dialects.get("host")

    def test_is_closed(self):
        mock_conn = MagicMock()
        mock_conn.is_connected.return_value = False
        dialect = MysqlDialect()

        assert dialect.is_closed(mock_conn) is True

        del mock_conn.is_connected
        with pytest.raises(AwsWrapperError):
            dialect.is_closed(mock_conn)

        dialect = PgDialect()
        mock_conn.closed = True

        assert dialect.is_closed(mock_conn) is True

        del mock_conn.closed
        with pytest.raises(AwsWrapperError):
            dialect.is_closed(mock_conn)

    def test_abort_connection(self):
        mock_conn = MagicMock()
        dialect = PgDialect()

        dialect.abort_connection(mock_conn)
        mock_conn.cancel.assert_called_once()

        del mock_conn.cancel
        with pytest.raises(AwsWrapperError):
            dialect.abort_connection(mock_conn)
