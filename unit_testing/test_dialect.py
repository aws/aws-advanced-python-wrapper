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

from aws_wrapper.dialect import (AuroraPgDialect, MariaDbDialect, MysqlDialect,
                                 PgDialect, RdsMysqlDialect, RdsPgDialect)


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
