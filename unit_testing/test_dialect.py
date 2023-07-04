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
from unittest.mock import MagicMock

from aws_wrapper.dialect import (MariaDbDialect, MySqlDialect,
                                 PgDialect, RdsMySqlDialect)


class TestDialect(TestCase):

    def test_pg_dialect_is_dialect(self):
        pg_dialect = PgDialect()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_fetchone_row = MagicMock()

        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = mock_fetchone_row

        assert pg_dialect.is_dialect(mock_conn)

        mock_cursor.fetchone.return_value = None
        assert not pg_dialect.is_dialect(mock_conn)

    def test_mysql_dialect_is_dialect(self):
        mysql_dialect = MySqlDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        records = [["some_value", "some_value"], ["some_value", "mysql"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert mysql_dialect.is_dialect(mock_conn)

        records = [["some_value", "some_value"], ["some_value", "some_value"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert not mysql_dialect.is_dialect(mock_conn)

    def test_mariadb_dialect_is_dialect(self):
        mariadb_dialect = MariaDbDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        records = [["some_value", "some_value"], ["some_value", "mariadb"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert mariadb_dialect.is_dialect(mock_conn)

        records = [["some_value", "some_value"], ["some_value", "some_value"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert not mariadb_dialect.is_dialect(mock_conn)

    def test_rds_mysql_dialect_is_dialect(self):
        rds_mysql_dialect = RdsMySqlDialect()
        mock_conn = MagicMock()

        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        records = [["some_value", "some_value"], ["some_value", "Source distribution"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert rds_mysql_dialect.is_dialect(mock_conn)

        records = [["some_value", "some_value"], ["some_value", "some_value"]]
        mock_cursor.__iter__.return_value = records.__iter__()

        assert not rds_mysql_dialect.is_dialect(mock_conn)
