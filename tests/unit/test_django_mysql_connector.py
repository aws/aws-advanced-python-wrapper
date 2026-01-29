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

from unittest.mock import MagicMock, patch

import pytest


class TestDatabaseWrapper:
    """Unit tests for Django MySQL Connector DatabaseWrapper"""

    @pytest.fixture
    def database_wrapper(self):
        """Create a DatabaseWrapper instance with mocked dependencies"""
        with patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.base.DatabaseWrapper.__init__'):
            from aws_advanced_python_wrapper.django.backends.mysql_connector.base import \
                DatabaseWrapper
            wrapper = DatabaseWrapper.__new__(DatabaseWrapper)
            wrapper._read_only = False
            return wrapper

    def test_get_connection_params_extracts_read_only(self, database_wrapper):
        """Test that get_connection_params extracts and removes read_only parameter"""
        with patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.base.DatabaseWrapper.get_connection_params') as mock_super:
            mock_super.return_value = {
                'host': 'localhost',
                'read_only': True
            }

            result = database_wrapper.get_connection_params()

            assert database_wrapper._read_only is True
            assert 'read_only' not in result

    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.AwsWrapperConnection.connect')
    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.mysql.connector.Connect')
    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.base.DjangoMySQLConverter')
    def test_get_new_connection_adds_converter_and_creates_wrapper(self, mock_converter, mock_connector, mock_wrapper_connect, database_wrapper):
        """Test that get_new_connection adds converter_class and creates AwsWrapperConnection"""
        mock_conn = MagicMock()
        mock_wrapper_connect.return_value = mock_conn
        database_wrapper._read_only = False

        conn_params = {'host': 'localhost'}
        result = database_wrapper.get_new_connection(conn_params)

        assert 'converter_class' in conn_params
        assert conn_params['converter_class'] == mock_converter
        mock_wrapper_connect.assert_called_once_with(mock_connector, **conn_params)
        assert result == mock_conn

    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.AwsWrapperConnection.connect')
    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.mysql.connector.Connect')
    def test_get_new_connection_sets_read_only_when_true(self, mock_connector, mock_wrapper_connect, database_wrapper):
        """Test that get_new_connection sets read_only=True on connection when _read_only is True"""
        mock_conn = MagicMock()
        mock_wrapper_connect.return_value = mock_conn
        database_wrapper._read_only = True

        result = database_wrapper.get_new_connection({'host': 'localhost'})

        assert result.read_only is True

    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.AwsWrapperConnection.connect')
    @patch('aws_advanced_python_wrapper.django.backends.mysql_connector.base.mysql.connector.Connect')
    def test_get_new_connection_passes_wrapper_properties(self, mock_connector, mock_wrapper_connect, database_wrapper):
        """Test that get_new_connection passes AWS wrapper properties like plugins"""
        mock_conn = MagicMock()
        mock_wrapper_connect.return_value = mock_conn
        database_wrapper._read_only = False

        conn_params = {
            'host': 'localhost',
            'user': 'test_user',
            'password': 'test_password',
            'plugins': 'failover,aurora_connection_tracker',
            'failover_timeout': 60,
            'cluster_id': 'my-cluster'
        }

        database_wrapper.get_new_connection(conn_params)

        # Verify all parameters including AWS wrapper properties are passed to connect
        mock_wrapper_connect.assert_called_once()
        call_args = mock_wrapper_connect.call_args[1]
        assert call_args['host'] == 'localhost'
        assert call_args['user'] == 'test_user'
        assert call_args['password'] == 'test_password'
        assert call_args['plugins'] == 'failover,aurora_connection_tracker'
        assert call_args['failover_timeout'] == 60
        assert call_args['cluster_id'] == 'my-cluster'

    def test_mysql_version_parses_standard_version(self, database_wrapper):
        """Test mysql_version parses standard MySQL version"""
        database_wrapper.mysql_server_data = {'version': '8.0.32'}
        assert database_wrapper.mysql_version == (8, 0, 32)

    def test_mysql_version_parses_version_with_suffix(self, database_wrapper):
        """Test mysql_version parses version with suffix"""
        database_wrapper.mysql_server_data = {'version': '5.7.42-log'}
        assert database_wrapper.mysql_version == (5, 7, 42)

    def test_mysql_version_raises_exception_for_invalid_format(self, database_wrapper):
        """Test mysql_version raises exception for invalid version format"""
        database_wrapper.mysql_server_data = {'version': 'invalid-version'}

        with pytest.raises(Exception, match="Unable to determine MySQL version"):
            _ = database_wrapper.mysql_version

    def test_mysql_is_mariadb_detects_mariadb(self, database_wrapper):
        """Test mysql_is_mariadb detects MariaDB"""
        database_wrapper.mysql_server_data = {'version': '10.11.2-MariaDB'}
        assert database_wrapper.mysql_is_mariadb is True

    def test_mysql_is_mariadb_detects_mysql(self, database_wrapper):
        """Test mysql_is_mariadb detects MySQL"""
        database_wrapper.mysql_server_data = {'version': '8.0.32'}
        assert database_wrapper.mysql_is_mariadb is False

    def test_sql_mode_parses_comma_separated_modes(self, database_wrapper):
        """Test sql_mode parses comma-separated modes"""
        database_wrapper.mysql_server_data = {'sql_mode': 'MODE1,MODE2,MODE3'}
        assert database_wrapper.sql_mode == {'MODE1', 'MODE2', 'MODE3'}

    def test_sql_mode_handles_empty_values(self, database_wrapper):
        """Test sql_mode handles empty string and None"""
        database_wrapper.mysql_server_data = {'sql_mode': ''}
        assert database_wrapper.sql_mode == set()

        del database_wrapper.__dict__['sql_mode']

        database_wrapper.mysql_server_data = {'sql_mode': None}
        assert database_wrapper.sql_mode == set()
