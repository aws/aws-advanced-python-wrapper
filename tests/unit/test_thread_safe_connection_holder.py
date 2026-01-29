#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  A copy of the License is located at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  or in the "license" file accompanying this file. This file is distributed
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#  express or implied. See the License for the specific language governing
#  permissions and limitations under the License.

import threading
import time
from unittest.mock import MagicMock

from aws_advanced_python_wrapper.utils.thread_safe_connection_holder import \
    ThreadSafeConnectionHolder


class TestThreadSafeConnectionHolder:
    def test_get_set(self):
        """Test basic get and set operations."""
        holder = ThreadSafeConnectionHolder()
        assert holder.get() is None

        mock_conn = MagicMock()
        holder.set(mock_conn, close_previous=False)
        assert holder.get() == mock_conn

    def test_set_closes_previous(self):
        """Test that set closes the previous connection when requested."""
        holder = ThreadSafeConnectionHolder()

        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()

        holder.set(mock_conn1, close_previous=False)
        holder.set(mock_conn2, close_previous=True)

        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_not_called()

    def test_set_does_not_close_previous_when_disabled(self):
        """Test that set doesn't close previous connection when close_previous=False."""
        holder = ThreadSafeConnectionHolder()

        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()

        holder.set(mock_conn1, close_previous=False)
        holder.set(mock_conn2, close_previous=False)

        mock_conn1.close.assert_not_called()
        mock_conn2.close.assert_not_called()

    def test_get_and_set(self):
        """Test get_and_set returns old value and sets new value."""
        holder = ThreadSafeConnectionHolder()

        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()

        holder.set(mock_conn1, close_previous=False)
        old_conn = holder.get_and_set(mock_conn2, close_previous=False)

        assert old_conn == mock_conn1
        assert holder.get() == mock_conn2

    def test_compare_and_set_success(self):
        """Test compare_and_set succeeds when expected value matches."""
        holder = ThreadSafeConnectionHolder()

        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()

        holder.set(mock_conn1, close_previous=False)
        result = holder.compare_and_set(mock_conn1, mock_conn2, close_previous=False)

        assert result is True
        assert holder.get() == mock_conn2

    def test_compare_and_set_failure(self):
        """Test compare_and_set fails when expected value doesn't match."""
        holder = ThreadSafeConnectionHolder()

        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_conn3 = MagicMock()

        holder.set(mock_conn1, close_previous=False)
        result = holder.compare_and_set(mock_conn2, mock_conn3, close_previous=False)

        assert result is False
        assert holder.get() == mock_conn1

    def test_clear(self):
        """Test clear removes and closes connection."""
        holder = ThreadSafeConnectionHolder()

        mock_conn = MagicMock()
        holder.set(mock_conn, close_previous=False)
        holder.clear()

        assert holder.get() is None
        mock_conn.close.assert_called_once()

    def test_use_connection(self):
        """Test use_connection safely executes function with connection."""
        holder = ThreadSafeConnectionHolder()

        mock_conn = MagicMock()
        mock_conn.cursor.return_value.execute.return_value = "result"
        holder.set(mock_conn, close_previous=False)

        result = holder.use_connection(lambda conn: conn.cursor().execute("SELECT 1"))

        assert result == "result"
        mock_conn.cursor.assert_called_once()

    def test_use_connection_with_none(self):
        """Test use_connection returns None when no connection is set."""
        holder = ThreadSafeConnectionHolder()

        result = holder.use_connection(lambda conn: conn.cursor())

        assert result is None

    def test_thread_safety_concurrent_set(self):
        """Test that concurrent set operations are thread-safe."""
        holder = ThreadSafeConnectionHolder()
        connections = [MagicMock() for _ in range(10)]

        def set_connection(conn):
            holder.set(conn, close_previous=False)
            time.sleep(0.001)

        threads = [threading.Thread(target=set_connection, args=(conn,)) for conn in connections]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # Should have one of the connections set
        final_conn = holder.get()
        assert final_conn in connections

    def test_thread_safety_race_condition(self):
        """Test that the race condition is prevented - connection can't be closed while in use."""
        holder = ThreadSafeConnectionHolder()
        mock_conn = MagicMock()
        holder.set(mock_conn, close_previous=False)

        results = []
        errors = []

        def use_connection():
            try:
                # This should be safe - connection won't be closed during execution
                result = holder.use_connection(lambda conn: (time.sleep(0.01), conn)[1])
                results.append(result)
            except Exception as e:
                errors.append(e)

        def close_connection():
            time.sleep(0.005)  # Let use_connection start first
            holder.clear()

        thread1 = threading.Thread(target=use_connection)
        thread2 = threading.Thread(target=close_connection)

        thread1.start()
        thread2.start()
        thread1.join()
        thread2.join()

        # Should have successfully used the connection
        assert len(results) == 1
        assert len(errors) == 0
        assert results[0] == mock_conn
