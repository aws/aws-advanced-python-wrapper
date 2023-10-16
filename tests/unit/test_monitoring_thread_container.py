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

import pytest

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.host_monitoring_plugin import \
    MonitoringThreadContainer


@pytest.fixture
def container(mock_executor):
    container = MonitoringThreadContainer()
    MonitoringThreadContainer._executor = mock_executor
    return container


@pytest.fixture
def mock_executor(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_future(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_monitor1(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = False
    return monitor


@pytest.fixture
def mock_monitor2(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = False
    return monitor


@pytest.fixture
def mock_stopped_monitor(mocker):
    monitor = mocker.MagicMock()
    monitor.is_stopped = True
    return monitor


@pytest.fixture
def mock_monitor_supplier(mocker, mock_monitor1, mock_monitor2):
    supplier = mocker.MagicMock()
    supplier.side_effect = [mock_monitor1, mock_monitor2]
    return supplier


@pytest.fixture(autouse=True)
def release_container():
    yield
    while MonitoringThreadContainer._instance is not None:
        MonitoringThreadContainer.release_instance()


def test_get_or_create_monitor__monitor_created(
        container, mock_monitor_supplier, mock_stopped_monitor, mock_monitor1, mock_executor, mock_future):
    container._available_monitors.put(mock_stopped_monitor)
    container._tasks_map.put_if_absent(mock_stopped_monitor, mock_future)

    result = container.get_or_create_monitor(frozenset({"alias-1", "alias-2"}), mock_monitor_supplier)
    assert mock_monitor1 == result
    assert container._tasks_map.get(mock_stopped_monitor) is None
    mock_future.cancel.assert_called_once()
    mock_monitor_supplier.assert_called_once()
    mock_executor.submit.assert_called_once_with(mock_monitor1.run)
    assert mock_monitor1 == container._monitor_map.get("alias-1")
    assert mock_monitor1 == container._monitor_map.get("alias-2")


def test_get_or_create_monitor__from_available_monitors(
        container, mock_monitor_supplier, mock_monitor1, mock_executor, mock_future):
    container._available_monitors.put(mock_monitor1)
    container._tasks_map.put_if_absent(mock_monitor1, mock_future)

    result = container.get_or_create_monitor(frozenset({"alias-1", "alias-2"}), mock_monitor_supplier)
    assert mock_monitor1 == result
    mock_future.cancel.assert_not_called()
    mock_monitor_supplier.assert_not_called()
    mock_executor.submit.assert_not_called()
    assert mock_monitor1 == container._monitor_map.get("alias-1")
    assert mock_monitor1 == container._monitor_map.get("alias-2")


def test_get_or_create_monitor__from_monitor_map(container, mock_monitor1):
    container._monitor_map.put_if_absent("alias-2", mock_monitor1)

    result = container.get_or_create_monitor(frozenset({"alias-1", "alias-2"}), mock_monitor_supplier)
    assert mock_monitor1 == result
    assert mock_monitor1 == container._monitor_map.get("alias-1")


def test_get_or_create_monitor__shared_aliases(container, mock_monitor_supplier, mock_monitor1):
    host_aliases1 = frozenset({"host-1", "host-2"})
    host_aliases2 = frozenset({"host-2"})

    aliases1_monitor = container.get_or_create_monitor(host_aliases1, mock_monitor_supplier)
    aliases2_monitor = container.get_or_create_monitor(host_aliases2, mock_monitor_supplier)
    assert mock_monitor1 == aliases1_monitor
    assert aliases1_monitor == aliases2_monitor
    mock_monitor_supplier.assert_called_once()


def test_get_or_create_monitor__separate_aliases(container, mock_monitor_supplier, mock_monitor1, mock_monitor2):
    host_aliases1 = frozenset({"host-1"})
    host_aliases2 = frozenset({"host-2"})

    aliases1_monitor = container.get_or_create_monitor(host_aliases1, mock_monitor_supplier)
    aliases1_monitor_second_call = container.get_or_create_monitor(host_aliases1, mock_monitor_supplier)
    assert mock_monitor1 == aliases1_monitor
    assert aliases1_monitor == aliases1_monitor_second_call
    mock_monitor_supplier.assert_called_once()

    aliases2_monitor = container.get_or_create_monitor(host_aliases2, mock_monitor_supplier)
    assert mock_monitor2 == aliases2_monitor
    assert aliases2_monitor != aliases1_monitor


def test_get_or_create_monitor__aliases_intersection(container, mock_monitor_supplier, mock_monitor1):
    host_aliases1 = frozenset({"host-1"})
    host_aliases2 = frozenset({"host-1", "host-2"})
    host_aliases3 = frozenset({"host-2"})

    aliases1_monitor = container.get_or_create_monitor(host_aliases1, mock_monitor_supplier)
    aliases2_monitor = container.get_or_create_monitor(host_aliases2, mock_monitor_supplier)
    aliases3_monitor = container.get_or_create_monitor(host_aliases3, mock_monitor_supplier)

    assert mock_monitor1 == aliases1_monitor
    assert aliases1_monitor == aliases2_monitor
    assert aliases3_monitor == aliases1_monitor
    mock_monitor_supplier.assert_called_once()


def test_get_or_create_monitor__empty_aliases(container, mock_monitor_supplier):
    with pytest.raises(AwsWrapperError):
        container.get_or_create_monitor(frozenset(), mock_monitor_supplier)


def test_get_or_create_monitor__null_monitor(container, mock_monitor_supplier):
    mock_monitor_supplier.side_effect = None
    mock_monitor_supplier.return_value = None
    with pytest.raises(AwsWrapperError):
        container.get_or_create_monitor(frozenset({"alias-1"}), mock_monitor_supplier)


def test_reset_resource(mock_monitor1, mock_monitor2, container):
    container._monitor_map.put_if_absent("alias-1", mock_monitor1)
    container._monitor_map.put_if_absent("alias-2", mock_monitor2)

    container.reset_resource(mock_monitor2)
    assert mock_monitor1 == container._monitor_map.get("alias-1")
    assert container._monitor_map.get("alias-2") is None
    assert mock_monitor2 == container._available_monitors.get()


def test_release_monitor(mocker, mock_monitor1, mock_monitor2, container):
    container._monitor_map.put_if_absent("alias-1", mock_monitor1)
    container._monitor_map.put_if_absent("alias-2", mock_monitor2)
    mock_future_1 = mocker.MagicMock()
    mock_future_2 = mocker.MagicMock()
    container._tasks_map.put_if_absent(mock_monitor1, mock_future_1)
    container._tasks_map.put_if_absent(mock_monitor2, mock_future_2)

    container.release_monitor(mock_monitor2)
    assert container._monitor_map.get("alias-1")
    assert container._monitor_map.get("alias-2") is None
    assert mock_future_1 == container._tasks_map.get(mock_monitor1)
    assert container._tasks_map.get(mock_monitor2) is None
    mock_future_1.cancel.assert_not_called()
    mock_future_2.cancel.assert_called_once()


def test_release_instance(mocker, container, mock_monitor1, mock_future):
    container._monitor_map.put_if_absent("alias-1", mock_monitor1)
    container._tasks_map.put_if_absent(mock_monitor1, mock_future)
    mock_future.done.return_value = False
    mock_future.cancelled.return_value = False
    spy = mocker.spy(container._instance, "_release_resources")

    container2 = MonitoringThreadContainer()
    assert container2 is container
    assert 2 == container._usage_count.get()

    container2.release_instance()
    spy.assert_not_called()

    container.release_instance()
    assert 0 == len(container._monitor_map)
    assert 0 == len(container._tasks_map)
    mock_future.cancel.assert_called_once()
    assert MonitoringThreadContainer._instance is None
    assert 0 == MonitoringThreadContainer._usage_count.get()
