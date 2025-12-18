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
from aws_advanced_python_wrapper.host_availability import HostAvailability
from aws_advanced_python_wrapper.host_selector import RoundRobinHostSelector
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


@pytest.fixture
def default_props():
    return Properties()


@pytest.fixture
def weighted_props():
    return Properties()


@pytest.fixture
def writer_host():
    return HostInfo("instance-0", 5432, HostRole.WRITER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host1() -> HostInfo:
    return HostInfo("instance-1", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host2():
    return HostInfo("instance-2", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host3():
    return HostInfo("instance-3", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def reader_host4():
    return HostInfo("instance-4", 5432, HostRole.READER, HostAvailability.AVAILABLE)


@pytest.fixture
def hosts_list_123(writer_host, reader_host2, reader_host3, reader_host1):
    return [writer_host, reader_host2, reader_host3, reader_host1]


@pytest.fixture
def hosts_list_1234(writer_host, reader_host4, reader_host2, reader_host3, reader_host1):
    return [writer_host, reader_host4, reader_host2, reader_host3, reader_host1]


@pytest.fixture
def hosts_list_13(writer_host, reader_host3, reader_host1):
    return [writer_host, reader_host3, reader_host1]


@pytest.fixture
def hosts_list_14(writer_host, reader_host4, reader_host1):
    return [writer_host, reader_host4, reader_host1]


@pytest.fixture
def hosts_list_23(writer_host, reader_host2, reader_host3):
    return [writer_host, reader_host2, reader_host3]


@pytest.fixture
def writer_hosts_list(writer_host):
    return [writer_host]


@pytest.fixture
def weighted_host_weights():
    host_weights: str = "instance-0:1," \
        + "instance-1:3," \
        + "instance-2:2," \
        + "instance-3:1"
    return host_weights


@pytest.fixture()
def round_robin_host_selector(weighted_host_weights, weighted_props):
    host_selector = RoundRobinHostSelector()
    weighted_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = weighted_host_weights
    yield host_selector
    host_selector.clear_cache()


def test_setup_empty_host(round_robin_host_selector, default_props, hosts_list_123):
    host_weights: str = "instance-0:1," \
        + ":3," \
        + "instance-2:2," \
        + "instance-3:3"
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = host_weights

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_setup_empty_weight(round_robin_host_selector, default_props, hosts_list_123):
    host_weights: str = "instance-0:1," \
        + "instance-1:," \
        + "instance-2:2," \
        + "instance-3:3"
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = host_weights

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props)


def test_setup_zero_weight(round_robin_host_selector, default_props, hosts_list_123):
    host_weights: str = "instance-0:1," \
        + "instance-1:0," \
        + "instance-2:2," \
        + "instance-3:3"
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = host_weights

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props)


def test_setup_zero_default_weight(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "0"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props)


def test_setup_bad_weight_format(round_robin_host_selector, default_props, hosts_list_123):
    host_weights: str = "instance-0:1," \
        + "instance-1:1:3," \
        + "instance-2:2," \
        + "instance-3:3"
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = host_weights

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_setup_float_weights(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "instance-0:1," \
        + "instance-1:1.123," \
        + "instance-2:2.456," \
        + "instance-3:3.789"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props)


def test_setup_float_default_weight(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "1.123"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_setup_negative_weight(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "instance-0:1," \
        + "instance-1:-1," \
        + "instance-2:-2," \
        + "instance-3:-3"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props)


def test_setup_negative_default_weight(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "-1"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_setup_parse_weight_error(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "instance-0:1,instance-1:1a"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_setup_parse_default_weight_error(round_robin_host_selector, default_props, hosts_list_123):
    default_props[WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.name] = "1a"

    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_get_host_no_readers(round_robin_host_selector, default_props, writer_hosts_list):
    with pytest.raises(AwsWrapperError):
        round_robin_host_selector.get_host(writer_hosts_list, HostRole.READER, default_props)


def test_get_host(round_robin_host_selector, default_props, reader_host1, reader_host2, reader_host3, hosts_list_123):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_get_host_null_properties(round_robin_host_selector, reader_host1, reader_host2, reader_host3, hosts_list_123):
    props = None

    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, props).host


def test_get_host_cache_entry_expired(round_robin_host_selector, default_props, reader_host1, reader_host2, hosts_list_123):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host

    round_robin_host_selector.clear_cache()

    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host


def test_get_host_scale_down(round_robin_host_selector, default_props, reader_host1, reader_host3, hosts_list_123, hosts_list_13):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_13, HostRole.READER, default_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_13, HostRole.READER, default_props).host


def test_get_host_last_host_not_in_hosts_list(round_robin_host_selector, default_props, reader_host1, reader_host2, reader_host3, hosts_list_123,
                                              hosts_list_13):

    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_13, HostRole.READER, default_props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_13, HostRole.READER, default_props).host


def test_get_host_scale_up(round_robin_host_selector, default_props, reader_host1, reader_host2, reader_host3, reader_host4, hosts_list_123,
                           hosts_list_1234):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, default_props).host
    assert reader_host4.host == round_robin_host_selector.get_host(hosts_list_1234, HostRole.READER, default_props).host


def test_get_host_all_hosts_changed(round_robin_host_selector, default_props, reader_host1, reader_host2, reader_host4, hosts_list_14,
                                    hosts_list_23):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_14, HostRole.READER, default_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_23, HostRole.READER, default_props).host
    assert reader_host4.host == round_robin_host_selector.get_host(hosts_list_14, HostRole.READER, default_props).host


def test_get_host_weighted(round_robin_host_selector, hosts_list_123, weighted_props, reader_host1, reader_host2, reader_host3):
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host2.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host3.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
    assert reader_host1.host == round_robin_host_selector.get_host(hosts_list_123, HostRole.READER, weighted_props).host
