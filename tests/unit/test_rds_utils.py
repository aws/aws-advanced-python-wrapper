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

from aws_advanced_python_wrapper.utils.rdsutils import RdsUtils

us_east_region_cluster = "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
us_east_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com"
us_east_region_instance = "instance-test-name.XYZ.us-east-2.rds.amazonaws.com"
us_east_region_proxy = "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com"
us_east_region_custom_domain = "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com"
china_region_cluster = "database-test-name.cluster-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
china_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
china_region_instance = "instance-test-name.XYZ.cn-northwest-1.rds.amazonaws.com.cn"
china_region_proxy = "proxy-test-name.proxy-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
china_region_custom_domain = "custom-test-name.cluster-custom-XYZ.cn-northwest-1.rds.amazonaws.com.cn"


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster,
    us_east_region_cluster_read_only,
    china_region_cluster,
    china_region_cluster_read_only,
])
def test_is_rds_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_rds_cluster_dns(test_value) is True


@pytest.mark.parametrize("test_value", [
    us_east_region_instance,
    us_east_region_proxy,
    us_east_region_custom_domain,
    china_region_instance,
    china_region_proxy,
    china_region_custom_domain
])
def test_is_not_rds_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_rds_cluster_dns(test_value) is False


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster,
    us_east_region_cluster_read_only,
    us_east_region_instance,
    us_east_region_proxy,
    us_east_region_custom_domain,
    china_region_cluster,
    china_region_cluster_read_only,
    china_region_instance,
    china_region_proxy,
    china_region_custom_domain
])
def test_is_rds_dns(test_value):
    target = RdsUtils()
    assert target.is_rds_dns(test_value) is True


@pytest.mark.parametrize("expected, test_value", [
    ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_cluster),
    ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_cluster_read_only),
    ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_instance),
    ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_proxy),
    ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_custom_domain),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_cluster),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_cluster_read_only),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_instance),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_proxy),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_custom_domain)
])
def test_get_rds_instance_host_pattern(expected, test_value):
    target = RdsUtils()
    assert expected == target.get_rds_instance_host_pattern(test_value)


@pytest.mark.parametrize("expected, test_value", [
    ("us-east-2", us_east_region_cluster),
    ("us-east-2", us_east_region_cluster_read_only),
    ("us-east-2", us_east_region_instance),
    ("us-east-2", us_east_region_proxy),
    ("us-east-2", us_east_region_custom_domain),
    ("cn-northwest-1", china_region_cluster),
    ("cn-northwest-1", china_region_cluster_read_only),
    ("cn-northwest-1", china_region_instance),
    ("cn-northwest-1", china_region_proxy),
    ("cn-northwest-1", china_region_custom_domain)
])
def test_get_rds_region(expected, test_value):
    target = RdsUtils()
    assert expected == target.get_rds_region(test_value)


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster,
    china_region_cluster,
])
def test_is_writer_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_writer_cluster_dns(test_value) is True


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster_read_only,
    us_east_region_instance,
    us_east_region_proxy,
    us_east_region_custom_domain,
    china_region_cluster_read_only,
    china_region_instance,
    china_region_proxy,
    china_region_custom_domain
])
def test_is_not_writer_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_writer_cluster_dns(test_value) is False


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster_read_only,
    china_region_cluster_read_only,
])
def test_is_reader_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_reader_cluster_dns(test_value) is True


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster,
    us_east_region_instance,
    us_east_region_proxy,
    us_east_region_custom_domain,
    china_region_cluster,
    china_region_instance,
    china_region_proxy,
    china_region_custom_domain
])
def test_is_not_reader_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_reader_cluster_dns(test_value) is False


def test_get_rds_cluster_host_url():
    expected: str = "foo.cluster-xyz.us-west-1.rds.amazonaws.com"
    expected2: str = "foo-1.cluster-xyz.us-west-1.rds.amazonaws.com.cn"

    ro_endpoint: str = "foo.cluster-ro-xyz.us-west-1.rds.amazonaws.com"
    china_ro_endpoint: str = "foo-1.cluster-ro-xyz.us-west-1.rds.amazonaws.com.cn"

    target = RdsUtils()

    assert expected == target.get_rds_cluster_host_url(ro_endpoint)
    assert expected2 == target.get_rds_cluster_host_url(china_ro_endpoint)


@pytest.mark.parametrize(
    "host, expected_id",
    [pytest.param(us_east_region_instance, "instance-test-name"),
     pytest.param(china_region_instance, "instance-test-name")]
)
def test_get_instance_id(host: str, expected_id: str):
    target = RdsUtils()
    assert expected_id == target.get_instance_id(host)
