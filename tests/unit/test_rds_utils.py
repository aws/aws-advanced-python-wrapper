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

china_alt_region_cluster = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
china_alt_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
china_alt_region_instance = "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn"
china_alt_region_proxy = "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
china_alt_region_custom_domain = "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
china_alt_region_limitless_db_shard_group = "database-test-name.shardgrp-XYZ.cn-northwest-1.rds.amazonaws.com.cn"
extra_rds_china_path = "database-test-name.cluster-XYZ.rds.cn-northwest-1.rds.amazonaws.com.cn"
missing_cn_china_path = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com"
missing_region_china_path = "database-test-name.cluster-XYZ.rds.amazonaws.com.cn"

us_east_region_elb_url = "elb-name.elb.us-east-2.amazonaws.com"
us_isob_east_region_cluster = "database-test-name.cluster-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_isob_east_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_isob_east_region_instance = "instance-test-name.XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_isob_east_region_proxy = "proxy-test-name.proxy-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_isob_east_region_custom_domain = "custom-test-name.cluster-custom-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_isob_east_region_limitless_db_shard_group = "database-test-name.shardgrp-XYZ.rds.us-isob-east-1.sc2s.sgov.gov"
us_gov_east_region_cluster = "database-test-name.cluster-XYZ.rds.us-gov-east-1.amazonaws.com"

us_iso_east_region_cluster = "database-test-name.cluster-XYZ.rds.us-iso-east-1.c2s.ic.gov"
us_iso_east_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.rds.us-iso-east-1.c2s.ic.gov"
us_iso_east_region_instance = "instance-test-name.XYZ.rds.us-iso-east-1.c2s.ic.gov"
us_iso_east_region_proxy = "proxy-test-name.proxy-XYZ.rds.us-iso-east-1.c2s.ic.gov"
us_iso_east_region_custom_domain = "custom-test-name.cluster-custom-XYZ.rds.us-iso-east-1.c2s.ic.gov"

us_iso_east_region_limitless_db_shard_group = "database-test-name.shardgrp-XYZ.rds.us-iso-east-1.c2s.ic.gov"


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster,
    us_east_region_cluster_read_only,
    china_region_cluster,
    china_alt_region_cluster,
    china_region_cluster_read_only,
    china_alt_region_cluster_read_only,
    us_isob_east_region_cluster,
    us_isob_east_region_cluster_read_only,
    us_gov_east_region_cluster,
    us_iso_east_region_cluster,
    us_iso_east_region_cluster_read_only
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
    china_region_custom_domain,
    china_alt_region_instance,
    china_alt_region_proxy,
    china_alt_region_custom_domain,
    china_alt_region_limitless_db_shard_group,
    us_east_region_elb_url,
    us_isob_east_region_instance,
    us_isob_east_region_proxy,
    us_isob_east_region_limitless_db_shard_group,
    us_iso_east_region_instance,
    us_iso_east_region_proxy,
    us_iso_east_region_limitless_db_shard_group,
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
    china_region_custom_domain,
    china_alt_region_cluster,
    china_alt_region_cluster_read_only,
    china_alt_region_instance,
    china_alt_region_proxy,
    china_alt_region_custom_domain,
    china_alt_region_limitless_db_shard_group,
    us_isob_east_region_cluster,
    us_isob_east_region_cluster_read_only,
    us_isob_east_region_instance,
    us_isob_east_region_proxy,
    us_isob_east_region_custom_domain,
    us_isob_east_region_limitless_db_shard_group,
    us_gov_east_region_cluster,
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
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_region_custom_domain),
    ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_alt_region_cluster),
    ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_alt_region_cluster_read_only),
    ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_alt_region_instance),
    ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_alt_region_proxy),
    ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_alt_region_custom_domain),
    ("?.XYZ.cn-northwest-1.rds.amazonaws.com.cn", china_alt_region_limitless_db_shard_group),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_cluster),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_cluster_read_only),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_instance),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_proxy),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_custom_domain),
    ("?.XYZ.rds.us-isob-east-1.sc2s.sgov.gov", us_isob_east_region_limitless_db_shard_group),
    ("?.XYZ.rds.us-gov-east-1.amazonaws.com", us_gov_east_region_cluster),
])
def test_get_rds_instance_host_pattern(expected, test_value):
    target = RdsUtils()
    assert target.get_rds_instance_host_pattern(test_value) == expected


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
    ("cn-northwest-1", china_region_custom_domain),
    ("cn-northwest-1", china_alt_region_cluster),
    ("cn-northwest-1", china_alt_region_cluster_read_only),
    ("cn-northwest-1", china_alt_region_instance),
    ("cn-northwest-1", china_alt_region_proxy),
    ("cn-northwest-1", china_alt_region_custom_domain),
    ("cn-northwest-1", china_alt_region_limitless_db_shard_group),
    ("us-isob-east-1", us_isob_east_region_cluster),
    ("us-isob-east-1", us_isob_east_region_cluster_read_only),
    ("us-isob-east-1", us_isob_east_region_instance),
    ("us-isob-east-1", us_isob_east_region_proxy),
    ("us-isob-east-1", us_isob_east_region_custom_domain),
    ("us-isob-east-1", us_isob_east_region_limitless_db_shard_group),
    ("us-gov-east-1", us_gov_east_region_cluster),
])
def test_get_rds_region(expected, test_value):
    target = RdsUtils()
    assert target.get_rds_region(test_value) == expected


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
    china_region_custom_domain,
    china_alt_region_cluster_read_only,
    china_alt_region_instance,
    china_alt_region_proxy,
    china_alt_region_custom_domain,
    china_alt_region_limitless_db_shard_group,
    us_isob_east_region_cluster_read_only,
    us_isob_east_region_instance,
    us_isob_east_region_proxy,
    us_isob_east_region_custom_domain,
    us_isob_east_region_limitless_db_shard_group,
])
def test_is_not_writer_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_writer_cluster_dns(test_value) is False


@pytest.mark.parametrize("test_value", [
    us_east_region_cluster_read_only,
    china_region_cluster_read_only,
    us_isob_east_region_cluster_read_only,
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
    china_region_custom_domain,
    china_region_cluster,
    china_region_instance,
    china_region_proxy,
    china_region_custom_domain,
    china_alt_region_limitless_db_shard_group,
    us_isob_east_region_cluster,
    us_isob_east_region_instance,
    us_isob_east_region_proxy,
    us_isob_east_region_custom_domain,
    us_isob_east_region_limitless_db_shard_group,
    us_gov_east_region_cluster,
])
def test_is_not_reader_cluster_dns(test_value):
    target = RdsUtils()

    assert target.is_reader_cluster_dns(test_value) is False


def test_get_rds_cluster_host_url():
    expected: str = "foo.cluster-xyz.us-west-1.rds.amazonaws.com"
    expected2: str = "foo-1.cluster-xyz.us-west-1.rds.amazonaws.com.cn"
    expected_limitless: str = "foo.shardgrp-xyz.us-west-1.rds.amazonaws.com"

    ro_endpoint: str = "foo.cluster-ro-xyz.us-west-1.rds.amazonaws.com"
    china_ro_endpoint: str = "foo-1.cluster-ro-xyz.us-west-1.rds.amazonaws.com.cn"
    limitless_endpoint: str = "foo.shardgrp-xyz.us-west-1.rds.amazonaws.com"

    target = RdsUtils()

    assert target.get_rds_cluster_host_url(ro_endpoint) == expected
    assert target.get_rds_cluster_host_url(china_ro_endpoint) == expected2
    assert target.get_rds_cluster_host_url(limitless_endpoint) == expected_limitless


@pytest.mark.parametrize(
    "host, expected_id",
    [pytest.param(us_east_region_instance, "instance-test-name"),
     pytest.param(china_region_instance, "instance-test-name")]
)
def test_get_instance_id(host: str, expected_id: str):
    target = RdsUtils()
    assert target.get_instance_id(host) == expected_id


@pytest.mark.parametrize("expected, test_value", [
    ("database-test-name", us_east_region_cluster),
    ("database-test-name", us_east_region_cluster_read_only),
    (None, us_east_region_instance),
    ("proxy-test-name", us_east_region_proxy),
    ("custom-test-name", us_east_region_custom_domain),
    ("database-test-name", china_region_cluster),
    ("database-test-name", china_region_cluster_read_only),
    (None, china_region_instance),
    ("proxy-test-name", china_region_proxy),
    ("custom-test-name", china_region_custom_domain),
    ("database-test-name", china_alt_region_cluster),
    ("database-test-name", china_alt_region_cluster_read_only),
    (None, china_alt_region_instance),
    ("proxy-test-name", china_alt_region_proxy),
    ("custom-test-name", china_alt_region_custom_domain),
    ("database-test-name", china_alt_region_limitless_db_shard_group),
    ("database-test-name", us_isob_east_region_cluster),
    ("database-test-name", us_isob_east_region_cluster_read_only),
    (None, us_isob_east_region_instance),
    ("proxy-test-name", us_isob_east_region_proxy),
    ("custom-test-name", us_isob_east_region_custom_domain),
    ("database-test-name", us_isob_east_region_limitless_db_shard_group),
    ("database-test-name", us_gov_east_region_cluster),
])
def test_get_cluster_id(expected, test_value):
    target = RdsUtils()
    assert target.get_cluster_id(test_value) == expected
