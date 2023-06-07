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

from parameterized import parameterized

from aws_wrapper.utils.rdsutils import RdsUtils


class TestRdsUtils(TestCase):
    us_east_region_cluster = "database-test-name.cluster-XYZ.us-east-2.rds.amazonaws.com"
    us_east_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com"
    us_east_region_instance = "instance-test-name.XYZ.us-east-2.rds.amazonaws.com"
    us_east_region_proxy = "proxy-test-name.proxy-XYZ.us-east-2.rds.amazonaws.com"
    us_east_region_custom_domain = "custom-test-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com"
    china_region_cluster = "database-test-name.cluster-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
    china_region_cluster_read_only = "database-test-name.cluster-ro-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
    china_region_instance = "instance-test-name.XYZ.rds.cn-northwest-1.amazonaws.com.cn"
    china_region_proxy = "proxy-test-name.proxy-XYZ.rds.cn-northwest-1.amazonaws.com.cn"
    china_region_custom_domain = "custom-test-name.cluster-custom-XYZ.rds.cn-northwest-1.amazonaws.com.cn"

    @parameterized.expand([
        us_east_region_cluster,
        us_east_region_cluster_read_only,
        china_region_cluster,
        china_region_cluster_read_only,
    ])
    def test_is_rds_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertTrue(target.is_rds_cluster_dns(test_value))

    @parameterized.expand([
        us_east_region_instance,
        us_east_region_proxy,
        us_east_region_custom_domain,
        china_region_instance,
        china_region_proxy,
        china_region_custom_domain
    ])
    def test_is_not_rds_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertFalse(target.is_rds_cluster_dns(test_value))

    @parameterized.expand([
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
    def test_is_rds_dns(self, test_value):
        target = RdsUtils()
        self.assertTrue(target.is_rds_dns(test_value))

    @parameterized.expand([
        ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_cluster),
        ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_cluster_read_only),
        ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_instance),
        ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_proxy),
        ("?.XYZ.us-east-2.rds.amazonaws.com", us_east_region_custom_domain),
        ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_region_cluster),
        ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_region_cluster_read_only),
        ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_region_instance),
        ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_region_proxy),
        ("?.XYZ.rds.cn-northwest-1.amazonaws.com.cn", china_region_custom_domain)
    ])
    def test_get_rds_instance_host_pattern(self, expected, test_value):
        target = RdsUtils()
        self.assertEqual(expected, target.get_rds_instance_host_pattern(test_value))

    @parameterized.expand([
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
    def test_get_rds_region(self, expected, test_value):
        target = RdsUtils()
        self.assertEqual(expected, target.get_rds_region(test_value))

    @parameterized.expand([
        us_east_region_cluster,
        china_region_cluster,
    ])
    def test_is_writer_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertTrue(target.is_writer_cluster_dns(test_value))

    @parameterized.expand([
        us_east_region_cluster_read_only,
        us_east_region_instance,
        us_east_region_proxy,
        us_east_region_custom_domain,
        china_region_cluster_read_only,
        china_region_instance,
        china_region_proxy,
        china_region_custom_domain
    ])
    def test_is_not_writer_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertFalse(target.is_writer_cluster_dns(test_value))

    @parameterized.expand([
        us_east_region_cluster_read_only,
        china_region_cluster_read_only,
    ])
    def test_is_reader_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertTrue(target.is_reader_cluster_dns(test_value))

    @parameterized.expand([
        us_east_region_cluster,
        us_east_region_instance,
        us_east_region_proxy,
        us_east_region_custom_domain,
        china_region_cluster,
        china_region_instance,
        china_region_proxy,
        china_region_custom_domain
    ])
    def test_is_not_reader_cluster_dns(self, test_value):
        target = RdsUtils()

        self.assertFalse(target.is_reader_cluster_dns(test_value))
