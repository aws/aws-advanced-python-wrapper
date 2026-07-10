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

from __future__ import annotations

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.gdb_monitoring_connection_priority import \
    GdbMonitoringConnectionPriority as Priority
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

rds_utils = RdsUtils()

WRITER_EAST = HostInfo("instance1.cluster-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.WRITER)
READER_EAST = HostInfo("instance2.cluster-ro-xyz.us-east-1.rds.amazonaws.com", 5432, HostRole.READER)
READER_WEST = HostInfo("instance3.cluster-ro-xyz.us-west-2.rds.amazonaws.com", 5432, HostRole.READER)
WRITER_WEST = HostInfo("instance1.cluster-xyz.us-west-2.rds.amazonaws.com", 5432, HostRole.WRITER)


class TestFromValue:
    def test_strict_writer_primary(self):
        p = Priority.from_value("strict-writer-primary")
        assert p is not None
        assert p.required_role is HostRole.WRITER
        assert p.required_region is None
        assert p.require_primary is True
        assert p.require_secondary is False

    def test_strict_reader_primary(self):
        p = Priority.from_value("strict-reader-primary")
        assert p is not None
        assert p.required_role is HostRole.READER
        assert p.required_region is None
        assert p.require_primary is True
        assert p.require_secondary is False

    def test_strict_reader_secondary(self):
        p = Priority.from_value("strict-reader-secondary")
        assert p is not None
        assert p.required_role is HostRole.READER
        assert p.required_region is None
        assert p.require_primary is False
        assert p.require_secondary is True

    def test_strict_writer_region(self):
        p = Priority.from_value("strict-writer-us-east-1")
        assert p is not None
        assert p.required_role is HostRole.WRITER
        assert p.required_region == "us-east-1"
        assert p.require_primary is False
        assert p.require_secondary is False

    def test_strict_reader_region(self):
        p = Priority.from_value("strict-reader-us-west-2")
        assert p is not None
        assert p.required_role is HostRole.READER
        assert p.required_region == "us-west-2"

    def test_plain_region(self):
        p = Priority.from_value("us-east-1")
        assert p is not None
        assert p.required_role is None
        assert p.required_region == "us-east-1"
        assert p.require_primary is False
        assert p.require_secondary is False

    def test_null_and_empty(self):
        assert Priority.from_value(None) is None
        assert Priority.from_value("") is None
        assert Priority.from_value("  ") is None

    def test_invalid_prefix_no_suffix(self):
        assert Priority.from_value("strict-writer-") is None
        assert Priority.from_value("strict-reader-") is None

    def test_strict_writer_secondary_rejected(self):
        assert Priority.from_value("strict-writer-secondary") is None

    def test_typo_logs_and_becomes_region_literal(self, mocker):
        debug = mocker.patch(
            "aws_advanced_python_wrapper.utils.gdb_monitoring_connection_priority.logger.debug")
        # A typo does not look like an AWS region, so it logs a debug message
        # but is still accepted as a (never-matching) region literal.
        p = Priority.from_value("strict-wrtier-primary")
        assert p is not None
        assert p.required_role is None
        assert p.required_region == "strict-wrtier-primary"
        debug.assert_called_once_with(
            "GdbMonitoringConnectionHandler.UnrecognizedPriority", "strict-wrtier-primary")

    def test_valid_region_does_not_log(self, mocker):
        debug = mocker.patch(
            "aws_advanced_python_wrapper.utils.gdb_monitoring_connection_priority.logger.debug")
        assert Priority.from_value("us-east-1") is not None
        debug.assert_not_called()

    def test_prefixed_region_does_not_log(self, mocker):
        debug = mocker.patch(
            "aws_advanced_python_wrapper.utils.gdb_monitoring_connection_priority.logger.debug")
        # strict-writer-<region> / strict-reader-<region> take the prefix path
        # and never reach the region-literal fallback, so nothing is logged.
        assert Priority.from_value("strict-writer-us-east-1") is not None
        assert Priority.from_value("strict-reader-us-west-2") is not None
        debug.assert_not_called()


class TestParseList:
    def test_default(self):
        result = Priority.parse_list(None)
        assert len(result) == 1
        assert str(result[0]) == "strict-writer-primary"

    def test_multiple_values(self):
        result = Priority.parse_list("strict-writer-primary,strict-reader-us-east-1,us-west-2")
        assert [str(p) for p in result] == [
            "strict-writer-primary", "strict-reader-us-east-1", "us-west-2"]

    def test_skips_invalid(self):
        result = Priority.parse_list("strict-writer-,strict-reader-primary")
        assert len(result) == 1
        assert str(result[0]) == "strict-reader-primary"


class TestIsSatisfiedBy:
    def test_writer_in_primary_region(self):
        p = Priority.from_value("strict-writer-primary")
        assert p.is_satisfied_by(WRITER_EAST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(WRITER_EAST, "us-west-2", rds_utils) is False

    def test_reader_in_primary_region(self):
        p = Priority.from_value("strict-reader-primary")
        assert p.is_satisfied_by(READER_EAST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(READER_EAST, "us-west-2", rds_utils) is False

    def test_reader_in_secondary_region(self):
        p = Priority.from_value("strict-reader-secondary")
        # Primary is us-east-1, so us-west-2 reader is secondary.
        assert p.is_satisfied_by(READER_WEST, "us-east-1", rds_utils) is True
        # Primary is us-west-2, so us-west-2 reader is NOT secondary.
        assert p.is_satisfied_by(READER_WEST, "us-west-2", rds_utils) is False

    def test_writer_in_specific_region(self):
        p = Priority.from_value("strict-writer-us-east-1")
        assert p.is_satisfied_by(WRITER_EAST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(WRITER_WEST, "us-east-1", rds_utils) is False

    def test_reader_in_specific_region(self):
        p = Priority.from_value("strict-reader-us-west-2")
        assert p.is_satisfied_by(READER_WEST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(READER_EAST, "us-east-1", rds_utils) is False

    def test_any_node_in_region(self):
        p = Priority.from_value("us-east-1")
        assert p.is_satisfied_by(WRITER_EAST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(READER_EAST, "us-east-1", rds_utils) is True
        assert p.is_satisfied_by(WRITER_WEST, "us-east-1", rds_utils) is False

    def test_role_check_rejects_wrong_role(self):
        p = Priority.from_value("strict-writer-primary")
        assert p.is_satisfied_by(READER_EAST, "us-east-1", rds_utils) is False


class TestFindMatchingHost:
    def test_finds_first_match(self):
        p = Priority.from_value("strict-reader-us-west-2")
        hosts = [WRITER_EAST, READER_EAST, READER_WEST]
        assert p.find_matching_host(hosts, "us-east-1", rds_utils) == READER_WEST

    def test_returns_none_when_no_match(self):
        p = Priority.from_value("strict-writer-us-west-2")
        hosts = [WRITER_EAST, READER_EAST]
        assert p.find_matching_host(hosts, "us-east-1", rds_utils) is None

    def test_find_matching_hosts_returns_all(self):
        p = Priority.from_value("us-east-1")
        hosts = [WRITER_EAST, READER_EAST, READER_WEST]
        assert p.find_matching_hosts(hosts, "us-east-1", rds_utils) == [WRITER_EAST, READER_EAST]
