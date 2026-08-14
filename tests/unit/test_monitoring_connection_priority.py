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

from aws_advanced_python_wrapper.utils.monitoring_connection_priority import \
    MonitoringConnectionPriority as Priority


class TestFromValue:
    def test_known_values(self):
        assert Priority.from_value("strict-writer") is Priority.STRICT_WRITER
        assert Priority.from_value("strict-reader") is Priority.STRICT_READER
        assert Priority.from_value("writer-or-reader") is Priority.WRITER_OR_READER

    def test_case_insensitive(self):
        assert Priority.from_value("STRICT-WRITER") is Priority.STRICT_WRITER

    def test_invalid_and_none(self):
        assert Priority.from_value("invalid") is None
        assert Priority.from_value(None) is None


class TestParseList:
    def test_default_when_none(self):
        assert Priority.parse_list(None) == [Priority.STRICT_WRITER]

    def test_default_when_empty(self):
        assert Priority.parse_list("") == [Priority.STRICT_WRITER]

    def test_single_value(self):
        assert Priority.parse_list("strict-reader") == [Priority.STRICT_READER]

    def test_multiple_values_preserve_order(self):
        assert Priority.parse_list("strict-writer,strict-reader,writer-or-reader") == [
            Priority.STRICT_WRITER, Priority.STRICT_READER, Priority.WRITER_OR_READER]

    def test_with_spaces(self):
        assert Priority.parse_list(" strict-reader , writer-or-reader ") == [
            Priority.STRICT_READER, Priority.WRITER_OR_READER]

    def test_ignores_duplicates(self):
        assert Priority.parse_list("strict-writer,strict-writer,strict-reader") == [
            Priority.STRICT_WRITER, Priority.STRICT_READER]

    def test_ignores_invalid_values(self):
        assert Priority.parse_list("invalid,strict-reader,bad-value") == [Priority.STRICT_READER]

    def test_all_invalid_falls_back_to_default(self):
        assert Priority.parse_list("invalid,bad-value") == [Priority.STRICT_WRITER]


class TestIsSatisfiedBy:
    def test_strict_writer(self):
        assert Priority.STRICT_WRITER.is_satisfied_by(True) is True
        assert Priority.STRICT_WRITER.is_satisfied_by(False) is False

    def test_strict_reader(self):
        assert Priority.STRICT_READER.is_satisfied_by(True) is False
        assert Priority.STRICT_READER.is_satisfied_by(False) is True

    def test_writer_or_reader(self):
        assert Priority.WRITER_OR_READER.is_satisfied_by(True) is True
        assert Priority.WRITER_OR_READER.is_satisfied_by(False) is True
