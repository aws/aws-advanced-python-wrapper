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

"""The trigger SQL in ``aws_advanced_python_wrapper/encryption/sql``, which the KMS Encryption Plugin guide tells
administrators to run. The live module runs it against real servers; these checks only read the files."""

import re
from importlib.resources import files

import pytest

from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService

_SQL = files("aws_advanced_python_wrapper.encryption") / "sql"
_FILES = pytest.mark.parametrize("name", ["trigger_pg.sql", "trigger_mysql.sql"])


def _read(name: str) -> str:
    return (_SQL / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name, version", [("trigger_pg.sql", "2"), ("trigger_mysql.sql", "1")])
def test_each_file_records_the_version_the_guide_describes(name: str, version: str) -> None:
    assert re.findall(r"'aws-advanced-python-wrapper kms_encryption trigger v(\d+)'", _read(name)) == [version]


@_FILES
def test_each_file_rejects_a_value_shorter_than_the_smallest_payload(name: str) -> None:
    assert re.findall(r"(?i)length\((?:col_value|data)\) < (\d+)", _read(name)) == \
        [str(EncryptionService.MIN_ENCRYPTED_LENGTH)]


def test_the_mysql_file_reads_the_metadata_from_the_schema_the_guide_says_to_replace() -> None:
    sql = _read("trigger_mysql.sql")
    assert "SCHEMA_NAME.encryption_metadata" in sql
    assert "SCHEMA_NAME.key_storage" in sql


def test_the_pg_file_reads_the_metadata_from_the_default_schema() -> None:
    sql = _read("trigger_pg.sql")
    assert "encrypt.encryption_metadata" in sql
    assert "encrypt.key_storage" in sql
