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
administrators to run. The integration tests run it against real servers; these checks only read the files."""

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
def test_trigger_file_version_matches_guide(name: str, version: str) -> None:
    assert re.findall(r"'aws-advanced-python-wrapper kms_encryption trigger v(\d+)'", _read(name)) == [version]


@_FILES
def test_trigger_file_rejects_short_value(name: str) -> None:
    assert re.findall(r"(?i)length\((?:col_value|data)\) < (\d+)", _read(name)) == \
        [str(EncryptionService.MIN_ENCRYPTED_LENGTH)]


def test_mysql_routines_read_no_session_variables() -> None:
    # A session user variable (@name, also quoted as @'name', @"name" or @`name`, not a system variable's @@name) is one
    # the writing session could set.
    sql = re.sub(r"--[^\n]*|/\*.*?\*/", "", _read("trigger_mysql.sql"), flags=re.DOTALL)
    assert re.findall(r"(?<!@)@(?!@)", sql) == []


def test_mysql_file_reads_metadata_from_schema_token() -> None:
    sql = _read("trigger_mysql.sql")
    assert "SCHEMA_NAME.encryption_metadata" in sql
    assert "SCHEMA_NAME.key_storage" in sql


def test_pg_file_reads_metadata_from_schema_token() -> None:
    sql = _read("trigger_pg.sql")
    assert "SCHEMA_NAME.encryption_metadata" in sql
    assert "SCHEMA_NAME.key_storage" in sql


@_FILES
def test_schema_token_replace_leaves_no_hardcoded_schema(name: str) -> None:
    sql = _read(name).replace("SCHEMA_NAME", "other_schema")
    assert "SCHEMA_NAME" not in sql
    assert "other_schema.encryption_metadata" in sql
    assert "other_schema.key_storage" in sql
    # no schema left hardcoded outside the comments
    assert "encrypt." not in re.sub(r"--.*", "", sql)
