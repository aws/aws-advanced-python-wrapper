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

from aws_advanced_python_wrapper.driver_dialect import DriverDialect
from aws_advanced_python_wrapper.encryption.sql_tokenizer import (ParamStyle,
                                                                  tokenize)
from aws_advanced_python_wrapper.errors import UnsupportedOperationError
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture
def dialect():
    return DriverDialect(Properties())


def test_upsert_clause_unsupported(dialect):
    with pytest.raises(UnsupportedOperationError):
        dialect.upsert_clause(["id"], ["name"])


def test_insert_returning_id_unsupported(dialect, mocker):
    with pytest.raises(UnsupportedOperationError):
        dialect.insert_returning_id(mocker.MagicMock(), "INSERT INTO t (name) VALUES (%s)", ("a",))


def test_analyzable_sql_bytes_runs_unanalyzed(dialect, mocker):
    assert dialect.analyzable_sql(b"SELECT 1", mocker.MagicMock()) == (None, None)


@pytest.mark.parametrize("params, style", [
    (None, ParamStyle.NONE), ({}, ParamStyle.NAMED), ([], ParamStyle.POSITIONAL), ((), ParamStyle.POSITIONAL)])
def test_param_style_of_any_parameters_binds(params, style):
    assert ParamStyle.of(params, DriverDialect) is style


def test_tokenize_without_token_pattern_raises():
    with pytest.raises(UnsupportedOperationError, match="DriverDialect"):
        tokenize("SELECT 1", DriverDialect, ParamStyle.NONE)
