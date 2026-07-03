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

import importlib

import pytest

from aws_advanced_python_wrapper import _dbapi

PEP249_NAMES = (
    "Warning", "Error", "InterfaceError", "DatabaseError",
    "DataError", "OperationalError", "IntegrityError",
    "InternalError", "ProgrammingError", "NotSupportedError",
    "Date", "Time", "Timestamp",
    "DateFromTicks", "TimeFromTicks", "TimestampFromTicks",
    "Binary", "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    "apilevel", "threadsafety", "paramstyle",
)


def test_install_populates_target_namespace_without_connect():
    ns = {}
    _dbapi.install(ns)
    for name in PEP249_NAMES:
        assert name in ns, f"missing {name}"
    assert "connect" not in ns
    assert set(ns["__all__"]) == set(PEP249_NAMES)


def test_install_sets_connect_when_provided():
    def sentinel(*a, **k):
        pass

    ns = {}
    _dbapi.install(ns, connect=sentinel)
    assert ns["connect"] is sentinel
    assert "connect" in ns["__all__"]


def test_apilevel_and_paramstyle_values():
    ns = {}
    _dbapi.install(ns)
    assert ns["apilevel"] == "2.0"
    assert ns["threadsafety"] == 2
    assert ns["paramstyle"] == "pyformat"


def test_binary_constructor_returns_bytes():
    ns = {}
    _dbapi.install(ns)
    assert ns["Binary"](b"x") == b"x"
    assert isinstance(ns["Binary"](b"x"), bytes)


def test_date_time_timestamp_aliases():
    import datetime
    ns = {}
    _dbapi.install(ns)
    assert ns["Date"] is datetime.date
    assert ns["Time"] is datetime.time
    assert ns["Timestamp"] is datetime.datetime


def test_type_singletons_compare_equal_to_member_codes():
    ns = {}
    _dbapi.install(ns)
    assert 25 in ns["STRING"]
    assert 17 in ns["BINARY"]
    assert 20 in ns["NUMBER"]
    assert 1082 in ns["DATETIME"]
    assert 26 in ns["ROWID"]
    assert ns["STRING"] == 25
    assert ns["STRING"] != 99999


def test_exception_hierarchy_roots():
    ns = {}
    _dbapi.install(ns)
    assert issubclass(ns["Warning"], Exception)
    assert issubclass(ns["Error"], Exception)
    for sub in ("InterfaceError", "DatabaseError"):
        assert issubclass(ns[sub], ns["Error"])
    for sub in ("DataError", "OperationalError", "IntegrityError",
                "InternalError", "ProgrammingError", "NotSupportedError"):
        assert issubclass(ns[sub], ns["DatabaseError"])


@pytest.mark.parametrize("module_name", [
    "aws_advanced_python_wrapper",
    "aws_advanced_python_wrapper.psycopg",
    "aws_advanced_python_wrapper.mysql_connector",
])
@pytest.mark.parametrize("attr_name", PEP249_NAMES + ("connect",))
def test_module_exports_pep249_attr(module_name, attr_name):
    mod = importlib.import_module(module_name)
    assert hasattr(mod, attr_name), f"{module_name} missing {attr_name}"


def test_top_level_connect_is_awswrapperconnection_connect():
    import aws_advanced_python_wrapper as aaw
    from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
    assert aaw.connect is AwsWrapperConnection.connect
