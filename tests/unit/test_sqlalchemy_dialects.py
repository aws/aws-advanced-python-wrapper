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
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.mysql.mysqlconnector import \
    MySQLDialect_mysqlconnector
from sqlalchemy.dialects.postgresql.psycopg import PGDialect_psycopg
from sqlalchemy.engine.url import make_url

import aws_advanced_python_wrapper.mysql_connector as wrapper_mysql
import aws_advanced_python_wrapper.psycopg as wrapper_psycopg
from aws_advanced_python_wrapper.sqlalchemy_dialects.mysql import \
    AwsWrapperMySQLConnectorDialect
from aws_advanced_python_wrapper.sqlalchemy_dialects.pg import \
    AwsWrapperPGPsycopgDialect


def test_pg_dialect_subclasses_pgdialect_psycopg():
    assert issubclass(AwsWrapperPGPsycopgDialect, PGDialect_psycopg)


def test_pg_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperPGPsycopgDialect.import_dbapi() is wrapper_psycopg


def test_pg_dialect_driver_attr():
    assert AwsWrapperPGPsycopgDialect.driver == "aws_wrapper_psycopg"


def test_mysql_dialect_subclasses_mysqldialect_mysqlconnector():
    assert issubclass(AwsWrapperMySQLConnectorDialect, MySQLDialect_mysqlconnector)


def test_mysql_dialect_import_dbapi_returns_wrapper_submodule():
    assert AwsWrapperMySQLConnectorDialect.import_dbapi() is wrapper_mysql


def test_mysql_dialect_driver_attr():
    assert AwsWrapperMySQLConnectorDialect.driver == "aws_wrapper_mysqlconnector"


@pytest.mark.parametrize(
    "dialect_cls", [AwsWrapperMySQLConnectorDialect, AwsWrapperPGPsycopgDialect])
def test_is_disconnect_handles_full_failover_error_family(dialect_cls):
    # Regression: is_disconnect must classify the ENTIRE FailoverError family
    # without probing driver-specific attrs (errno/args[0]). A mid-transaction
    # failover raises TransactionResolutionUnknownError, which carries no
    # ``errno`` -- the old code only special-cased FailoverSuccessError /
    # FailoverFailedError and fell through to the upstream errno probe, raising
    # "AttributeError: 'TransactionResolutionUnknownError' object has no
    # attribute 'errno'" (MySQL SQLAlchemy failover tests, env-3/env-4).
    from aws_advanced_python_wrapper.errors import (
        FailoverFailedError, FailoverSuccessError,
        TransactionResolutionUnknownError)
    d = dialect_cls.__new__(dialect_cls)  # avoid full dialect __init__/dbapi
    # Reconnected-to-new-writer signals -> not a disconnect (slot still valid).
    assert d.is_disconnect(FailoverSuccessError("x"), None, None) is False
    assert d.is_disconnect(
        TransactionResolutionUnknownError("x"), None, None) is False
    # No usable connection -> disconnect (SA invalidates the slot).
    assert d.is_disconnect(FailoverFailedError("x"), None, None) is True


# Registration uses the SA <dialect>.<driver> convention: the wrapper plugs in
# as a driver under the stock ``postgresql`` / ``mysql`` dialects, so the URL is
# ``postgresql+aws_wrapper_psycopg`` / ``mysql+aws_wrapper_mysqlconnector``.
def test_registry_resolves_postgresql_aws_wrapper_psycopg():
    cls = registry.load("postgresql.aws_wrapper_psycopg")
    assert cls is AwsWrapperPGPsycopgDialect


def test_registry_resolves_mysql_aws_wrapper_mysqlconnector():
    cls = registry.load("mysql.aws_wrapper_mysqlconnector")
    assert cls is AwsWrapperMySQLConnectorDialect


def test_url_get_dialect_pg():
    url = make_url(
        "postgresql+aws_wrapper_psycopg://u:p@h:5432/db?wrapper_dialect=aurora-pg"
    )
    assert url.get_dialect() is AwsWrapperPGPsycopgDialect


def test_url_get_dialect_mysql():
    url = make_url(
        "mysql+aws_wrapper_mysqlconnector://u:p@h:3306/db?wrapper_dialect=aurora-mysql"
    )
    assert url.get_dialect() is AwsWrapperMySQLConnectorDialect


def test_url_query_args_flow_through_to_wrapper_connect(mocker):
    """URL query args must become kwargs on AwsWrapperConnection.connect."""
    from aws_advanced_python_wrapper.wrapper import AwsWrapperConnection
    mock_connect = mocker.patch.object(
        AwsWrapperConnection, "connect", return_value=mocker.MagicMock(),
    )
    # NOTE: URL uses `wrapper_plugins` instead of `plugins` because SA's
    # create_engine intercepts `plugins` as its own engine-plugin loader.
    # Our dialect's create_connect_args translates wrapper_plugins → plugins
    # before handing kwargs to the DBAPI.
    engine = create_engine(
        "postgresql+aws_wrapper_psycopg://u:p@h:5432/db"
        "?wrapper_dialect=aurora-pg&wrapper_plugins=failover,efm"
    )
    try:
        with engine.connect():
            pass
    except Exception:
        # The MagicMock connection may not satisfy SA's probes.
        # We only care that AwsWrapperConnection.connect was invoked with the
        # right kwargs.
        pass

    assert mock_connect.called, "AwsWrapperConnection.connect was never invoked"
    _args, kwargs = mock_connect.call_args
    assert kwargs.get("wrapper_dialect") == "aurora-pg"
    assert kwargs.get("plugins") == "failover,efm"
    assert "wrapper_plugins" not in kwargs, (
        "dialect should have renamed wrapper_plugins → plugins before the connect call"
    )


# ---- _type_info_fetch unwrap (sync) -------------------------------------


def test_pg_dialect_type_info_fetch_unwraps_target_connection(mocker):
    """Regression guard for

        TypeError: expected Connection or AsyncConnection,
                   got AwsWrapperConnection

    at ``psycopg/_typeinfo.py:90``. ``psycopg.TypeInfo.fetch`` strictly
    isinstance-checks its first argument -- our wrapper proxy is not a
    subclass of ``psycopg.Connection``. The dialect override unwraps
    to the native psycopg connection via ``target_connection`` before
    calling into psycopg.
    """
    from unittest.mock import MagicMock
    native_conn = MagicMock(name="native_psycopg_connection")
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native_conn
    # SA's sync path: connection.connection IS the DBAPI conn (our
    # wrapper). No extra adapter layer between them.
    sa_connection = MagicMock()
    sa_connection.connection = wrapper
    # Make the wrapper look like it has driver_connection too (mirrors
    # AdaptedConnection contract added in d372b5f):
    wrapper.driver_connection = wrapper

    # Patch TypeInfo.fetch so we can assert its first arg without
    # needing a live Postgres.
    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="type-info-sentinel")

    dialect = AwsWrapperPGPsycopgDialect()
    result = dialect._type_info_fetch(sa_connection, "hstore")

    assert result == "type-info-sentinel"
    fetch_mock.assert_called_once()
    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native_conn, (
        "TypeInfo.fetch must receive the native psycopg connection, "
        "not our wrapper proxy")


def test_pg_dialect_type_info_fetch_falls_through_when_no_target_connection(mocker):
    """If the DBAPI connection lacks ``target_connection`` (e.g., SA is
    pointed at a real psycopg connection directly, no wrapper in the
    middle), pass the connection through unchanged -- don't break
    non-wrapper deployments."""
    from unittest.mock import MagicMock

    # spec= so attribute-lookup returns a real AttributeError for
    # target_connection and we fall through to the conn itself.
    class _NativeLike:
        pass
    native = _NativeLike()
    sa_connection = MagicMock()
    sa_connection.connection = native

    fetch_mock = mocker.patch(
        "psycopg.types.TypeInfo.fetch", return_value="ok")

    dialect = AwsWrapperPGPsycopgDialect()
    dialect._type_info_fetch(sa_connection, "hstore")

    called_arg = fetch_mock.call_args.args[0]
    assert called_arg is native, (
        "no-wrapper deployments should pass the native conn through unchanged")


# --- do_ping / pool_pre_ping support (AWS PR #1245, ported to all dialects) ---

def test_mysql_do_ping_pings_target_connection():
    """MySQL do_ping must ping the native mysql-connector connection (the
    wrapper exposes no ``.ping()``), so pool_pre_ping works."""
    from unittest.mock import MagicMock
    native = MagicMock(name="native_mysqlconnector")
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native

    dialect = AwsWrapperMySQLConnectorDialect()
    assert dialect.do_ping(wrapper) is True
    native.ping.assert_called_once_with(reconnect=False)


def test_mysql_do_ping_returns_false_on_dead_connection():
    from unittest.mock import MagicMock
    native = MagicMock(name="native_mysqlconnector")
    native.ping.side_effect = Exception("server has gone away")
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native

    dialect = AwsWrapperMySQLConnectorDialect()
    assert dialect.do_ping(wrapper) is False


def test_pg_do_ping_runs_select_one_on_target_connection():
    """PG do_ping runs SELECT 1 on the native psycopg connection (psycopg3
    has no ``.ping()``)."""
    from unittest.mock import MagicMock
    native = MagicMock(name="native_psycopg")
    native.autocommit = True  # already autocommit -> no toggle
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native

    dialect = AwsWrapperPGPsycopgDialect()
    assert dialect.do_ping(wrapper) is True
    native.execute.assert_called_once_with("SELECT 1")


def test_pg_do_ping_returns_false_on_dead_connection():
    from unittest.mock import MagicMock
    native = MagicMock(name="native_psycopg")
    native.autocommit = True
    native.execute.side_effect = Exception("connection is closed")
    wrapper = MagicMock(name="AwsWrapperConnection")
    wrapper.target_connection = native

    dialect = AwsWrapperPGPsycopgDialect()
    assert dialect.do_ping(wrapper) is False
