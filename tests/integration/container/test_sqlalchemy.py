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

"""SQLAlchemy creator-pattern integration tests for Aurora PG and Aurora MySQL.

Proves:
- SA `create_engine(..., creator=lambda: aws_advanced_python_wrapper.<driver>.connect(...))`
  succeeds against real Aurora clusters for both drivers.
- Aurora failover surfaces as sqlalchemy.exc.OperationalError (via the
  OperationalError-classified FailoverSuccessError) and a retry recovers.
- Read/Write Splitting's read_only flip routes to a reader and returns to a writer.

Fixtures follow the existing test_read_write_splitting.py / test_aurora_failover.py
patterns (see conftest.py in this directory).
"""

from __future__ import annotations

import gc

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from aws_advanced_python_wrapper import release_resources
from aws_advanced_python_wrapper.mysql_connector import \
    connect as mysql_connect
from aws_advanced_python_wrapper.psycopg import connect as pg_connect
from aws_advanced_python_wrapper.utils.log import Logger
from .utils.conditions import (disable_on_features, enable_on_deployments,
                               enable_on_num_instances)
from .utils.database_engine import DatabaseEngine
from .utils.database_engine_deployment import DatabaseEngineDeployment
from .utils.rds_test_utility import RdsTestUtility
from .utils.test_driver import TestDriver
from .utils.test_environment import TestEnvironment
from .utils.test_environment_features import TestEnvironmentFeatures

logger = Logger(__name__)


def _is_mysql(test_driver: TestDriver) -> bool:
    return test_driver == TestDriver.MYSQL


def _sa_url(test_driver: TestDriver) -> str:
    # Use the wrapper-aware SA dialects (registered via pyproject entry points)
    # so SA's internal isinstance(...psycopg.Connection) checks see the native
    # connection via the wrapper's _type_info_fetch override. The built-in
    # ``postgresql+psycopg``/``mysql+mysqlconnector`` dialects do not unwrap
    # AwsWrapperConnection and raise TypeError on lazy type fetches.
    return "mysql+aws_wrapper_mysqlconnector://" if _is_mysql(test_driver) else "postgresql+aws_wrapper_psycopg://"


def _wrapper_dialect(test_driver: TestDriver) -> str:
    return "aurora-mysql" if _is_mysql(test_driver) else "aurora-pg"


def _instance_id_sql(test_driver: TestDriver) -> str:
    if _is_mysql(test_driver):
        return "SELECT @@aurora_server_id"
    return "SELECT pg_catalog.aurora_db_instance_identifier()"


def _is_reader_sql(test_driver: TestDriver) -> str:
    # Data-plane "is this a reader?" check. After an Aurora failover this
    # is authoritative on the connected instance, while the control-plane
    # DescribeDBClusters.IsClusterWriter can lag the data plane by tens of
    # seconds on multi-instance clusters (5-instance MULTI-5 is the worst).
    if _is_mysql(test_driver):
        return "SELECT @@innodb_read_only"
    return "SELECT pg_catalog.pg_is_in_recovery()"


def _readonly_option(test_driver: TestDriver) -> dict:
    return {"mysql_readonly": True} if _is_mysql(test_driver) else {"postgresql_readonly": True}


def _build_engine(
        test_driver: TestDriver,
        conninfo_kwargs: dict,
        plugins: str,
        **extra_props,
):
    """Build a SQLAlchemy engine whose creator goes through the wrapper.

    ``**extra_props`` flows straight through to the wrapper's connect()
    call as additional connection properties (e.g. ``topology_refresh_ms``,
    ``failover_timeout_sec``). The wrapper picks these up via its
    ``WrapperProperties`` registry just like the URL-query path does.
    """
    if _is_mysql(test_driver):
        # ``conninfo_kwargs`` (from ``DriverHelper.get_connect_params`` for MYSQL)
        # already includes ``use_pure: True``; don't pass it again here, or
        # mysql_connect() raises ``TypeError: got multiple values for 'use_pure'``.
        creator = lambda: mysql_connect(  # noqa: E731
            "", wrapper_dialect=_wrapper_dialect(test_driver),
            plugins=plugins, **extra_props, **conninfo_kwargs,
        )
    else:
        creator = lambda: pg_connect(  # noqa: E731
            "", wrapper_dialect=_wrapper_dialect(test_driver),
            plugins=plugins, **extra_props, **conninfo_kwargs,
        )
    return create_engine(_sa_url(test_driver), creator=creator)


@enable_on_num_instances(min_instances=2)
@enable_on_deployments([DatabaseEngineDeployment.AURORA])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestSqlAlchemy:

    @pytest.fixture(autouse=True)
    def setup_method(self, request):
        logger.info(f"Starting test: {request.node.name}")
        yield
        release_resources()
        logger.info(f"Ending test: {request.node.name}")
        release_resources()
        gc.collect()

    @pytest.fixture(scope="class")
    def aurora_utility(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    def test_sqlalchemy_creator_survives_aurora_failover(
            self, test_driver: TestDriver, conn_utils, aurora_utility):
        """SA engine recovers from Aurora failover via the OperationalError retry path."""
        # Failover test is PG+MySQL symmetric; skip on unsupported engines.
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        initial_writer_id = aurora_utility.get_cluster_writer_instance_id()

        # MySQL Connector/Python doesn't support thread-based connection abort,
        # which BOTH host_monitoring plugins (v1 and v2) require. Per
        # docs/using-the-python-wrapper/using-plugins/UsingTheHostMonitoringPlugin.md:
        # "this plugin is incompatible with the MySQL Connector/Python driver."
        # Drop EFM entirely on MySQL; PG keeps host_monitoring_v2.
        plugins = "failover" if _is_mysql(test_driver) else "failover,host_monitoring_v2"
        engine = _build_engine(
            test_driver, conn_utils.get_connect_params(), plugins=plugins,
        )
        try:
            with engine.connect() as conn:
                pre_failover_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                assert pre_failover_id == initial_writer_id

            # Trigger failover against the cluster.
            aurora_utility.failover_cluster_and_wait_until_writer_changed()

            # First query after failover should raise sqlalchemy.exc.OperationalError
            # because FailoverSuccessError is reclassified as OperationalError.
            recovered = False
            attempts_remaining = 10
            while attempts_remaining > 0 and not recovered:
                try:
                    with engine.connect() as conn:
                        new_writer_id = conn.execute(
                            text(_instance_id_sql(test_driver))
                        ).scalar_one()
                        assert new_writer_id != initial_writer_id
                        # Data-plane role check: pg_is_in_recovery / @@innodb_read_only
                        # on the connected instance is authoritative and race-free
                        # against the control plane (DescribeDBClusters), which can
                        # disagree with the data plane for tens of seconds on
                        # multi-instance Aurora clusters post-failover.
                        is_reader = conn.execute(text(_is_reader_sql(test_driver))).scalar_one()
                        assert is_reader in (0, False)
                        recovered = True
                except OperationalError:
                    attempts_remaining -= 1

            assert recovered, "SA did not recover from failover within 10 attempts"
        finally:
            engine.dispose()
            release_resources()

    def test_sqlalchemy_creator_read_write_splitting(
            self, test_driver: TestDriver, conn_utils, aurora_utility):
        """R/W splitting routes read_only connections to a reader and returns to writer."""
        engine_kind = TestEnvironment.get_current().get_engine()
        if engine_kind not in (DatabaseEngine.PG, DatabaseEngine.MYSQL):
            pytest.skip(f"Unsupported engine: {engine_kind}")

        # Pair ``read_write_splitting`` with ``failover_v2`` (and, on PG,
        # ``host_monitoring_v2``) per the canonical AWS samples:
        #   * docs/examples/MySQLReadWriteSplitting.py:87 ("…,failover")
        #   * docs/examples/PGReadWriteSplitting.py:87 ("…,failover,host_monitoring")
        #   * docs/using-the-python-wrapper/using-plugins/UsingTheReadWriteSplittingPlugin.md:11
        # ``failover_v2`` is the modern equivalent of ``failover`` and is
        # in main's DEFAULT_PLUGINS. Its ``connect()`` calls
        # ``plugin_service.refresh_host_list(conn)`` on the initial
        # connection (failover_v2_plugin.py:178), which starts the
        # cluster topology monitor thread; without that, the host list
        # stays at {writer-only} and the read-only flip falls back to
        # the writer. MySQL omits host_monitoring_v2 because mysql-
        # connector-python doesn't support thread-based abort (see the
        # EFM sweep elsewhere in this file and
        # docs/.../UsingTheHostMonitoringPlugin.md:19).
        plugins = (
            "read_write_splitting,failover_v2"
            if _is_mysql(test_driver)
            else "read_write_splitting,failover_v2,host_monitoring_v2"
        )
        engine = _build_engine(
            test_driver, conn_utils.get_connect_params(),
            plugins=plugins,
        )
        try:
            with engine.connect() as conn:
                writer_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                conn.commit()

            # SA's PG dialect registers ``postgresql_readonly`` via
            # ``PGReadOnlyConnectionCharacteristic``
            # (sqlalchemy/dialects/postgresql/base.py:3263) which routes
            # through ``dialect.set_readonly`` → the wrapper's
            # ``AwsWrapperConnection.read_only`` property setter →
            # ``CONNECTION_SET_READ_ONLY`` plugin dispatch. SA's MySQL
            # dialect has no analogous characteristic — ``mysql_readonly``
            # passed to ``execution_options`` is silently ignored — so on
            # MySQL we reach the wrapper's read_only setter directly via
            # the DBAPI connection (mirrors what wrapper-aware MySQL users
            # would do today). Reset to ``False`` before pool checkin so a
            # later ``engine.connect()`` isn't stuck read-only.
            if _is_mysql(test_driver):
                with engine.connect() as conn:
                    raw = conn.connection.dbapi_connection
                    raw.read_only = True
                    try:
                        reader_id = conn.execute(
                            text(_instance_id_sql(test_driver))
                        ).scalar_one()
                        conn.commit()
                    finally:
                        raw.read_only = False
            else:
                with engine.connect().execution_options(**_readonly_option(test_driver)) as conn:
                    reader_id = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                    conn.commit()

            assert reader_id != writer_id, (
                f"read-only connection should route to a reader, got {reader_id}"
            )

            # Next non-read-only connection should return to the writer.
            with engine.connect() as conn:
                back_to_writer = conn.execute(text(_instance_id_sql(test_driver))).scalar_one()
                conn.commit()
            assert back_to_writer == writer_id
        finally:
            engine.dispose()
            release_resources()
