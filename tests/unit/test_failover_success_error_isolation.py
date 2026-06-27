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

"""Lock-in: FailoverSuccessError must not subclass any driver-native error
class. This is the contract that prevents Django's ``wrap_database_errors``
from swallowing the wrapper's failover signal -- on MySQL today and on
PostgreSQL if/when a Django-PG backend ships in the wrapper.

SA classification of FailoverSuccessError to ``sqlalchemy.exc.OperationalError``
happens at the dialect boundary via
``aws_advanced_python_wrapper.sqlalchemy_dialects._exception_handling
._FailoverSuccessRewrapMixin``, which catches FailoverSuccessError in
``do_execute`` / ``do_executemany`` and re-raises as the dialect's native
``OperationalError``. That mechanism does NOT require the driver-native
multi-inheritance below; if someone re-introduces it, Django (and any
other consumer that walks ``issubclass`` against driver error modules)
will start wrapping failover signals.

Regression these tests guard against: see
tests/integration/container/django/test_django_plugins.py::
test_django_failover_during_query (commit d5ce856 introduced the
multi-inheritance and broke this test; the revert restores it).
"""

import pytest

from aws_advanced_python_wrapper.errors import FailoverSuccessError


def test_failover_success_error_is_not_psycopg_operational_error():
    psycopg = pytest.importorskip("psycopg")
    assert not issubclass(FailoverSuccessError, psycopg.errors.OperationalError), (
        "FailoverSuccessError must not subclass psycopg.errors.OperationalError. "
        "If it does, Django's wrap_database_errors will wrap it on a Django-PG "
        "backend and block ``except FailoverSuccessError:`` handlers."
    )


def test_failover_success_error_is_not_mysqlconnector_operational_error():
    mc_errors = pytest.importorskip("mysql.connector.errors")
    assert not issubclass(FailoverSuccessError, mc_errors.OperationalError), (
        "FailoverSuccessError must not subclass "
        "mysql.connector.errors.OperationalError. If it does, Django's "
        "wrap_database_errors wraps it on the MySQL Django backend "
        "(test_django_failover_during_query regression)."
    )
