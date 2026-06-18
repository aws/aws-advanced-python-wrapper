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

from typing import Optional

from .pep249 import Error, InterfaceError, NotSupportedError, OperationalError


class AwsWrapperError(Error):
    __module__ = "aws_advanced_python_wrapper"
    driver_error: Optional[Exception]

    def __init__(self, message: str = "", original_error: Optional[Exception] = None):
        super().__init__(message)
        # If wrapping another AwsWrapperError, preserve the original driver exception
        if isinstance(original_error, AwsWrapperError) and original_error.driver_error is not None:
            self.driver_error = original_error.driver_error
        else:
            self.driver_error = original_error


class UnsupportedOperationError(AwsWrapperError, NotSupportedError):
    __module__ = "aws_advanced_python_wrapper"


class QueryTimeoutError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverError(OperationalError):
    __module__ = "aws_advanced_python_wrapper"


class TransactionResolutionUnknownError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverFailedError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverSuccessError(FailoverError):
    # SA classification is handled at the dialect boundary by
    # ``sqlalchemy_dialects._exception_handling._FailoverSuccessRewrapMixin``,
    # which catches FailoverSuccessError in ``do_execute`` /
    # ``do_executemany`` and re-raises as the dialect's native
    # OperationalError. Do NOT add driver-native OperationalError classes
    # (psycopg / mysql.connector) as bases here: Django's
    # ``wrap_database_errors`` walks ``issubclass`` against the driver's
    # own error module and would swallow FailoverSuccessError before any
    # user ``except FailoverSuccessError:`` handler could see it
    # (regression seen in tests/integration/container/django/
    # test_django_plugins.py::test_django_failover_during_query).
    __module__ = "aws_advanced_python_wrapper"


class ReadWriteSplittingError(AwsWrapperError, InterfaceError):
    __module__ = "aws_advanced_python_wrapper"


class AwsConnectError(AwsWrapperError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"
