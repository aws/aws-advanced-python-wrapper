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

from aws_advanced_python_wrapper import errors, pep249


def test_aws_wrapper_error_is_pep249_error():
    err = errors.AwsWrapperError("x")
    assert isinstance(err, pep249.Error)


def test_aws_connect_error_is_operational_error():
    err = errors.AwsConnectError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.OperationalError)


@pytest.mark.parametrize("cls", [
    errors.FailoverError,
    errors.FailoverSuccessError,
    errors.FailoverFailedError,
    errors.TransactionResolutionUnknownError,
])
def test_failover_errors_are_operational_errors(cls):
    err = cls("x")
    assert isinstance(err, pep249.OperationalError)
    assert isinstance(err, pep249.Error)


def test_query_timeout_is_operational_error():
    err = errors.QueryTimeoutError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.OperationalError)


def test_read_write_splitting_error_is_interface_error():
    err = errors.ReadWriteSplittingError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.InterfaceError)


def test_unsupported_operation_is_not_supported_error():
    err = errors.UnsupportedOperationError("x")
    assert isinstance(err, errors.AwsWrapperError)
    assert isinstance(err, pep249.NotSupportedError)


def test_existing_except_awswrappererror_still_catches_children():
    for cls in (errors.AwsConnectError, errors.QueryTimeoutError,
                errors.ReadWriteSplittingError, errors.UnsupportedOperationError):
        try:
            raise cls("x")
        except errors.AwsWrapperError:
            pass
        else:
            pytest.fail(f"{cls.__name__} should be caught by except AwsWrapperError")


def test_failover_errors_caught_by_except_error():
    for cls in (errors.FailoverError, errors.FailoverSuccessError,
                errors.FailoverFailedError, errors.TransactionResolutionUnknownError):
        try:
            raise cls("x")
        except pep249.Error:
            pass
        else:
            pytest.fail(f"{cls.__name__} should be caught by except pep249.Error")


def test_mro_shape_has_single_error_ancestor():
    for cls in (errors.AwsConnectError, errors.QueryTimeoutError,
                errors.ReadWriteSplittingError, errors.UnsupportedOperationError):
        error_ancestors = [c for c in cls.__mro__ if c is pep249.Error]
        assert len(error_ancestors) == 1, f"{cls.__name__} has >1 Error in MRO"
