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

from .pep249 import (Error, IntegrityError, InterfaceError, NotSupportedError,
                     OperationalError)


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
    # SA classification needs no help at the dialect boundary: ``FailoverError``
    # derives from the wrapper's own ``pep249.OperationalError``, and SA's
    # ``DBAPIError.instance`` matches ``orig.__class__.__mro__`` by class *name*,
    # so this already maps to ``sqlalchemy.exc.OperationalError`` with
    # ``DBAPIError.orig`` left as this exception.
    #
    # Do NOT add driver-native OperationalError classes
    # (psycopg / mysql.connector / aiomysql) as bases here: Django's
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


# -- KMS encryption plugin errors --
#
# Callers branch on the exception type. For example, the read path's lenient
# ``encryption_return_unverified_data`` mode will catch ``IntegrityCheckError``
# and let every other failure propagate.
#
# Each class derives from ``AwsWrapperError`` for the wrapper's own error
# handling and mixes in a PEP 249 type, which is what SQLAlchemy uses to
# classify errors at the DB-API boundary. SQLAlchemy picks the first PEP 249
# class in the MRO, so a class whose mix-in should win lists it first. Django
# does not translate these errors: its backends look error classes up on the
# native driver module, which these do not derive from, so they reach Django
# code as raised. The optional ``algorithm`` and ``data_type`` attributes help
# with diagnostics.
#
# The class hierarchy is specific to this wrapper.

class EncryptionPluginError(AwsWrapperError):
    __module__ = "aws_advanced_python_wrapper"


class EncryptionError(EncryptionPluginError, OperationalError):
    __module__ = "aws_advanced_python_wrapper"

    def __init__(self, message: str = "", *, algorithm: Optional[str] = None,
                 data_type: Optional[str] = None):
        super().__init__(message)
        self.algorithm = algorithm
        self.data_type = data_type


class IntegrityCheckError(IntegrityError, EncryptionError):
    """The payload is not a valid encrypted value: too short to hold the
    framing, or its HMAC tag does not match.

    ``IntegrityError`` is listed first so SQLAlchemy classifies it as an
    integrity error rather than through ``EncryptionError``'s
    ``OperationalError``. Lenient mode (``encryption_return_unverified_data``)
    will swallow this read-side failure and no other. A value that verifies
    but will not decrypt raises ``DecryptionError``, which lenient mode always
    lets through.
    """
    __module__ = "aws_advanced_python_wrapper"


class DecryptionError(EncryptionError):
    """A verified payload could not be decrypted, usually because the data key
    is wrong (the GCM authentication tag does not match) or the marker is bad.
    """
    __module__ = "aws_advanced_python_wrapper"


class EncryptionFailedError(EncryptionError):
    """A value could not be encrypted or serialized."""
    __module__ = "aws_advanced_python_wrapper"


class InvalidKeyError(EncryptionError):
    """A data key or HMAC key is missing or the wrong length."""
    __module__ = "aws_advanced_python_wrapper"


class InvalidAlgorithmError(EncryptionError):
    """An unsupported encryption algorithm was requested."""
    __module__ = "aws_advanced_python_wrapper"


class TypeConversionError(EncryptionError):
    """A decrypted value could not be coerced to its declared or requested type."""
    __module__ = "aws_advanced_python_wrapper"


class EncryptionConfigError(EncryptionPluginError, InterfaceError):
    """The ``encryption_*`` connection properties are missing or invalid."""
    __module__ = "aws_advanced_python_wrapper"


class KeyManagementError(EncryptionPluginError, OperationalError):
    """A data key or master key could not be created, read, decrypted, or
    stored.

    ``operation`` names the failing step (for example ``"DECRYPT_DATA_KEY"``)
    for diagnostics. It is not part of any cross-wrapper contract.
    """
    __module__ = "aws_advanced_python_wrapper"

    def __init__(self, message: str = "", *, operation: Optional[str] = None):
        super().__init__(message)
        self.operation = operation


class UnauthorizedMasterKeyError(KeyManagementError):
    """The master key named by a ``key_storage`` row is not listed in
    ``encryption_allowed_master_key_arns``. Raised before KMS is called.
    """
    __module__ = "aws_advanced_python_wrapper"


class InvalidKeyMetadataError(KeyManagementError):
    """A ``key_storage`` row is unusable: it names no master key, or its
    encrypted data key, HMAC key, or key spec is missing or malformed.
    """
    __module__ = "aws_advanced_python_wrapper"


class MetadataError(EncryptionPluginError, OperationalError):
    """The ``encryption_metadata`` / ``key_storage`` tables could not be read."""
    __module__ = "aws_advanced_python_wrapper"

    def __init__(self, message: str = "", *, table_name: Optional[str] = None,
                 column_name: Optional[str] = None):
        super().__init__(message)
        self.table_name = table_name
        self.column_name = column_name
