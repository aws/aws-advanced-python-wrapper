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

from .pep249 import Error


class AwsWrapperError(Error):
    __module__ = "aws_advanced_python_wrapper"


class UnsupportedOperationError(AwsWrapperError):
    __module__ = "aws_advanced_python_wrapper"


class QueryTimeoutError(AwsWrapperError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverError(Error):
    __module__ = "aws_advanced_python_wrapper"


class TransactionResolutionUnknownError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverFailedError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class FailoverSuccessError(FailoverError):
    __module__ = "aws_advanced_python_wrapper"


class ReadWriteSplittingError(AwsWrapperError):
    __module__ = "aws_advanced_python_wrapper"
