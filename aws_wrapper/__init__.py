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

from logging import DEBUG, Formatter, Handler, StreamHandler, getLogger

from .wrapper import AwsWrapperConnection

# PEP249 compliance
connect = AwsWrapperConnection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


def set_logger(name='aws_wrapper', level=DEBUG, format_string=None):
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)-12s [%(levelname)-8s] -- %(process)s -- %(message)s"

    handler = StreamHandler()
    handler.setFormatter(Formatter(format_string))
    handler.setLevel(level)

    logger = getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)


class NullHandler(Handler):
    def emit(self, record):
        pass


getLogger("aws_wrapper").addHandler(NullHandler())
