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

from __future__ import annotations

from logging import getLogger

from aws_wrapper.utils.messages import Messages


class Logger:
    def __init__(self, name: str):
        self.logger = getLogger(name)

    def debug(self, msg, *args, **kwargs):
        if args is not None and len(args) > 0:
            self.logger.debug(Messages.get_formatted("TESTING_ABCD " + msg, *args, **kwargs))
        else:
            self.logger.debug(Messages.get("TESTING_NO_ARGS_ABCD " + msg, *args, **kwargs))

    def error(self, msg, *args, **kwargs):
        if args is not None and len(args) > 0:
            self.logger.error(Messages.get_formatted(msg, *args, **kwargs))
        else:
            self.logger.error(Messages.get(msg, *args, **kwargs))

    def warning(self, msg, *args, **kwargs):
        if args is not None and len(args) > 0:
            self.logger.warning(Messages.get_formatted(msg, *args, **kwargs))
        else:
            self.logger.warning(Messages.get(msg, *args, **kwargs))
