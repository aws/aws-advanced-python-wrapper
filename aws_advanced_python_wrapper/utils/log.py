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

import logging
from logging import getLogger

from ResourceBundle import NotInResourceBundleError

from aws_advanced_python_wrapper.utils.messages import Messages


class Logger:
    def __init__(self, name: str):
        self.logger = getLogger(name)

    def debug(self, msg, *args, **kwargs):
        if not self.logger.isEnabledFor(logging.DEBUG):
            return

        if args is not None and len(args) > 0:
            self.logger.debug(Messages.get_formatted(msg, *args), **kwargs)
        else:
            try:
                self.logger.debug(Messages.get(msg), **kwargs)
            except NotInResourceBundleError:
                self.logger.debug(msg, **kwargs)

    def error(self, msg, *args, **kwargs):
        if not self.logger.isEnabledFor(logging.ERROR):
            return

        if args is not None and len(args) > 0:
            self.logger.error(Messages.get_formatted(msg, *args), **kwargs)
        else:
            try:
                self.logger.error(Messages.get(msg), **kwargs)
            except NotInResourceBundleError:
                self.logger.error(msg, **kwargs)

    def warning(self, msg, *args, **kwargs):
        if not self.logger.isEnabledFor(logging.WARNING):
            return

        if args is not None and len(args) > 0:
            self.logger.warning(Messages.get_formatted(msg, *args), **kwargs)
        else:
            try:
                self.logger.warning(Messages.get(msg), **kwargs)
            except NotInResourceBundleError:
                self.logger.warning(msg, **kwargs)

    def info(self, msg, *args, **kwargs):
        if not self.logger.isEnabledFor(logging.INFO):
            return

        if args is not None and len(args) > 0:
            self.logger.info(Messages.get_formatted(msg, *args), **kwargs)
        else:
            try:
                self.logger.info(Messages.get(msg), **kwargs)
            except NotInResourceBundleError:
                self.logger.info(msg, **kwargs)
