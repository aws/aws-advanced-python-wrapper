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

import sys
from importlib import import_module
from logging import DEBUG, Formatter, Logger, StreamHandler
from queue import Empty, Queue
from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo


class LogUtils:
    @staticmethod
    def setup_logger(logger: Logger, level: int = DEBUG, format_string: Optional[str] = None):
        for handler in logger.handlers:
            if isinstance(handler, StreamHandler):
                return

        if format_string is None:
            format_string = \
                "%(asctime)s.%(msecs)03d %(name)-12s:%(funcName)s [%(levelname)-8s] - %(threadName)s - %(message)s"

        handler = StreamHandler(stream=sys.stdout)
        handler.setFormatter(Formatter(format_string))
        handler.setLevel(level)

        logger.setLevel(level)
        logger.addHandler(handler)

    @staticmethod
    def log_topology(hosts: Tuple[HostInfo, ...], message_prefix: Optional[str] = None):
        """Returns a formatted string of the topology that looks like the following
        Topology: {
            HostInfo[host=url, port=123]
            <null>
            HostInfo[host=url1, port=234]}

        Args:
            hosts (list): a list of hosts representing the cluster topology
            message_prefix (str): messages to prefix to the topology information

        Returns:
            _type_: a formatted string of the topology
        """
        msg = "\n\t".join(["<null>" if not host else str(host) for host in hosts])
        prefix = "" if not message_prefix else message_prefix + " "
        return prefix + f": {{\n\t{msg}}}"


class QueueUtils:
    @staticmethod
    def get(q: Queue):
        try:
            return q.get_nowait()
        except Empty:
            return None

    @staticmethod
    def clear(q: Queue):
        with q.mutex:
            q.queue.clear()
            q.all_tasks_done.notify_all()
            q.unfinished_tasks = 0


class Utils:
    @staticmethod
    def initialize_class(full_class_name: str, *args):
        try:
            parts = full_class_name.split('.')
            m = import_module(".".join(parts[:-1]))
            return getattr(m, parts[-1])(*args)
        except ModuleNotFoundError:
            return None
