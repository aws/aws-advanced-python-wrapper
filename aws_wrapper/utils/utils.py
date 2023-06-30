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


class Utils:

    @staticmethod
    def log_topology(hosts: list, message_prefix: Optional[str] = None):
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
        return prefix + f"Topology: {{\n\t{msg}}}"
