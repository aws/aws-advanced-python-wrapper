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

import re
from typing import TYPE_CHECKING, List, Optional

from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole
from aws_advanced_python_wrapper.utils.log import Logger

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

logger = Logger(__name__)

_STRICT_WRITER_PREFIX = "strict-writer-"
_STRICT_READER_PREFIX = "strict-reader-"
_PRIMARY = "primary"
_SECONDARY = "secondary"
_DEFAULT_VALUE = "strict-writer-primary"

# AWS region identifiers look like "us-east-1", "eu-west-2", "ap-southeast-1".
# Any unrecognized token that does not match this shape is most likely a typo
# (e.g. "strict-writer-primary" misspelled as "strict-wrtier-primary").
_REGION_SHAPE = re.compile(r"^[a-z]{2}-[a-z]+-\d+$")


class GdbMonitoringConnectionPriority:
    """Region-aware monitoring connection priority for Aurora Global Databases.

    A priority describes the kind of host the topology monitor prefers for its
    background connection, combining an optional required role, an optional
    required region, and primary/secondary-region flags.

    Supported priority strings:

    - ``strict-writer-primary`` — writer in the primary region.
    - ``strict-reader-primary`` — reader in the primary region.
    - ``strict-reader-secondary`` — reader in any secondary region.
    - ``strict-writer-<region>`` / ``strict-reader-<region>`` — that role in the
      named region. ``strict-writer-secondary`` is rejected (a writer cannot be
      in a secondary region).
    - ``<region>`` — any host in the named region.
    """

    def __init__(
            self,
            required_role: Optional[HostRole],
            required_region: Optional[str],
            require_primary: bool,
            require_secondary: bool,
            original_value: str):
        self._required_role = required_role
        self._required_region = required_region
        self._require_primary = require_primary
        self._require_secondary = require_secondary
        self._original_value = original_value

    @property
    def required_role(self) -> Optional[HostRole]:
        return self._required_role

    @property
    def required_region(self) -> Optional[str]:
        return self._required_region

    @property
    def require_primary(self) -> bool:
        return self._require_primary

    @property
    def require_secondary(self) -> bool:
        return self._require_secondary

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional[GdbMonitoringConnectionPriority]:
        if value is None or not value.strip():
            return None

        trimmed = value.strip().lower()

        if trimmed.startswith(_STRICT_WRITER_PREFIX):
            suffix = trimmed[len(_STRICT_WRITER_PREFIX):]
            if not suffix:
                return None
            if suffix == _PRIMARY:
                return cls(HostRole.WRITER, None, True, False, trimmed)
            if suffix == _SECONDARY:
                # A writer cannot live in a secondary region for an Aurora
                # Global Database (only the primary region has a writer).
                return None
            return cls(HostRole.WRITER, suffix, False, False, trimmed)

        if trimmed.startswith(_STRICT_READER_PREFIX):
            suffix = trimmed[len(_STRICT_READER_PREFIX):]
            if not suffix:
                return None
            if suffix == _PRIMARY:
                return cls(HostRole.READER, None, True, False, trimmed)
            if suffix == _SECONDARY:
                return cls(HostRole.READER, None, False, True, trimmed)
            return cls(HostRole.READER, suffix, False, False, trimmed)

        # Any token without a known prefix is treated as a bare region literal.
        # If it does not even look like an AWS region identifier, it is almost
        # certainly a typo that will never match a host, so warn to aid
        # diagnosis rather than coercing it silently.
        if not _REGION_SHAPE.match(trimmed):
            logger.debug("GdbMonitoringConnectionHandler.UnrecognizedPriority", value)
        return cls(None, trimmed, False, False, trimmed)

    @classmethod
    def parse_list(cls, value: Optional[str]) -> List[GdbMonitoringConnectionPriority]:
        """Parse a comma-separated priority list.

        Defaults to ``[strict-writer-primary]`` when the value is unset/empty or
        when no item parses to a valid priority. Unlike the plain
        :class:`MonitoringConnectionPriority`, duplicates are **not** dropped.
        """
        result: List[GdbMonitoringConnectionPriority] = []
        if value is None or not value.strip():
            result.append(cls(HostRole.WRITER, None, True, False, _DEFAULT_VALUE))
            return result

        for item in value.split(","):
            priority = cls.from_value(item)
            if priority is not None:
                result.append(priority)

        if not result:
            result.append(cls(HostRole.WRITER, None, True, False, _DEFAULT_VALUE))
        return result

    def is_satisfied_by(
            self,
            host: HostInfo,
            primary_region: Optional[str],
            rds_utils: RdsUtils) -> bool:
        if self._required_role is not None and host.role != self._required_role:
            return False

        host_region = rds_utils.get_rds_region(host.host)

        if self._require_primary:
            if primary_region is None or host_region is None \
                    or primary_region.casefold() != host_region.casefold():
                return False
        if self._require_secondary:
            if primary_region is None or host_region is None \
                    or primary_region.casefold() == host_region.casefold():
                return False

        if self._required_region is not None:
            if host_region is None or self._required_region.casefold() != host_region.casefold():
                return False

        return True

    def find_matching_host(
            self,
            hosts: List[HostInfo],
            primary_region: Optional[str],
            rds_utils: RdsUtils) -> Optional[HostInfo]:
        for host in hosts:
            if self.is_satisfied_by(host, primary_region, rds_utils):
                return host
        return None

    def find_matching_hosts(
            self,
            hosts: List[HostInfo],
            primary_region: Optional[str],
            rds_utils: RdsUtils) -> List[HostInfo]:
        return [host for host in hosts if self.is_satisfied_by(host, primary_region, rds_utils)]

    def __str__(self) -> str:
        return self._original_value

    def __repr__(self) -> str:
        return f"GdbMonitoringConnectionPriority({self._original_value!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GdbMonitoringConnectionPriority):
            return NotImplemented
        return (self._required_role == other._required_role
                and self._required_region == other._required_region
                and self._require_primary == other._require_primary
                and self._require_secondary == other._require_secondary)

    def __hash__(self) -> int:
        return hash((self._required_role, self._required_region,
                     self._require_primary, self._require_secondary))
