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

from typing import TYPE_CHECKING, FrozenSet, List, Optional, Sequence

from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo


class AccessibleRegions:
    """Utilities for the ``gdb_accessible_regions`` restriction."""

    @staticmethod
    def parse(props: Properties) -> Optional[FrozenSet[str]]:
        """Parse the ``gdb_accessible_regions`` property into an immutable set
        of normalized (lowercased, trimmed) region names.

        Returns ``None`` when the property is unset or empty, meaning all
        regions are considered accessible (no restriction).
        """
        raw = WrapperProperties.GDB_ACCESSIBLE_REGIONS.get(props)
        if not raw or not raw.strip():
            return None

        regions = frozenset(
            region.strip().casefold()
            for region in raw.split(",")
            if region.strip()
        )
        return regions if regions else None

    @staticmethod
    def is_in_accessible_region(
            host: str,
            accessible_regions: Optional[FrozenSet[str]],
            rds_utils: RdsUtils) -> bool:
        """Return whether ``host`` lies in one of the ``accessible_regions``.

        When ``accessible_regions`` is ``None`` or empty (no restriction),
        every host is considered accessible. Otherwise the host's region is
        parsed from its endpoint; a host whose region cannot be parsed or is
        not in the set is excluded.
        """
        if not accessible_regions:
            return True
        region = rds_utils.get_rds_region(host)
        return region is not None and region.casefold() in accessible_regions

    @staticmethod
    def filter_available_hosts(
            hosts: Sequence[HostInfo],
            accessible_regions: Optional[FrozenSet[str]],
            rds_utils: Optional[RdsUtils] = None) -> List[HostInfo]:
        """Return the subset of ``hosts`` located in ``accessible_regions``.

        Shared implementation behind the Global Aurora dialects'
        ``filter_available_hosts`` overrides. Returns the hosts unchanged (as a
        list) when there is no accessible-regions restriction. A ``RdsUtils``
        instance is created on demand when one is not supplied.
        """
        if not accessible_regions:
            return list(hosts)
        rds_utils = rds_utils or RdsUtils()
        return [
            host for host in hosts
            if AccessibleRegions.is_in_accessible_region(host.host, accessible_regions, rds_utils)
        ]
