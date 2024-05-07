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

import random
from re import search
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Tuple

from .host_availability import HostAvailability

if TYPE_CHECKING:
    from .hostinfo import HostInfo, HostRole

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.cache_map import CacheMap
from .pep249 import Error
from .utils.messages import Messages
from .utils.properties import Properties, WrapperProperties


class HostSelector(Protocol):
    """
    Interface for a strategy defining how to pick a host from a list of hosts.
    """

    def get_host(self, hosts: Tuple[HostInfo, ...], role: HostRole, props: Optional[Properties] = None) -> HostInfo:
        ...


class RandomHostSelector(HostSelector):

    def get_host(self, hosts: Tuple[HostInfo, ...], role: HostRole, props: Optional[Properties] = None) -> HostInfo:

        eligible_hosts = [host for host in hosts if host.role == role and host.get_availability() == HostAvailability.AVAILABLE]

        if len(eligible_hosts) == 0:
            raise Error(Messages.get("HostSelector.NoEligibleHost"))

        return random.choice(eligible_hosts)


class RoundRobinClusterInfo:
    _last_host: Optional[HostInfo] = None
    _cluster_weights_dict: Dict[str, int] = {}
    _default_weight: int = 1
    _weight_counter: int = 0

    @property
    def last_host(self) -> Optional[HostInfo]:
        return self._last_host

    @last_host.setter
    def last_host(self, value):
        self._last_host = value

    @property
    def cluster_weights_dict(self) -> Dict[str, int]:
        return self._cluster_weights_dict

    @cluster_weights_dict.setter
    def cluster_weights_dict(self, value):
        self._cluster_weights_dict = value

    @property
    def default_weight(self):
        return self._default_weight

    @default_weight.setter
    def default_weight(self, value):
        self._default_weight = value

    @property
    def weight_counter(self) -> int:
        return self._weight_counter

    @weight_counter.setter
    def weight_counter(self, value):
        self._weight_counter = value


class RoundRobinHostSelector(HostSelector):
    _DEFAULT_WEIGHT: int = 1
    _DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANOS = 60000000000 * 10  # 10 minutes
    _HOST_WEIGHT_PAIRS_PATTERN = r"((?P<host>[^:/?#]*):(?P<weight>.*))"
    _round_robin_cache: CacheMap[str, Optional[RoundRobinClusterInfo]] = CacheMap()

    def get_host(self, hosts: Tuple[HostInfo, ...], role: HostRole, props: Optional[Properties] = None) -> HostInfo:

        eligible_hosts: List[HostInfo] = [host for host in hosts if host.role == role and host.get_availability() == HostAvailability.AVAILABLE]
        eligible_hosts.sort(key=lambda host: host.host, reverse=False)
        if len(eligible_hosts) == 0:
            raise AwsWrapperError(Messages.get_formatted("HostSelector.NoHostsMatchingRole", role))

        # Create new cache entries for provided hosts if necessary. All hosts point to the same cluster info.
        self._create_cache_entry_for_hosts(eligible_hosts, props)
        current_cluster_info_key: str = eligible_hosts[0].host
        cluster_info: Optional[RoundRobinClusterInfo] = RoundRobinHostSelector._round_robin_cache.get(current_cluster_info_key)

        last_host_index: int = -1
        if cluster_info is None:
            raise AwsWrapperError(Messages.get("RoundRobinHostSelector.ClusterInfoNone"))

        last_host = cluster_info.last_host
        # Check if last_host is in list of eligible hosts. Update last_host_index.
        if last_host is not None:
            for i in range(0, len(eligible_hosts)):
                if eligible_hosts[i].host == last_host.host:
                    last_host_index = i

        if cluster_info.weight_counter > 0 and last_host_index != -1:
            target_host_index = last_host_index
        else:
            if last_host_index != -1 and last_host_index != (len(eligible_hosts) - 1):
                target_host_index = last_host_index + 1
            else:
                target_host_index = 0
            weight = cluster_info.cluster_weights_dict.get(eligible_hosts[target_host_index].host)
            cluster_info.weight_counter = cluster_info.default_weight if weight is None else weight

        cluster_info.weight_counter = (cluster_info.weight_counter - 1)
        cluster_info.last_host = eligible_hosts[target_host_index]
        return eligible_hosts[target_host_index]

    def _create_cache_entry_for_hosts(self, hosts: List[HostInfo], props: Optional[Properties]) -> None:
        cached_info = None
        for host in hosts:
            info = self._round_robin_cache.get(host.host)
            if info is not None:
                cached_info = info
                break
        if cached_info is not None:
            for host in hosts:
                # Update the expiration time
                self._round_robin_cache.put(
                    host.host, cached_info, RoundRobinHostSelector._DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANOS)
        else:
            round_robin_cluster_info: RoundRobinClusterInfo = RoundRobinClusterInfo()
            self._update_cache_properties_for_round_robin_cluster_info(round_robin_cluster_info, props)
            for host in hosts:
                self._round_robin_cache.put(
                    host.host, round_robin_cluster_info, RoundRobinHostSelector._DEFAULT_ROUND_ROBIN_CACHE_EXPIRE_NANOS)

    def _update_cache_properties_for_round_robin_cluster_info(self, round_robin_cluster_info: RoundRobinClusterInfo, props: Optional[Properties]):
        cluster_default_weight: int = RoundRobinHostSelector._DEFAULT_WEIGHT
        if props is not None:
            props_weight = WrapperProperties.ROUND_ROBIN_DEFAULT_WEIGHT.get_int(props)
            if props_weight < RoundRobinHostSelector._DEFAULT_WEIGHT:
                raise AwsWrapperError(Messages.get("RoundRobinHostSelector.RoundRobinInvalidDefaultWeight"))
            cluster_default_weight = props_weight
        round_robin_cluster_info.default_weight = cluster_default_weight

        if props is not None:
            host_weights: Optional[str] = WrapperProperties.ROUND_ROBIN_HOST_WEIGHT_PAIRS.get(props)
            if host_weights is not None and len(host_weights) != 0:
                host_weight_pairs: List[str] = host_weights.split(",")

                for pair in host_weight_pairs:
                    match = search(RoundRobinHostSelector._HOST_WEIGHT_PAIRS_PATTERN, pair)
                    if match:
                        host_name = match.group("host")
                        host_weight = match.group("weight")
                    else:
                        raise AwsWrapperError(Messages.get("RoundRobinHostSelector.RoundRobinInvalidHostWeightPairs"))

                    if len(host_name) == 0 or len(host_weight) == 0:
                        raise AwsWrapperError(Messages.get("RoundRobinHostSelector.RoundRobinInvalidHostWeightPairs"))
                    try:
                        weight: int = int(host_weight)

                        if weight < RoundRobinHostSelector._DEFAULT_WEIGHT:
                            raise AwsWrapperError(Messages.get("RoundRobinHostSelector.RoundRobinInvalidHostWeightPairs"))

                        round_robin_cluster_info.cluster_weights_dict[host_name] = weight
                    except ValueError:
                        raise AwsWrapperError(Messages.get("RoundRobinHostSelector.RoundRobinInvalidHostWeightPairs"))

    def clear_cache(self):
        RoundRobinHostSelector._round_robin_cache.clear()
