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

from datetime import datetime, timedelta
from enum import Enum, auto

from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)


class HostAvailability(Enum):
    AVAILABLE = auto()
    UNAVAILABLE = auto()


class HostAvailabilityStrategy:
    def get_host_availability(self, raw_host_availability) -> HostAvailability:
        return raw_host_availability

    def set_host_availability(self, host_availability: HostAvailability):
        pass  # do nothing


class ExponentialBackoffHostAvailabilityStrategy(HostAvailabilityStrategy):
    name = "exponential_backoff"

    def __init__(self, properties: Properties):
        max_retries = WrapperProperties.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.get_int(properties)
        if max_retries < 1:
            raise ValueError(Messages.get_formatted("HostAvailabilityStrategy.InvalidMaxRetries", max_retries))
        self._max_retries = max_retries

        backoff_time = WrapperProperties.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.get_int(properties)
        if backoff_time < 1:
            raise ValueError(Messages.get_formatted("HostAvailabilityStrategy.InvalidInitialBackoffTime", backoff_time))
        self._initial_backoff_time_secs = backoff_time

        self._not_available_count = 0
        self._last_changed = datetime.now()

    def get_host_availability(self, raw_host_availability) -> HostAvailability:
        if raw_host_availability == HostAvailability.AVAILABLE:
            return HostAvailability.AVAILABLE

        if self._not_available_count >= self._max_retries:
            return HostAvailability.UNAVAILABLE

        retry_delay_ms = 2 ** self._not_available_count * self._initial_backoff_time_secs * 1000
        earliest_retry = self._last_changed + timedelta(milliseconds=retry_delay_ms)
        if earliest_retry < datetime.now():
            return HostAvailability.AVAILABLE

        return raw_host_availability

    def set_host_availability(self, host_availability: HostAvailability):
        self._last_changed = datetime.now()
        if host_availability == HostAvailability.AVAILABLE:
            self._not_available_count = 0
        else:
            self._not_available_count += 1


def create_host_availability_strategy(properties: Properties):
    if properties is None:
        return HostAvailabilityStrategy()

    default_strategy = WrapperProperties.DEFAULT_HOST_AVAILABILITY_STRATEGY.get(properties)
    if default_strategy is None or default_strategy == "":
        return HostAvailabilityStrategy()

    if default_strategy == ExponentialBackoffHostAvailabilityStrategy.name:
        return ExponentialBackoffHostAvailabilityStrategy(properties)

    return HostAvailabilityStrategy()
