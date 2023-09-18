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

from time import sleep

import pytest

from aws_wrapper.host_availability import (
    ExponentialBackoffHostAvailabilityStrategy, HostAvailability,
    HostAvailabilityStrategy, create_host_availability_strategy)
from aws_wrapper.utils.properties import Properties, WrapperProperties


def test_create_default_availability_strategy_given_empty_properties():
    props = Properties()
    strategy = create_host_availability_strategy(props)
    assert strategy is HostAvailabilityStrategy


def test_create_default_availability_strategy_given_override_property():
    props = Properties()
    WrapperProperties.DEFAULT_HOST_AVAILABILITY_STRATEGY.set(props, ExponentialBackoffHostAvailabilityStrategy.name)
    strategy = create_host_availability_strategy(props)
    assert isinstance(strategy, ExponentialBackoffHostAvailabilityStrategy)


def test_get_host_availability():
    strategy = HostAvailabilityStrategy()
    availability = strategy.get_host_availability(HostAvailability.UNAVAILABLE)
    assert availability == HostAvailability.UNAVAILABLE


def test_get_host_availability_returns_available():
    props = Properties()
    strategy = ExponentialBackoffHostAvailabilityStrategy(props)
    availability = strategy.get_host_availability(HostAvailability.AVAILABLE)
    assert availability == HostAvailability.AVAILABLE


def test_get_host_availability_max_retries_exceeded():
    props = Properties()
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.set(props, "1")
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.set(props, "3")
    strategy = ExponentialBackoffHostAvailabilityStrategy(props)
    strategy.set_host_availability(HostAvailability.UNAVAILABLE)
    strategy.set_host_availability(HostAvailability.UNAVAILABLE)

    availability = strategy.get_host_availability(HostAvailability.UNAVAILABLE)
    assert availability == HostAvailability.UNAVAILABLE


def test_get_host_availability_past_threshold():
    props = Properties()
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.set(props, "3")
    strategy = ExponentialBackoffHostAvailabilityStrategy(props)

    sleep(4)

    availability = strategy.get_host_availability(HostAvailability.UNAVAILABLE)
    assert availability == HostAvailability.AVAILABLE


def test_get_host_availability_before_threshold():
    props = Properties()
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.set(props, "3")
    strategy = ExponentialBackoffHostAvailabilityStrategy(props)

    sleep(2)

    availability = strategy.get_host_availability(HostAvailability.UNAVAILABLE)
    assert availability == HostAvailability.UNAVAILABLE


def test_raises_exception_with_invalid_max_retries():
    props = Properties()
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_MAX_RETRIES.set(props, "0")

    with pytest.raises(ValueError):
        ExponentialBackoffHostAvailabilityStrategy(props)


def test_raises_exception_with_invalid_backoff_time():
    props = Properties()
    WrapperProperties.HOST_AVAILABILITY_STRATEGY_INITIAL_BACKOFF_TIME.set(props, "0")

    with pytest.raises(ValueError):
        ExponentialBackoffHostAvailabilityStrategy(props)
