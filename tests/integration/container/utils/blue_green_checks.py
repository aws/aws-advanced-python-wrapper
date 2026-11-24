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

"""Probe results and waits shared by the sync and async Blue/Green switchover tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Condition, Event
from time import perf_counter_ns, sleep, thread_time
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional,
                    Sequence, Tuple)

from aws_advanced_python_wrapper.blue_green_plugin import BlueGreenPhase
from .test_timings import (BG_API_STATUS_WAIT_SEC,
                           BG_SUSPENDED_CALL_MAX_CPU_SEC,
                           BG_SUSPENDED_CALL_MAX_SEC,
                           BG_SWITCHOVER_REQUEST_WAIT_SEC,
                           BG_SWITCHOVER_WAIT_SEC, BG_TRIGGER_JOIN_SEC,
                           BG_WAIT_STEP_SEC)

if TYPE_CHECKING:
    from threading import Thread

    from .rds_test_utility import RdsTestUtility

_NS_PER_SEC = 1_000_000_000
_PHASES_PAST_IN_PROGRESS = (BlueGreenPhase.POST.name, BlueGreenPhase.COMPLETED.name)


@dataclass
class ProbeResult:
    """What one probe saw. A phase is the name of the plugin's phase, read from its status store."""
    phase_at_start: Optional[str] = None
    phase_at_end: Optional[str] = None
    start_ns: int = 0
    end_ns: int = 0
    cpu_sec: float = 0.0
    error: Optional[Exception] = None
    driver_host: Optional[str] = None
    server_id: Optional[str] = None
    _cpu_start_sec: float = field(default=0.0, repr=False)

    def begin(self) -> None:
        """Call on the probe's own thread right before the call."""
        self._cpu_start_sec = thread_time()
        self.start_ns = perf_counter_ns()

    def end(self, error: Optional[Exception], phase_at_end: Optional[str]) -> None:
        """Call on the probe's own thread right after the call."""
        self.cpu_sec = thread_time() - self._cpu_start_sec
        self.error = error
        self.phase_at_end = phase_at_end
        self.end_ns = perf_counter_ns()

    def is_in_flight(self) -> bool:
        return self.start_ns > 0 and self.end_ns == 0

    def duration_sec(self, now_ns: int) -> float:
        """How long the call took, or has taken so far while it is in flight."""
        if self.start_ns == 0:
            return 0.0
        if self.end_ns == 0:
            return (now_ns - self.start_ns) / _NS_PER_SEC
        return (self.end_ns - self.start_ns) / _NS_PER_SEC

    def is_stuck(self, now_ns: int) -> bool:
        return self.is_in_flight() and self.duration_sec(now_ns) > BG_SUSPENDED_CALL_MAX_SEC


def suspended_call_failures(
        label: str, probe: ProbeResult, now_ns: int, check_held: bool, check_cpu: bool) -> List[str]:
    """Judge a call that a probe started in IN_PROGRESS. It gets at most one failure line.

    :param label: names the call in the line, for example "execute".
    :param probe: the probe's result.
    :param now_ns: when the test stopped waiting, from perf_counter_ns().
    :param check_held: whether the plugin must hold the call until the phase moves on.
    :param check_cpu: whether the call must stay within BG_SUSPENDED_CALL_MAX_CPU_SEC of its thread's CPU.
    :return: the failure line, or no line when the call passed.
    """
    if probe.phase_at_start != BlueGreenPhase.IN_PROGRESS.name:
        return [f"The {label} probe didn't start in IN_PROGRESS (phase {probe.phase_at_start})."]
    if probe.duration_sec(now_ns) > BG_SUSPENDED_CALL_MAX_SEC:
        return [f"The {label} started in IN_PROGRESS didn't return within {BG_SUSPENDED_CALL_MAX_SEC:.0f} s."]
    if check_held and isinstance(probe.error, TimeoutError):
        return [f"bg_connect_timeout_ms released the {label} while the plugin phase was still IN_PROGRESS."]
    if check_held and probe.phase_at_end not in _PHASES_PAST_IN_PROGRESS:
        return [f"The {label} returned in plugin phase {probe.phase_at_end}, so the plugin didn't hold it."]
    if check_cpu and probe.cpu_sec > BG_SUSPENDED_CALL_MAX_CPU_SEC:
        return [f"The suspended {label} used more than {BG_SUSPENDED_CALL_MAX_CPU_SEC} s of its thread's CPU."]
    return []


class PhaseObserver:
    """The first time a test thread saw each phase the plugin published."""

    def __init__(self) -> None:
        self.first_seen_ns: Dict[str, int] = {}
        self._latest_phase_value = 0
        self._changed = Condition()

    def record(self, status: Any) -> Optional[str]:
        """Record a status read from the plugin. Works for the sync and the aio status classes.

        :param status: the status, or None when the plugin has published none.
        :return: the name of the status's phase, or None when there is no status.
        """
        if status is None:
            return None
        phase = status.phase
        with self._changed:
            if phase.name not in self.first_seen_ns:
                self.first_seen_ns[phase.name] = perf_counter_ns()
            self._latest_phase_value = max(self._latest_phase_value, phase.phase_value)
            self._changed.notify_all()
        return phase.name

    def is_seen(self, phase: BlueGreenPhase) -> bool:
        return phase.name in self.first_seen_ns

    def wait_for(self, phase: BlueGreenPhase, stop: Event) -> bool:
        """Wait until the phase is seen.

        :param phase: the phase to wait for.
        :param stop: ends the wait when set.
        :return: True when the phase was seen; False on stop, or when a later phase was seen without it.
        """
        with self._changed:
            while not stop.is_set():
                if phase.name in self.first_seen_ns:
                    return True
                if self._latest_phase_value > phase.phase_value:
                    return False
                self._changed.wait(BG_WAIT_STEP_SEC)
        return False


@dataclass
class SwitchoverRequest:
    """Filled by the trigger thread. requested_ns is set before requested, and error only after it."""
    requested: Event = field(default_factory=Event)
    requested_ns: int = 0
    error: Optional[Exception] = None


def _is_settled(thread: Thread, result: ProbeResult, now_ns: int) -> bool:
    return not thread.is_alive() or result.end_ns > 0 or result.is_stuck(now_ns)


def wait_for_switchover(
        request: SwitchoverRequest,
        threads_start_ns: int,
        probes: Sequence[Tuple[Thread, ProbeResult]],
        is_completed: Callable[[], bool]) -> str:
    """Wait until the switchover completed and every probe ended, or a probe is stuck, within the bounds.

    :param request: the trigger thread's switchover request.
    :param threads_start_ns: when the test threads started, from perf_counter_ns().
    :param probes: each probe's thread and result.
    :param is_completed: tells whether a test thread saw the switchover complete.
    :return: why the wait ended.
    """
    request_deadline_ns = threads_start_ns + int(BG_SWITCHOVER_REQUEST_WAIT_SEC * _NS_PER_SEC)
    switchover_wait_ns = int(BG_SWITCHOVER_WAIT_SEC * _NS_PER_SEC)
    while request.error is None:
        now_ns = perf_counter_ns()
        if not request.requested.is_set():
            if now_ns > request_deadline_ns:
                return f"no switchover request within {BG_SWITCHOVER_REQUEST_WAIT_SEC:.0f} s of thread start"
        elif now_ns > request.requested_ns + switchover_wait_ns:
            return f"the switchover did not finish within {BG_SWITCHOVER_WAIT_SEC:.0f} s of the request"
        elif all(_is_settled(thread, result, now_ns) for thread, result in probes):
            if is_completed():
                return "the switchover completed"
            if any(result.is_stuck(now_ns) for _, result in probes):
                return "a probe is stuck"
        sleep(BG_WAIT_STEP_SEC)
    return "the switchover request failed"


def settle_switchover(
        test_utility: RdsTestUtility,
        bg_id: str,
        trigger_thread: Thread,
        stop: Event,
        request: SwitchoverRequest) -> Optional[str]:
    """Stop the trigger if it has not requested the switchover, else wait for the RDS API to finish it. Always sets stop.

    :param test_utility: reads the deployment from the RDS API.
    :param bg_id: the Blue/Green deployment ID.
    :param trigger_thread: the thread that requests the switchover.
    :param stop: the stop signal of every test thread.
    :param request: the trigger thread's switchover request.
    :return: the deployment status the RDS API reported last. None when no switchover request succeeded, or when no
        read found the deployment.
    """
    try:
        if not request.requested.is_set():
            stop.set()
            trigger_thread.join(BG_TRIGGER_JOIN_SEC)
        if not request.requested.is_set() or request.error is not None:
            return None
        remaining_sec = BG_API_STATUS_WAIT_SEC - (perf_counter_ns() - request.requested_ns) / _NS_PER_SEC
        return test_utility.wait_until_blue_green_deployment_has_desired_status(
            bg_id, remaining_sec, "SWITCHOVER_COMPLETED", "SWITCHOVER_FAILED")
    finally:
        stop.set()
