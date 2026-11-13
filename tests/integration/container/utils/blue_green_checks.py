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

"""Checks shared by the sync and async Blue/Green switchover tests.

The switchover tests run probes that hit the plugin's PREPARATION, IN_PROGRESS
and POST phases and then judge them here. A suspended call must return once the
phase moves on, without a busy loop, and a new connection to a blue host must go
to the substitute host the plugin's status lists for it. This module holds the
pure logic, so it imports no database driver.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from threading import Event, Lock, get_ident, get_native_id
from time import perf_counter_ns, sleep, thread_time
from typing import (TYPE_CHECKING, Any, Callable, Dict, List, Optional,
                    Sequence, Tuple)

from aws_advanced_python_wrapper.blue_green_plugin import (
    BlueGreenRole, SubstituteConnectRouting, SuspendConnectRouting)
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from threading import Thread

    from aws_advanced_python_wrapper.utils.atomic import AtomicInt

# No bg_id is set in the test's connect params, so the plugin keys its status by the property default, normalized as
# BlueGreenPlugin.__init__ does.
BG_ID = str(WrapperProperties.BG_ID.default_value).strip().lower()
# Pinned in the switchover tests' connect params, at its default.
BG_CONNECT_TIMEOUT_MS = 30_000
# Twice bg_connect_timeout_ms (pinned to 30 s in the connect params), which caps a correct suspend. Same 60 s as JDBC's
# MAX_TRAFFIC_SUSPENSION_MS (bgSwitchoverTimeoutMs / 3), derived differently.
MAX_TRAFFIC_SUSPENSION_NS = 2 * BG_CONNECT_TIMEOUT_MS * 1_000_000
# A correct suspend waits on the status condition in 50 ms steps; the unfixed loop burned about a whole core.
MAX_SUSPENDED_CALL_CPU_SEC = 1.0
# Thread CPU sample taken on a call that is still in flight past MAX_TRAFFIC_SUSPENSION_NS.
SPIN_SAMPLE_SEC = 2.0
# Past the main thread's sample of a stuck call: the 60 s bound, a 1 s wait tick and the 2 s CPU sample, with headroom.
ASYNC_EXECUTE_TIMEOUT_SEC = MAX_TRAFFIC_SUSPENSION_NS // 1_000_000_000 + 15
# A spinning thread shares the GIL with the other spinners and still gets far more than this in 2 s; a blocked read gets close to 0.
SPIN_MIN_CPU_SEC = 0.05
# The plugin publishes each phase for at least 1.7 s (measured), so a 20 ms poll sees every phase.
PLUGIN_STATUS_POLL_SEC = 0.02
# Poll step for probes waiting on a phase, and the main thread's wait tick.
PROBE_WAIT_STEP_SEC = 0.5
WAIT_TICK_SEC = 1.0
# Connect timeout of each routing and connect probe attempt; POST lasts 23 to 26 s (measured).
PROBE_CONNECT_TIMEOUT_SEC = 10
ROUTING_PROBE_ATTEMPTS = 3
# The async probe must connect, and own the status provider, before any other thread starts.
ASYNC_CONNECT_WAIT_SEC = 30
# From thread start: the trigger waits for the start latch and then 30 s before it requests the switchover.
TRIGGER_WAIT_SEC = 150
# From the switchover request: the plugin reached COMPLETED 41 to 45 s after it (measured).
SWITCHOVER_WAIT_SEC = 240
# From the switchover request: the RDS API reported SWITCHOVER_COMPLETED after 68 to 73 s (measured).
BG_API_WAIT_SEC = 300
BG_API_POLL_SEC = 5.0
TRIGGER_JOIN_SEC = 5
# One deadline shared by every join.
THREAD_JOIN_SEC = 20
# See the worst case on the switchover tests' marker.
SWITCHOVER_TEST_TIMEOUT_SEC = 900
# Lets the main thread get the GIL often while stuck probes spin.
MIN_SWITCH_INTERVAL_SEC = 0.0005

PHASE_ORDER = ("PREPARATION", "IN_PROGRESS", "POST", "COMPLETED")

MSG_SPIN_EXECUTE = "#1352 spin - sync execute suspended in IN_PROGRESS did not return within 60 s"
MSG_SPIN_CONNECT = "#1352 spin - sync connect suspended in IN_PROGRESS did not return within 60 s"
MSG_SPIN_ASYNC_EXECUTE = "#1352 spin - async execute suspended in IN_PROGRESS did not return within 60 s"
MSG_BUSY_LOOP = "#1352 busy loop - a suspended call used more than 1.0 s of its thread CPU"
MSG_ROUTING_POST = "#1352 routing - connection opened in POST went to the dialled host, not the listed substitute"
MSG_ROUTING_PREPARATION = "#1352 routing - connection opened in PREPARATION went to the dialled host, not the listed substitute"
MSG_BLOCKED = "#1352 check inconclusive - a suspended probe is blocked, not spinning"
MSG_UNEXPECTED_HOST = "#1352 check inconclusive - a probe connection went to an unexpected host"
MSG_EXECUTE_NOT_IN_PROGRESS = "#1352 check not run - the execute probe did not start in IN_PROGRESS"
MSG_EXECUTE_NOT_HELD = "#1352 check not run - the execute probe call was not held by the plugin"
MSG_ASYNC_EXECUTE_NOT_IN_PROGRESS = "#1352 check not run - the async execute probe did not start in IN_PROGRESS"
MSG_ASYNC_EXECUTE_NOT_HELD = "#1352 check not run - the async execute probe call was not held by the plugin"
MSG_CONNECT_NOT_IN_PROGRESS = "#1352 check not run - the connect probe did not start in IN_PROGRESS"
MSG_CONNECT_NOT_SUSPENDED = "#1352 check not run - the connect probe was not suspended by the plugin"
MSG_ROUTING_NO_SUBSTITUTE = "#1352 check not run - the routing probe saw no POST status with a substitute routing for its host"
MSG_ROUTING_NO_CONNECTION = "#1352 check not run - the routing probe opened no connection while a substitute was listed"
MSG_DRIVER_HOST_UNREADABLE = "#1352 check not run - the driver host of a probe connection could not be read"
MSG_ASYNC_NOT_CONNECTED = "#1352 check not run - the async execute probe did not connect before the other threads"

# label -> (spin, not started in IN_PROGRESS, not held); connect has no hold.
_SUSPENDED_CALL_MESSAGES: Dict[str, Tuple[str, str, Optional[str]]] = {
    "execute": (MSG_SPIN_EXECUTE, MSG_EXECUTE_NOT_IN_PROGRESS, MSG_EXECUTE_NOT_HELD),
    "connect": (MSG_SPIN_CONNECT, MSG_CONNECT_NOT_IN_PROGRESS, None),
    "async execute": (MSG_SPIN_ASYNC_EXECUTE, MSG_ASYNC_EXECUTE_NOT_IN_PROGRESS, MSG_ASYNC_EXECUTE_NOT_HELD),
}
_ROUTING_MESSAGES: Dict[str, str] = {"POST": MSG_ROUTING_POST, "PREPARATION": MSG_ROUTING_PREPARATION}


@dataclass
class RoutingAttempt:
    phase: str
    substitute_host: Optional[str]  # None when the status listed no substitute for the probe host
    driver_host: Optional[str] = None
    outcome: str = ""


@dataclass
class ProbeResult:
    name: str
    probe_host: str = ""
    phase_at_start: Optional[str] = None
    # The plugin phase read right after a suspended call returned.
    phase_at_end: Optional[str] = None
    start_ns: int = 0
    end_ns: int = 0  # 0 while the call is in flight
    hold_ns: int = 0
    cpu_sec: float = 0.0
    outcome: str = ""  # "ok", or the exception class and the start of its message
    suspend_listed: bool = False
    suspend_detail: str = ""
    # For the summary only: how the plugin's execute would route this host under the status that started the call.
    routing_detail: str = ""
    attempts: List[RoutingAttempt] = field(default_factory=list)
    native_id: Optional[int] = None
    ident: Optional[int] = None
    done: bool = False
    # Filled by the main thread when the call is still in flight.
    spinning: Optional[bool] = None
    spin_cpu_delta_sec: Optional[float] = None
    frames: Tuple[str, str] = ("none", "none")
    _cpu_start_sec: float = field(default=0.0, repr=False)

    def begin(self) -> None:
        """Called on the probe's own thread right before the call."""
        self.native_id = get_native_id()
        self.ident = get_ident()
        self._cpu_start_sec = thread_time()
        self.start_ns = perf_counter_ns()

    def end(self, error: Optional[BaseException]) -> None:
        """Called on the probe's own thread right after the call."""
        self.cpu_sec = thread_time() - self._cpu_start_sec
        self.outcome = "ok" if error is None else describe_error(error)
        self.end_ns = perf_counter_ns()

    def in_flight_ns(self, now_ns: int) -> int:
        if self.start_ns == 0:
            return 0
        return (self.end_ns if self.end_ns > 0 else now_ns) - self.start_ns

    def is_in_flight(self) -> bool:
        return self.start_ns > 0 and self.end_ns == 0

    def is_stuck(self, now_ns: int) -> bool:
        return self.is_in_flight() and now_ns - self.start_ns > MAX_TRAFFIC_SUSPENSION_NS


@dataclass
class PluginStatusObservation:
    """What the plugin published, as the test's observer saw it."""
    plugin_phase_first_seen_ns: Dict[str, int] = field(default_factory=dict)
    phase_events: Dict[str, Event] = field(default_factory=lambda: {phase: Event() for phase in PHASE_ORDER})
    status_seen: Event = field(default_factory=Event)
    pre_trigger_status: Any = None
    error: str = ""
    probe_host: Optional[str] = None
    probe_host_choice: str = ""
    # Async only: whether the status provider's monitors were still running at the end of the window.
    provider_monitors_running: Optional[bool] = None
    lock: Lock = field(default_factory=Lock)


def describe_error(error: BaseException) -> str:
    return f"{type(error).__name__}: {str(error)[:200]}"


def _clean(text: str) -> str:
    return text.replace(";", ",").replace("|", "/").replace("\n", " ")


def failure_message(text: str, detail: str) -> str:
    return f"{text} ({_clean(detail)})"


def _sec(ns: int) -> str:
    return f"{ns / 1_000_000_000:.2f} s"


def record_plugin_phase(
        phase_first_seen_ns: Dict[str, int],
        phase_events: Dict[str, Event],
        status: Any) -> Optional[str]:
    """Record the first time a phase is seen; works for the sync and the aio status classes."""
    if status is None:
        return None
    name: str = status.phase.name
    if name not in phase_first_seen_ns:
        phase_first_seen_ns[name] = perf_counter_ns()
    event = phase_events.get(name)
    if event is not None:
        event.set()
    return name


def wait_for_phase(observation: PluginStatusObservation, phase: str, stop: Event) -> bool:
    """Wait for the phase; False on stop or when a later phase was seen without it."""
    later = PHASE_ORDER[PHASE_ORDER.index(phase) + 1:]
    while not stop.is_set():
        if observation.phase_events[phase].is_set():
            return True
        if any(observation.phase_events[p].is_set() for p in later):
            return False
        observation.phase_events[phase].wait(PROBE_WAIT_STEP_SEC)
    return False


def choose_probe_host(status: Any, cluster_endpoint: str, writer_host: str) -> Tuple[str, str]:
    if status is None:
        return cluster_endpoint, "no pre-trigger status"
    if status.get_role(HostInfo(host=cluster_endpoint)) is not None:
        return cluster_endpoint, "cluster endpoint"
    return writer_host, "writer instance"


def get_probe_host(
        observation: PluginStatusObservation,
        cluster_endpoint: str,
        writer_host: str,
        switchover_requested: Event,
        stop: Event) -> str:
    """Wait for the first published status (or the request), then choose the probe host once for every probe."""
    while not (observation.status_seen.is_set() or switchover_requested.is_set() or stop.is_set()):
        observation.status_seen.wait(PROBE_WAIT_STEP_SEC)
    with observation.lock:
        if observation.probe_host is None:
            observation.probe_host, observation.probe_host_choice = \
                choose_probe_host(observation.pre_trigger_status, cluster_endpoint, writer_host)
        return observation.probe_host


def listed_substitute(status: Any, probe_host: str, port: int) -> Optional[str]:
    """The substitute host the status lists for the probe host. Reads the routings; never calls is_match, the code under test."""
    if status is None:
        return None
    host = probe_host.lower()
    endpoints = (host, f"{host}:{port}")
    for routing in status.connect_routings:
        if isinstance(routing, SubstituteConnectRouting) and routing._endpoint is not None \
                and routing._endpoint.lower() in endpoints:
            return routing._substitute_host_info.host
    return None


def connect_suspend_listed(status: Any, host: str) -> Tuple[bool, str]:
    if status is None:
        return False, "no status"
    role = status.get_role(HostInfo(host=host))
    if role != BlueGreenRole.TARGET:
        return False, f"role of the connect probe host was {role}"
    for routing in status.connect_routings:
        if isinstance(routing, SuspendConnectRouting) and routing._endpoint is None \
                and routing._bg_role == BlueGreenRole.TARGET:
            return True, "SuspendConnectRouting(None, TARGET) listed"
    return False, "no SuspendConnectRouting(None, TARGET) listed"


def execute_routing_detail(status: Any, plugin_service: Any) -> str:
    """The current host's role and first matching execute routing, as the plugin's execute picks them. Summary only."""
    try:
        if status is None:
            return "routing: no status"
        host_info = plugin_service.current_host_info
        role = status.get_role(host_info)
        routing = None if role is None else next(
            (r for r in status.execute_routings if r.is_match(host_info, role)), None)
        host = None if host_info is None else host_info.get_host_and_port()
        return (f"current host {host}, role {role}, first matching execute routing {routing}, "
                f"roles by endpoint {sorted(status.roles_by_endpoint.keys())}")
    except Exception as e:
        return f"routing: {describe_error(e)}"


def thread_cpu_sec(native_id: Optional[int]) -> Optional[float]:
    """utime + stime of one thread of this process (Linux); None where /proc is missing."""
    if native_id is None:
        return None
    try:
        with open(f"/proc/self/task/{native_id}/stat") as stat_file:
            stat = stat_file.read()
        fields = stat[stat.rindex(")") + 2:].split()
        # Fields after the command name start at field 3 (state); utime and stime are fields 14 and 15.
        return (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, IndexError):
        return None


def stuck_frames(ident: Optional[int]) -> Tuple[str, str]:
    """The innermost frame of a thread and its innermost frame in the wrapper, as file:function (never arguments)."""
    frame = None if ident is None else sys._current_frames().get(ident)
    if frame is None:
        return "none", "none"
    innermost = f"{os.path.basename(frame.f_code.co_filename)}:{frame.f_code.co_name}"
    wrapper_frame = "none"
    marker = f"aws_advanced_python_wrapper{os.sep}"
    current: Any = frame
    while current is not None:
        filename = current.f_code.co_filename
        if marker in filename:
            wrapper_frame = f"{filename[filename.rindex(marker):]}:{current.f_code.co_name}"
            break
        current = current.f_back
    return innermost, wrapper_frame


def is_spinning(cpu_before: Optional[float], cpu_after: Optional[float]) -> bool:
    if cpu_before is None or cpu_after is None:
        return True
    return cpu_after - cpu_before >= SPIN_MIN_CPU_SEC


def sample_in_flight_probes(probes: Sequence[ProbeResult]) -> None:
    """Sample the thread CPU of in-flight probes once over SPIN_SAMPLE_SEC, and lower the switch interval first."""
    if not probes:
        return
    sys.setswitchinterval(MIN_SWITCH_INTERVAL_SEC)
    before = [thread_cpu_sec(probe.native_id) for probe in probes]
    sleep(SPIN_SAMPLE_SEC)
    for probe, cpu_before in zip(probes, before):
        cpu_after = thread_cpu_sec(probe.native_id)
        probe.spinning = is_spinning(cpu_before, cpu_after)
        probe.spin_cpu_delta_sec = None if cpu_before is None or cpu_after is None else cpu_after - cpu_before
        probe.frames = stuck_frames(probe.ident)


def _is_settled(probe: ProbeResult, now_ns: int, completed: bool) -> bool:
    return probe.done or probe.end_ns > 0 or probe.is_stuck(now_ns) or (probe.start_ns == 0 and completed)


def wait_for_switchover(
        probes: Sequence[ProbeResult],
        observation: PluginStatusObservation,
        switchover_requested: Event,
        switchover_requested_ns: AtomicInt,
        threads_start_ns: int,
        completed_seen: Callable[[], bool]) -> str:
    """Wait until every probe settled and the switchover completed (or a probe is stuck), within the stated bounds."""
    sampled: List[str] = []
    while True:
        now_ns = perf_counter_ns()
        requested = switchover_requested.is_set()
        if requested and now_ns >= switchover_requested_ns.get() + SWITCHOVER_WAIT_SEC * 1_000_000_000:
            reason = f"bound reached: switchover request + {SWITCHOVER_WAIT_SEC} s"
            break
        if not requested and now_ns >= threads_start_ns + TRIGGER_WAIT_SEC * 1_000_000_000:
            reason = f"bound reached: no switchover request {TRIGGER_WAIT_SEC} s after thread start"
            break

        newly_stuck = [p for p in probes if p.is_stuck(now_ns) and p.name not in sampled]
        if newly_stuck:
            sample_in_flight_probes(newly_stuck)
            sampled.extend(p.name for p in newly_stuck)
            continue

        completed = observation.phase_events["COMPLETED"].is_set() or completed_seen()
        if requested and all(_is_settled(p, now_ns, completed) for p in probes) \
                and (completed or any(p.is_stuck(now_ns) for p in probes)):
            return "probes settled" + (", switchover completed" if completed else ", a probe is stuck")
        sleep(WAIT_TICK_SEC)

    sample_in_flight_probes([p for p in probes if p.is_in_flight() and p.name not in sampled])
    return reason


def evaluate_suspended_call(
        label: str,
        result: ProbeResult,
        now_ns: int,
        post_first_seen_ns: Optional[int]) -> List[str]:
    spin_message, not_run_message, not_held_message = _SUSPENDED_CALL_MESSAGES[label]
    # A probe sampled past the bound as blocked gets the inconclusive text; an unsampled or spinning one the defect's.
    stuck_message = MSG_BLOCKED if result.spinning is False else spin_message
    if result.phase_at_start != "IN_PROGRESS":
        return [failure_message(not_run_message, f"{result.name} phase at start {result.phase_at_start}, outcome {result.outcome or '-'}")]
    if label == "connect" and not result.suspend_listed:
        return [failure_message(MSG_CONNECT_NOT_SUSPENDED, f"{result.name} {result.suspend_detail}")]

    if result.is_in_flight():
        cpu_delta = "unread" if result.spin_cpu_delta_sec is None else f"{result.spin_cpu_delta_sec:.3f} s"
        detail = (f"{result.name} on {result.probe_host} in flight {_sec(result.in_flight_ns(now_ns))}, "
                  f"CPU over {SPIN_SAMPLE_SEC} s sample {cpu_delta}, "
                  f"innermost frame {result.frames[0]}, wrapper frame {result.frames[1]}")
        return [failure_message(stuck_message, detail)]

    returned = f"{result.name} returned after {_sec(result.in_flight_ns(now_ns))} with {result.outcome}"
    if result.in_flight_ns(now_ns) > MAX_TRAFFIC_SUSPENSION_NS or result.hold_ns > MAX_TRAFFIC_SUSPENSION_NS:
        return [failure_message(stuck_message, f"{returned}, hold {_sec(result.hold_ns)}, over the 60 s bound")]
    if result.phase_at_end == "IN_PROGRESS":
        # Released by the plugin's suspend timer, not by the phase change.
        return [failure_message(not_run_message, f"{returned}, plugin phase still IN_PROGRESS when it returned")]
    if not_held_message is not None and result.hold_ns == 0:
        return [failure_message(not_held_message, returned)]
    if label == "connect" and (post_first_seen_ns is None or result.end_ns < post_first_seen_ns - 1_000_000_000):
        return [failure_message(MSG_CONNECT_NOT_SUSPENDED, f"{returned}, before the plugin's POST")]
    if result.cpu_sec > MAX_SUSPENDED_CALL_CPU_SEC:
        return [failure_message(MSG_BUSY_LOOP, f"{returned}, thread CPU {result.cpu_sec:.3f} s")]
    return []


def evaluate_routing(result: ProbeResult, phase_label: str, required: bool, writer_ips: str) -> List[str]:
    listed = [a for a in result.attempts if a.substitute_host is not None and a.phase == phase_label]
    if result.phase_at_start != phase_label or not listed:
        if not required:
            return []
        detail = f"{result.name} phase at start {result.phase_at_start}, {len(result.attempts)} attempts, none with a substitute"
        return [failure_message(MSG_ROUTING_NO_SUBSTITUTE, detail)]

    succeeded = [a for a in listed if a.outcome == "ok"]
    if not succeeded:
        return [failure_message(MSG_ROUTING_NO_CONNECTION, f"{result.name} in {phase_label}, last error {listed[-1].outcome}")]

    attempt = succeeded[0]
    if attempt.driver_host is None:
        return [failure_message(MSG_DRIVER_HOST_UNREADABLE, f"{result.name} in {phase_label}")]

    driver_host = attempt.driver_host.lower()
    substitute = (attempt.substitute_host or "").lower()
    detail = (f"{result.name} in {phase_label} dialled {result.probe_host}, listed substitute {attempt.substitute_host}, "
              f"driver host {attempt.driver_host}, {writer_ips}")
    if driver_host == substitute:
        return []
    if driver_host == result.probe_host.lower():
        return [failure_message(_ROUTING_MESSAGES[phase_label], detail)]
    return [failure_message(MSG_UNEXPECTED_HOST, detail)]


def join_all(threads: Sequence[Thread], sec: float) -> int:
    """Join every thread within one shared deadline; returns how many are still alive."""
    deadline_ns = perf_counter_ns() + int(sec * 1_000_000_000)
    for thread in threads:
        remaining = (deadline_ns - perf_counter_ns()) / 1_000_000_000
        if remaining <= 0:
            break
        thread.join(remaining)
    return sum(1 for thread in threads if thread.is_alive())


def wait_for_bg_deployment_status(
        test_utility: Any,
        bg_id: str,
        deadline_ns: int,
        poll_sec: float = BG_API_POLL_SEC) -> Optional[str]:
    """Poll the RDS API until the deployment reports a finished switchover or the deadline passes. Never raises."""
    status: Optional[str] = None
    while True:
        try:
            deployment = test_utility.get_blue_green_deployment(bg_id)
            status = None if deployment is None else deployment.get("Status")
        except Exception as e:
            print(f"Blue/Green deployment status read failed: {describe_error(e)}")
            status = None
        if status in ("SWITCHOVER_COMPLETED", "SWITCHOVER_FAILED"):
            return status
        remaining = (deadline_ns - perf_counter_ns()) / 1_000_000_000
        if remaining <= 0:
            print(f"Blue/Green deployment status still {status} at the API wait deadline.")
            return status
        sleep(min(poll_sec, remaining))


def settle_switchover(
        test_utility: Any,
        bg_id: str,
        trigger_thread: Thread,
        stop: Event,
        switchover_requested: Event,
        switchover_requested_ns: AtomicInt) -> None:
    """Stop the trigger if it has not requested the switchover, else wait for the RDS API to finish it. Always sets stop."""
    try:
        if not switchover_requested.is_set():
            stop.set()
            trigger_thread.join(TRIGGER_JOIN_SEC)
        if switchover_requested.is_set():
            wait_for_bg_deployment_status(
                test_utility, bg_id, switchover_requested_ns.get() + BG_API_WAIT_SEC * 1_000_000_000)
    finally:
        stop.set()


def summary_lines(
        test_start_ns: int,
        threads_start_ns: int,
        switchover_requested_ns: int,
        wait_result: str,
        observation: PluginStatusObservation,
        probes: Sequence[ProbeResult],
        extra: Sequence[str] = ()) -> List[str]:
    zero_ns = switchover_requested_ns if switchover_requested_ns > 0 else threads_start_ns
    zero_name = "switchover request" if switchover_requested_ns > 0 else "thread start"
    lines = [
        "#1352 checks summary",
        f"  test entry to thread start: {_sec(threads_start_ns - test_start_ns)}",
        f"  probe host: {observation.probe_host} ({observation.probe_host_choice or 'not chosen'})",
        f"  wait: {wait_result}",
        f"  observer error: {observation.error or '-'}",
    ]
    if observation.provider_monitors_running is not None:
        lines.append(f"  status provider monitors running at the end of the window: {observation.provider_monitors_running}")
    lines.extend(f"  {line}" for line in extra)
    for phase in PHASE_ORDER:
        seen_ns = observation.plugin_phase_first_seen_ns.get(phase)
        offset = "not seen" if seen_ns is None else f"{_sec(seen_ns - zero_ns)} after {zero_name}"
        lines.append(f"  plugin phase {phase}: {offset}")
    for probe in probes:
        timing = "not started" if probe.start_ns == 0 else f"start {_sec(probe.start_ns - zero_ns)}, " + (
            "in flight" if probe.end_ns == 0 else f"end {_sec(probe.end_ns - zero_ns)}")
        line = f"  {probe.name}: host {probe.probe_host or '-'}, phase at start {probe.phase_at_start}, "
        if probe.phase_at_end is not None:
            line += f"phase at end {probe.phase_at_end}, "
        line += (f"{timing}, hold {_sec(probe.hold_ns)}, thread CPU {probe.cpu_sec:.3f} s, "
                 f"outcome {probe.outcome or '-'}")
        if probe.suspend_detail:
            line += f", suspend listed {probe.suspend_listed} {probe.suspend_detail}"
        if probe.routing_detail:
            line += f", {probe.routing_detail}"
        if probe.spinning is not None:
            line += (f", spinning {probe.spinning}, sample CPU {probe.spin_cpu_delta_sec}, "
                     f"frames {probe.frames[0]} / {probe.frames[1]}")
        lines.append(line)
        for index, attempt in enumerate(probe.attempts, 1):
            lines.append(f"    attempt {index} in {attempt.phase}: substitute {attempt.substitute_host}, "
                         f"driver host {attempt.driver_host}, outcome {attempt.outcome or '-'}")
    return lines
