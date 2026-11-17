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

"""Central spec for the integration-test suite's failover-related timings.

These constants are *test-tuning* knobs — they exist to absorb Aurora's
control-plane lag and DNS-rotation races without flaking. Each one has a
rationale in the docstring below; please update the docstring when
adjusting a value, and keep tests importing from here rather than
re-introducing magic numbers at call sites.

If you find yourself adding a fifth-or-sixth tuning constant at a call
site, add it here instead.
"""

from __future__ import annotations

# ───── Cluster-RO DNS-routing race ─────────────────────────────────────
# Aurora's ``cluster-ro-*`` reader endpoint can briefly route a fresh TCP
# connect to the writer instance in the window right after the cluster's
# reader is first marked healthy. The wrapper's SRW cache pins the reader
# connection per ``AwsWrapperConnection``, so toggling ``read_only`` on
# the same wrapper doesn't reach the DNS layer twice. The test absorbs
# the race by retrying with brand-new top-level wrapper connections.
#
# 4 attempts at ~1 fresh-connect/iteration covers Aurora's documented
# "few seconds" routing race comfortably; an unhealthy cluster fails the
# fresh connect itself, which fails earlier with a useful error.
RWS_CLUSTER_RO_DNS_RETRY_ATTEMPTS: int = 4

# ───── Custom-endpoint monitor refresh ─────────────────────────────────
# Make the CustomEndpointMonitor poll the AWS RDS API every 2 s instead
# of the default 30 s. Without this, ``modify_db_cluster_endpoint`` in
# the test races the monitor: the test's wait helper confirms AWS-side
# endpoint update via direct RDS-API check, but the wrapper's monitor
# still has its previous (stale) member set when ``conn.read_only =
# False`` fires, so ReadWriteSplittingPlugin's writer-discovery fails
# and the test raises ReadWriteSplittingError instead of switching
# cleanly. 2 s is short enough to close the race, long enough to avoid
# hammering the RDS API in a tight loop.
CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS: int = 2_000

# ───── ``writer_changed`` SQL-probe loop ───────────────────────────────
# Inside ``RdsTestUtility.writer_changed`` we open a fresh raw driver
# connection through the cluster endpoint and poll. ``connect_timeout``
# bounds each individual connect attempt (Aurora can sit on a half-open
# TCP for ~10 s under load if we don't cap this). ``poll_interval`` is
# the sleep between successive probes.
WRITER_CHANGED_PROBE_CONNECT_TIMEOUT_SEC: int = 5
WRITER_CHANGED_PROBE_POLL_INTERVAL_SEC: float = 2.0

# ───── Failover helper deadline ────────────────────────────────────────
# ``RdsTestUtility.failover_cluster_and_wait_until_writer_changed`` waits
# for a new writer, for the initial writer to restart as a reader, for the
# cluster endpoint's DNS and for the other instances. Every wait shares one
# deadline, so a cluster that stops answering for minutes fails one test
# instead of running past pytest's 600 s ``--timeout``, which ends the
# whole session. This bounds one call to about 420 s (one connect attempt
# can run past it); the setup hook's waits are separate. A normal failover
# takes 20 to 35 s.
FAILOVER_DEADLINE_SEC: float = 420
# How long one failover request gets to produce a new writer before the
# helper checks whether the initial writer is still the writer.
WRITER_CHANGE_TIMEOUT_SEC: float = 180
# Aurora can keep the initial writer as the writer, or make it the writer
# again. The helper then requests the failover again, at most this many times
# and only while this much of the deadline is left.
FAILOVER_MAX_REISSUES: int = 2
FAILOVER_REISSUE_MIN_REMAINING_SEC: float = 120
# After the failover the initial writer restarts and answers as a reader.
# If it keeps answering as the writer for this long without a break, it is
# still, or again, the writer. Before a normal restart it answers as the
# writer for well under a minute.
INITIAL_WRITER_REVERT_WINDOW_SEC: float = 60
INITIAL_WRITER_POLL_INTERVAL_SEC: float = 1
# Upper bounds of ``RdsTestUtility`` waits when no deadline is passed in:
# for the cluster to reach a status, and for each instance to accept a
# connection.
CLUSTER_STATUS_TIMEOUT_SEC: float = 600
INSTANCE_UP_TIMEOUT_SEC: float = 300

# ───── RDS events on a failed test ────────────────────────────────────
# When a test fails on an Aurora or Multi-AZ DB cluster, the conftest logs
# the cluster's and its instances' RDS events from this far back, to show
# whether RDS restarted or failed over an instance around the failure. It covers one
# failover helper call plus the test's own work. Each RDS API call gets a
# short timeout and one attempt, the lookup stops at the first failed call,
# and it reads no more sources once its budget is spent, so it adds well
# under a minute to the failed test.
RDS_EVENTS_LOOKBACK_SEC: float = 900
RDS_EVENTS_CALL_TIMEOUT_SEC: float = 5
RDS_EVENTS_BUDGET_SEC: float = 20

# ───── Rerun after a cluster outage ───────────────────────────────────
# On Aurora and Multi-AZ DB clusters, a test that failed because the
# cluster stopped accepting connections is run once more after this delay,
# so the instances can come back first.
CLUSTER_OUTAGE_RERUN_DELAY_SEC: float = 60

# ───── Long-running failover-during-transaction test ───────────────────
# ``test_writer_fail_within_transaction_start_transaction`` exercises a
# rare-but-realistic pathology: a writer instance failing mid-transaction
# on a multi-instance Aurora cluster, where the wrapper has to detect
# the broken connection, drive ReaderFailoverHandler + WriterFailoverHandler,
# wait for promotion, and re-bind the session. End-to-end this is bounded
# by ``failover_timeout_sec`` (default 300 s) but the test combines that
# with extra topology-refresh polling, so 900 s is a sensible ceiling
# that catches genuine hangs without hiding real timeouts.
FAILOVER_WITHIN_TRANSACTION_PYTEST_TIMEOUT_SEC: int = 900
