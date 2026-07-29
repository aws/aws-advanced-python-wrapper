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
