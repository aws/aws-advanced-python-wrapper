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

"""Async topology queries and row parsing.

The async counterparts of the synchronous ``TopologyUtils`` family. A
topology-utils object owns one dialect's topology SQL and the parsing of its
rows, so the topology monitor can query a cluster without holding a host list
provider.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.hostinfo import HostInfo, HostRole, Topology
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties
from aws_advanced_python_wrapper.utils.rds_url_type import RdsUrlType
from aws_advanced_python_wrapper.utils.rds_utils import RdsUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.utils.properties import Properties

logger = Logger(__name__)


def _is_programming_error(ex: BaseException) -> bool:
    """True when ``ex`` is a PEP-249 ``ProgrammingError``.

    The topology query runs on the raw driver connection, so the exception is
    the driver's own ``ProgrammingError`` (psycopg or aiomysql) rather than the
    wrapper's. Match on the class name across the MRO so all of them are
    recognised; sync catches ``pep249.ProgrammingError`` directly.
    """
    from aws_advanced_python_wrapper.pep249 import \
        ProgrammingError as WrapperProgrammingError
    if isinstance(ex, WrapperProgrammingError):
        return True
    return any(cls.__name__ == "ProgrammingError" for cls in type(ex).__mro__)


class AsyncTopologyUtils(ABC):
    """Query a cluster's topology and turn the rows into hosts."""

    def __init__(
            self,
            props: Properties,
            topology_query: str,
            default_port: int) -> None:
        self._props = props
        self._topology_query = topology_query
        self._default_port = default_port
        self._rds_utils = RdsUtils()

        self.initial_host_info: HostInfo = HostInfo(
            host=str(props.get("host", "")), port=self._port_from_props(default_port))
        self.instance_template: Optional[HostInfo] = self._build_instance_template()

    def _port_from_props(self, default_port: int) -> int:
        try:
            return int(self._props["port"])
        except (KeyError, TypeError, ValueError):
            return default_port

    @property
    def topology_query(self) -> str:
        return self._topology_query

    def reconfigure(
            self, topology_query: str, default_port: Optional[int] = None) -> None:
        """Swap in a resolved dialect's query after the connection exists.

        The utils are built from the dialect guessed off the endpoint, which
        for a MySQL instance endpoint is PostgreSQL's query and port.
        """
        if topology_query:
            self._topology_query = topology_query
        if default_port:
            self._default_port = default_port

    async def query_for_topology(self, conn: Any) -> Topology:
        """Query ``conn`` for the cluster topology.

        Returns an empty topology when the query fails for a reason that a
        later attempt could recover from, so callers fall back to whatever is
        already stored. An invalid query raises instead: that is a
        configuration or dialect mismatch, not a transient failure.
        """
        # cursor() is inside the try on purpose: on a closed connection
        # psycopg raises from cursor() itself, not only from execute().
        try:
            return await self._query_for_topology(conn)
        except AwsWrapperError:
            raise
        except Exception as ex:
            if _is_programming_error(ex):
                raise AwsWrapperError(
                    Messages.get("RdsHostListProvider.InvalidQuery")) from ex
            logger.debug(
                "AsyncTopologyUtils.TopologyQueryFailed", type(self).__name__, ex)
            return ()

    @abstractmethod
    async def _query_for_topology(self, conn: Any) -> Topology:
        ...

    def create_host(
            self,
            host_id: str,
            is_writer: bool,
            last_update: Optional[datetime] = None,
            port: Optional[int] = None) -> HostInfo:
        host_info = HostInfo(
            host=self._host_from_host_id(host_id),
            port=port if port is not None else self._resolved_port(),
            role=HostRole.WRITER if is_writer else HostRole.READER,
            last_update_time=last_update,
            # host_id is how identify_connection correlates a live connection
            # back to its topology row; without it every cluster-endpoint
            # identification fails.
            host_id=str(host_id))
        host_info.add_alias(str(host_id))
        return host_info

    def _resolved_port(self) -> int:
        """The port topology hosts should use.

        An instance template may carry an explicit ``:port`` -- proxied test
        endpoints use ``?.<suffix>:8666`` -- and that port applies to every
        host in the topology. Otherwise the connection's own port, which is
        what sync uses: a cluster reached on a non-default port has hosts on
        that port, not on the dialect default.
        """
        template = self.instance_template
        if template is not None and template.is_port_specified():
            return template.port
        if self.initial_host_info.is_port_specified():
            return self.initial_host_info.port
        return self._default_port

    def _host_from_host_id(self, host_id: str) -> str:
        """Turn an instance id into a reachable host name.

        Falling back to the bare id would yield a name that does not resolve,
        so every reader and failover connect to a topology host would fail.
        """
        template = self.instance_template
        if template is not None and "?" in template.host:
            return template.host.replace("?", str(host_id))
        return str(host_id)

    def _build_instance_template(self) -> Optional[HostInfo]:
        """Resolve the instance host pattern once, as sync does on init.

        Prefers ``cluster_instance_host_pattern``; otherwise derives the
        pattern from the connection endpoint.
        """
        pattern = WrapperProperties.CLUSTER_INSTANCE_HOST_PATTERN.get(self._props)
        if pattern and "?" not in pattern:
            # An explicit pattern with no '?' placeholder is a configuration
            # error; report it rather than quietly deriving a different one.
            self._validate_host_pattern(pattern.split(":", 1)[0])
        if not (pattern and "?" in pattern):
            derived = self._rds_utils.get_rds_instance_host_pattern(
                str(self._props.get("host", "")))
            pattern = derived if derived and "?" in derived else None
        if not pattern:
            return None

        port = HostInfo.NO_PORT
        if ":" in pattern:
            pattern, _, port_str = pattern.rpartition(":")
            try:
                port = int(port_str)
            except ValueError:
                port = HostInfo.NO_PORT
        self._validate_host_pattern(pattern)
        return HostInfo(host=pattern, port=port)

    def _validate_host_pattern(self, host: str) -> None:
        if not self._rds_utils.is_dns_pattern_valid(host):
            logger.error("RdsHostListProvider.InvalidPattern")
            raise AwsWrapperError(Messages.get("RdsHostListProvider.InvalidPattern"))
        url_type = self._rds_utils.identify_rds_type(host)
        if url_type == RdsUrlType.RDS_PROXY:
            logger.error("RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSProxy")
            raise AwsWrapperError(Messages.get(
                "RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSProxy"))
        if url_type == RdsUrlType.RDS_CUSTOM_CLUSTER:
            logger.error("RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSCustom")
            raise AwsWrapperError(Messages.get(
                "RdsHostListProvider.ClusterInstanceHostPatternNotSupportedForRDSCustom"))

    @staticmethod
    async def _fetch_all(conn: Any, query: str) -> List[tuple]:
        """Run ``query`` on the raw driver connection and return its rows.

        The plugin pipeline is bypassed on purpose: topology queries run in a
        tight loop, and routing them through it would trigger failover retry
        on every refresh.
        """
        async with conn.cursor() as cur:
            await cur.execute(query)
            return list(await cur.fetchall())


class AsyncAuroraTopologyUtils(AsyncTopologyUtils):
    """Aurora topology from ``aurora_replica_status``."""

    async def _query_for_topology(self, conn: Any) -> Topology:
        rows = await self._fetch_all(conn, self._topology_query)
        return self._process_query_results(rows)

    def _process_query_results(self, rows: List[tuple]) -> Topology:
        hosts_by_host: Dict[str, HostInfo] = {}
        for row in rows:
            if not row:
                continue
            host_id = row[0]
            is_writer = bool(row[1]) if len(row) > 1 else False
            # LAST_UPDATE_TIMESTAMP may arrive as a fifth column; it
            # disambiguates multiple writer rows during a failover window.
            last_update = (row[4] if len(row) > 4 and isinstance(row[4], datetime)
                           else None)
            host_info = self.create_host(host_id, is_writer, last_update)
            hosts_by_host[host_info.host] = host_info

        if not hosts_by_host:
            return ()
        writers = [h for h in hosts_by_host.values() if h.role == HostRole.WRITER]
        readers = [h for h in hosts_by_host.values() if h.role != HostRole.WRITER]
        if not writers:
            # A readers-only list is an invalid topology. Return empty so the
            # caller keeps using what is already stored.
            logger.debug("RdsHostListProvider.InvalidTopology")
            return ()
        return (self._verify_writer(writers), *readers)

    @staticmethod
    def _verify_writer(writers: List[HostInfo]) -> HostInfo:
        """Pick the current writer when the query reports more than one.

        During a failover window several rows can claim the writer role; the
        most recently updated one wins. Rows without a timestamp are only
        considered when none carry one.
        """
        if len(writers) == 1:
            return writers[0]
        timestamped = [w for w in writers if w.last_update_time is not None]
        logger.debug("AsyncTopologyUtils.MultipleWriters", len(writers))
        if timestamped:
            timestamped.sort(reverse=True,
                             key=lambda h: h.last_update_time or datetime.min)
            return timestamped[0]
        return writers[0]


class AsyncMultiAzTopologyUtils(AsyncTopologyUtils):
    """RDS Multi-AZ DB cluster topology.

    Two steps, matching sync: the writer host query names the writer, then the
    topology query returns ``(id, host, port)`` rows. An empty writer row means
    this connection is the writer (MySQL), so its own id is queried instead.
    """

    def __init__(
            self,
            props: Properties,
            topology_query: str,
            default_port: int,
            writer_host_query: str,
            host_id_query: str,
            writer_host_column_index: int = 0,
            instance_template_host: Optional[str] = None) -> None:
        super().__init__(props, topology_query, default_port)
        self._writer_host_query = writer_host_query
        self._host_id_query = host_id_query
        # The PG writer query returns a single column; MySQL's SHOW REPLICA
        # STATUS carries the source host at column 39.
        self._writer_host_column_index = writer_host_column_index
        self._instance_template_host = instance_template_host

    async def _query_for_topology(self, conn: Any) -> Topology:
        async with conn.cursor() as cur:
            await cur.execute(self._writer_host_query)
            writer_row = await cur.fetchone()
            if writer_row is not None:
                writer_id = str(writer_row[self._writer_host_column_index])
            else:
                await cur.execute(self._host_id_query)
                self_row = await cur.fetchone()
                if self_row is None:
                    return ()
                writer_id = str(self_row[0])

            await cur.execute(self._topology_query)
            rows = list(await cur.fetchall())
        return self._process_multi_az_query_results(rows, writer_id)

    def _process_multi_az_query_results(
            self, rows: List[tuple], writer_id: str) -> Topology:
        hosts: List[HostInfo] = []
        for row in rows:
            if len(row) < 3:
                continue
            row_id = str(row[0])
            host = str(row[1])
            try:
                port = int(row[2])
            except (TypeError, ValueError):
                port = self._default_port

            if self._instance_template_host:
                host, port = self._apply_instance_template(host, port)

            host_info = HostInfo(
                host=host,
                port=port,
                role=HostRole.WRITER if row_id == writer_id else HostRole.READER,
                host_id=row_id)
            host_info.add_alias(host)
            hosts.append(host_info)

        if not any(h.role == HostRole.WRITER for h in hosts):
            return ()
        hosts.sort(key=lambda h: 0 if h.role == HostRole.WRITER else 1)
        return tuple(hosts)

    def _apply_instance_template(self, host: str, port: int) -> Tuple[str, int]:
        instance_id = self._rds_utils.get_instance_id(host)
        if not instance_id or not self._instance_template_host:
            return host, port
        substituted = self._instance_template_host.replace("?", instance_id)
        if ":" in substituted:
            host_part, _, port_part = substituted.partition(":")
            return host_part, int(port_part) if port_part.isdigit() else port
        return substituted, port


class AsyncGlobalAuroraTopologyUtils(AsyncTopologyUtils):
    """Aurora Global Database topology, which spans regions.

    Rows are ``(server_id, is_writer, lag, aws_region)``. Each host is built
    from the template configured for its region, because an instance in another
    region does not share this connection's DNS suffix.
    """

    def __init__(
            self,
            props: Properties,
            topology_query: str,
            default_port: int,
            instance_templates_by_region: Optional[Dict[str, str]] = None) -> None:
        super().__init__(props, topology_query, default_port)
        if instance_templates_by_region is None:
            raw = WrapperProperties.GLOBAL_CLUSTER_INSTANCE_HOST_PATTERNS.get(props)
            instance_templates_by_region = (
                self.parse_instance_templates(raw) if raw else {})
        self._templates_by_region: Dict[str, str] = instance_templates_by_region

    @staticmethod
    def parse_instance_templates(raw: str) -> Dict[str, str]:
        """Parse ``region:host[:port],…`` into a mapping.

        Same format the sync provider accepts, so one
        ``global_cluster_instance_host_patterns`` value works for both. A pair
        without a host raises, as sync does.
        """
        result: Dict[str, str] = {}
        for pair in raw.split(","):
            pair = pair.strip()
            if not pair:
                continue
            parts = pair.split(":", 2)
            if len(parts) < 2 or not parts[0].strip() or not parts[1].strip():
                raise AwsWrapperError(Messages.get_formatted(
                    "GlobalAuroraTopologyUtils.invalidInstanceTemplate", pair))
            template = parts[1].strip()
            if len(parts) > 2 and parts[2].strip():
                template = f"{template}:{parts[2].strip()}"
            result[parts[0].strip()] = template
        return result

    async def _query_for_topology(self, conn: Any) -> Topology:
        rows = await self._fetch_all(conn, self._topology_query)
        return self._process_global_query_results(rows)

    def _process_global_query_results(self, rows: List[tuple]) -> Topology:
        hosts: List[HostInfo] = []
        for row in rows:
            if len(row) < 4:
                continue
            server_id = str(row[0])
            is_writer = bool(row[1])
            region = str(row[3])
            template = self._templates_by_region.get(region)
            if not template:
                logger.debug(
                    "AsyncTopologyUtils.NoTemplateForRegion", server_id, region)
                continue
            host, port = self._host_from_template(template, server_id)
            host_info = HostInfo(
                host=host,
                port=port,
                role=HostRole.WRITER if is_writer else HostRole.READER,
                host_id=server_id)
            host_info.add_alias(host)
            hosts.append(host_info)

        writers = [h for h in hosts if h.role == HostRole.WRITER]
        if not writers:
            return ()
        readers = [h for h in hosts if h.role != HostRole.WRITER]
        return (AsyncAuroraTopologyUtils._verify_writer(writers), *readers)

    def _host_from_template(self, template: str, server_id: str) -> Tuple[str, int]:
        host = template.replace("?", server_id)
        if ":" in host:
            host_part, _, port_part = host.rpartition(":")
            try:
                return host_part, int(port_part)
            except ValueError:
                # A non-numeric suffix is not a port; keep the whole string.
                return host, self._default_port
        return host, self._default_port
