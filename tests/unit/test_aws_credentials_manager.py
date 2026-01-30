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

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from time import sleep

import pytest
from boto3 import Session

from aws_advanced_python_wrapper.aws_credentials_manager import \
    AwsCredentialsManager
from aws_advanced_python_wrapper.hostinfo import HostInfo
from aws_advanced_python_wrapper.utils.atomic import AtomicInt
from aws_advanced_python_wrapper.utils.properties import Properties


@pytest.fixture(autouse=True)
def cleanup():
    AwsCredentialsManager.release_resources()
    AwsCredentialsManager.reset_custom_handler()
    yield
    AwsCredentialsManager.release_resources()
    AwsCredentialsManager.reset_custom_handler()


@pytest.fixture
def host_info():
    return HostInfo("foo.us-east-1.rds.amazonaws.com", 5432)


@pytest.fixture
def props():
    return Properties({})


@pytest.fixture
def region():
    return "us-east-1"


@pytest.fixture
def concurrent_counter():
    return AtomicInt()


@pytest.fixture
def counter():
    return AtomicInt()


@pytest.fixture
def num_threads():
    return 20


@pytest.fixture
def regions():
    return ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ca-central-1"]


@pytest.fixture
def mock_session(mocker):
    session = mocker.MagicMock(spec=Session)
    mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session', return_value=session)
    return session


@pytest.fixture
def mock_client(mocker, mock_session):
    mock_client = mocker.MagicMock()
    mocker.patch.object(mock_session, 'client', return_value=mock_client)
    return mock_client


class TestAwsCredentialsManagerBasic:
    def test_get_session_creates_default_session(self, mock_session, host_info, props, region):
        mock_session.region_name = region

        session = AwsCredentialsManager.get_session(host_info, props, region)

        assert session is not None
        assert session is mock_session
        assert session.region_name == region

    def test_get_session_caches_session(self, mock_session, host_info, props, region):
        session1 = AwsCredentialsManager.get_session(host_info, props, region)
        session2 = AwsCredentialsManager.get_session(host_info, props, region)

        assert session1 is session2

    def test_get_session_different_regions(self, host_info, props, mocker):
        mock_session1 = mocker.MagicMock(spec=Session)
        mock_session1.region_name = "us-east-1"
        mock_session2 = mocker.MagicMock(spec=Session)
        mock_session2.region_name = "us-west-2"

        mock_session_class = mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session')
        mock_session_class.side_effect = [mock_session1, mock_session2]

        session1 = AwsCredentialsManager.get_session(host_info, props, "us-east-1")
        session2 = AwsCredentialsManager.get_session(host_info, props, "us-west-2")

        assert session1 is not session2
        assert session1.region_name == "us-east-1"
        assert session2.region_name == "us-west-2"

    def test_get_session_different_hosts(self, props, region, mocker):
        host1 = HostInfo("host1.rds.amazonaws.com", 5432)
        host2 = HostInfo("host2.rds.amazonaws.com", 5432)

        mock_session1 = mocker.MagicMock(spec=Session)
        mock_session2 = mocker.MagicMock(spec=Session)

        mock_session_class = mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session')
        mock_session_class.side_effect = [mock_session1, mock_session2]

        session1 = AwsCredentialsManager.get_session(host1, props, region)
        session2 = AwsCredentialsManager.get_session(host2, props, region)

        assert session1 is not session2

    def test_get_session_with_custom_handler(self, mock_session, host_info, props, region, mocker):
        custom_session = mock_session
        custom_session.region_name = "custom-region"
        custom_handler = mocker.MagicMock(return_value=custom_session)

        AwsCredentialsManager.set_custom_handler(custom_handler)
        session = AwsCredentialsManager.get_session(host_info, props, region)

        assert session is custom_session
        custom_handler.assert_called_once_with(host_info, props)

    def test_reset_custom_handler(self, host_info, props, region, mocker):
        custom_session = mocker.MagicMock(spec=Session)
        custom_handler = mocker.MagicMock(return_value=custom_session)

        mock_default_session = mocker.MagicMock(spec=Session)
        mock_default_session.region_name = region
        mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session', return_value=mock_default_session)

        AwsCredentialsManager.set_custom_handler(custom_handler)
        AwsCredentialsManager.reset_custom_handler()
        session = AwsCredentialsManager.get_session(host_info, props, region)

        assert session is not custom_session
        assert session is mock_default_session
        assert session.region_name == region

    def test_get_client_creates_client(self, host_info, props, region, mocker):
        session = AwsCredentialsManager.get_session(host_info, props, region)
        mock_client = mocker.MagicMock()
        mocker.patch.object(session, "client", return_value=mock_client)

        client = AwsCredentialsManager.get_client("rds", session, host_info.host, region)

        assert client is mock_client
        session.client.assert_called_once_with(service_name="rds")

    def test_get_client_caches_client(self, mock_client, host_info, props, region):
        session = AwsCredentialsManager.get_session(host_info, props, region)

        client1 = AwsCredentialsManager.get_client("rds", session, host_info.host, region)
        client2 = AwsCredentialsManager.get_client("rds", session, host_info.host, region)

        assert client1 is client2
        session.client.assert_called_once()

    def test_get_client_different_services(self, host_info, props, region, mocker):
        session = AwsCredentialsManager.get_session(host_info, props, region)
        mock_rds_client = mocker.MagicMock()
        mock_secrets_client = mocker.MagicMock()

        def client_side_effect(service_name):
            if service_name == "rds":
                return mock_rds_client
            elif service_name == "secretsmanager":
                return mock_secrets_client

        mocker.patch.object(session, "client", side_effect=client_side_effect)

        rds_client = AwsCredentialsManager.get_client("rds", session, host_info.host, region)
        secrets_client = AwsCredentialsManager.get_client("secretsmanager", session, host_info.host, region)

        assert rds_client is mock_rds_client
        assert secrets_client is mock_secrets_client
        assert rds_client is not secrets_client

    def test_release_resources_clears_caches(self, host_info, props, region, mocker):
        mock_session1 = mocker.MagicMock(spec=Session)
        mock_session2 = mocker.MagicMock(spec=Session)
        mock_client = mocker.MagicMock()

        mock_session_class = mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session')
        mock_session_class.side_effect = [mock_session1, mock_session2]

        mock_session1.client.return_value = mock_client

        session = AwsCredentialsManager.get_session(host_info, props, region)
        AwsCredentialsManager.get_client("rds", session, host_info.host, region)

        AwsCredentialsManager.release_resources()

        new_session = AwsCredentialsManager.get_session(host_info, props, region)
        assert new_session is not mock_session1
        assert new_session is mock_session2

    def test_concurrent_get_session_same_host(self, mock_session, host_info, props, region, counter, concurrent_counter, num_threads):
        barrier = Barrier(num_threads)
        sessions = []

        def get_session_thread():
            barrier.wait()
            val = counter.get_and_increment()
            if val != 0:
                concurrent_counter.get_and_increment()

            session = AwsCredentialsManager.get_session(host_info, props, region)
            sessions.append(session)

            sleep(0.001)
            counter.get_and_decrement()

        with ThreadPoolExecutor(num_threads) as executor:
            futures = [executor.submit(get_session_thread) for _ in range(num_threads)]
            for future in futures:
                future.result()

        assert len(sessions) == num_threads
        assert all(session is sessions[0] for session in sessions)
        assert concurrent_counter.get() > 0

    def test_concurrent_get_session_different_hosts(self, props, region, counter, concurrent_counter, mocker, num_threads):
        barrier = Barrier(num_threads)
        results = []

        # One session per host
        mock_sessions = [mocker.MagicMock(spec=Session) for _ in range(num_threads)]
        mock_session_class = mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session')
        mock_session_class.side_effect = mock_sessions

        def get_session_thread(thread_id):
            barrier.wait()
            val = counter.get_and_increment()
            if val != 0:
                concurrent_counter.get_and_increment()

            host = HostInfo(f"host-{thread_id}.rds.amazonaws.com", 5432)
            session = AwsCredentialsManager.get_session(host, props, region)
            results.append((thread_id, session))

            sleep(0.001)
            counter.get_and_decrement()

        with ThreadPoolExecutor(num_threads) as executor:
            futures = [executor.submit(get_session_thread, i) for i in range(num_threads)]
            for future in futures:
                future.result()

        assert len(results) == num_threads
        sessions_by_id = {thread_id: session for thread_id, session in results}
        assert len(set(id(session) for session in sessions_by_id.values())) == num_threads
        assert concurrent_counter.get() > 0

    def test_concurrent_get_session_different_regions(self, num_threads, host_info, props, counter, concurrent_counter, regions, mocker):
        barrier = Barrier(num_threads)
        results = []

        # One session per region
        mock_sessions = {region: mocker.MagicMock(spec=Session) for region in regions}

        def session_factory(region_name=None, **kwargs):
            return mock_sessions[region_name]

        mocker.patch('aws_advanced_python_wrapper.aws_credentials_manager.Session', side_effect=session_factory)

        def get_session_thread(thread_id):
            barrier.wait()
            val = counter.get_and_increment()
            if val != 0:
                concurrent_counter.get_and_increment()

            region = regions[thread_id % len(regions)]
            session = AwsCredentialsManager.get_session(host_info, props, region)
            results.append((region, session))

            sleep(0.001)
            counter.get_and_decrement()

        with ThreadPoolExecutor(num_threads) as executor:
            futures = [executor.submit(get_session_thread, i) for i in range(num_threads)]
            for future in futures:
                future.result()

        # Group sessions by region
        sessions_by_region = {}
        for region, session in results:
            if region not in sessions_by_region:
                sessions_by_region[region] = []
            sessions_by_region[region].append(session)

        # All sessions for the same region should be identical
        for region, sessions in sessions_by_region.items():
            assert all(session is sessions[0] for session in sessions)

        # Sessions for different regions should be different
        unique_sessions = [sessions[0] for sessions in sessions_by_region.values()]
        assert len(set(id(s) for s in unique_sessions)) == len(regions)
        assert concurrent_counter.get() > 0

    def test_concurrent_get_client_same_parameters(self, host_info, props, region, counter, concurrent_counter, mocker):
        num_threads = 20
        barrier = Barrier(num_threads)
        clients = []

        session = AwsCredentialsManager.get_session(host_info, props, region)
        mock_client = mocker.MagicMock()
        mocker.patch.object(session, 'client', return_value=mock_client)

        def get_client_thread():
            barrier.wait()
            val = counter.get_and_increment()
            if val != 0:
                concurrent_counter.get_and_increment()

            client = AwsCredentialsManager.get_client("rds", session, host_info.host, region)
            clients.append(client)

            sleep(0.001)
            counter.get_and_decrement()

        with ThreadPoolExecutor(num_threads) as executor:
            futures = [executor.submit(get_client_thread) for _ in range(num_threads)]
            for future in futures:
                future.result()

        assert len(clients) == num_threads
        assert all(client is clients[0] for client in clients)
        session.client.assert_called_once_with(service_name="rds")
        assert concurrent_counter.get() > 0

    def test_concurrent_get_client_different_services(self, host_info, props, region, counter, concurrent_counter, mocker):
        num_threads = 20
        services = ["rds", "secretsmanager", "sts", "iam"]
        barrier = Barrier(num_threads)
        results = []

        session = AwsCredentialsManager.get_session(host_info, props, region)

        def client_side_effect(service_name):
            return mocker.MagicMock(name=f"{service_name}_client")

        mocker.patch.object(session, 'client', side_effect=client_side_effect)

        def get_client_thread(thread_id):
            barrier.wait()
            val = counter.get_and_increment()
            if val != 0:
                concurrent_counter.get_and_increment()

            service = services[thread_id % len(services)]
            client = AwsCredentialsManager.get_client(service, session, host_info.host, region)
            results.append((service, client))

            sleep(0.001)
            counter.get_and_decrement()

        with ThreadPoolExecutor(num_threads) as executor:
            futures = [executor.submit(get_client_thread, i) for i in range(num_threads)]
            for future in futures:
                future.result()

        # Group clients by service
        clients_by_service = {}
        for service, client in results:
            if service not in clients_by_service:
                clients_by_service[service] = []
            clients_by_service[service].append(client)

        # All clients for the same service should be identical
        for service, clients in clients_by_service.items():
            assert all(client is clients[0] for client in clients)

        # Clients for different services should be different
        unique_clients = [clients[0] for clients in clients_by_service.values()]
        assert len(set(id(c) for c in unique_clients)) == len(services)
        assert concurrent_counter.get() > 0

    def test_release_resources_closes_all_clients(self, host_info, props, region, mocker):
        session = AwsCredentialsManager.get_session(host_info, props, region)

        mock_rds_client = mocker.MagicMock()
        mock_secrets_client = mocker.MagicMock()
        mock_sts_client = mocker.MagicMock()

        def client_side_effect(service_name):
            if service_name == "rds":
                return mock_rds_client
            elif service_name == "secretsmanager":
                return mock_secrets_client
            elif service_name == "sts":
                return mock_sts_client

        mocker.patch.object(session, "client", side_effect=client_side_effect)

        rds_client = AwsCredentialsManager.get_client("rds", session, host_info.host, region)
        secrets_client = AwsCredentialsManager.get_client("secretsmanager", session, host_info.host, region)
        sts_client = AwsCredentialsManager.get_client("sts", session, host_info.host, region)

        assert rds_client is mock_rds_client
        assert secrets_client is mock_secrets_client
        assert sts_client is mock_sts_client

        AwsCredentialsManager.release_resources()

        mock_rds_client.close.assert_called_once()
        mock_secrets_client.close.assert_called_once()
        mock_sts_client.close.assert_called_once()
