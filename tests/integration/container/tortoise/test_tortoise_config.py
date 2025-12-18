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

import pytest
import pytest_asyncio
from tortoise import Tortoise, connections

# Import to register the aws-mysql backend
import aws_advanced_python_wrapper.tortoise_orm  # noqa: F401
from tests.integration.container.tortoise.models.test_models import User
from tests.integration.container.tortoise.test_tortoise_common import \
    reset_tortoise
from tests.integration.container.utils.conditions import (
    disable_on_deployments, disable_on_engines, disable_on_features)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.database_engine_deployment import \
    DatabaseEngineDeployment
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@disable_on_engines([DatabaseEngine.PG])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseConfig:
    """Test class for Tortoise ORM configuration scenarios."""

    async def _clear_all_test_models(self):
        """Clear all test models for a specific connection."""
        await User.all().delete()

    @pytest_asyncio.fixture
    async def setup_tortoise_dict_config(self, conn_utils):
        """Setup Tortoise with dictionary configuration instead of URL."""
        # Ensure clean state
        host = conn_utils.writer_cluster_host if conn_utils.writer_cluster_host else conn_utils.writer_host
        config = {
            "connections": {
                "default": {
                    "engine": "aws_advanced_python_wrapper.tortoise_orm.backends.mysql",
                    "credentials": {
                        "host": host,
                        "port": conn_utils.port,
                        "user": conn_utils.user,
                        "password": conn_utils.password,
                        "database": conn_utils.dbname,
                        "plugins": "aurora_connection_tracker",
                    }
                }
            },
            "apps": {
                "models": {
                    "models": ["tests.integration.container.tortoise.models.test_models"],
                    "default_connection": "default",
                }
            }
        }

        await Tortoise.init(config=config)
        await Tortoise.generate_schemas()
        await self._clear_all_test_models()

        yield

        await self._clear_all_test_models()
        await reset_tortoise()

    @pytest_asyncio.fixture
    async def setup_tortoise_multi_db(self, conn_utils):
        """Setup Tortoise with two different databases using same backend."""
        # Create second database name
        original_db = conn_utils.dbname
        second_db = f"{original_db}_test2"
        host = conn_utils.writer_cluster_host if conn_utils.writer_cluster_host else conn_utils.writer_host
        config = {
            "connections": {
                "default": {
                    "engine": "aws_advanced_python_wrapper.tortoise_orm.backends.mysql",
                    "credentials": {
                        "host": host,
                        "port": conn_utils.port,
                        "user": conn_utils.user,
                        "password": conn_utils.password,
                        "database": original_db,
                        "plugins": "aurora_connection_tracker",
                    }
                },
                "second_db": {
                    "engine": "aws_advanced_python_wrapper.tortoise_orm.backends.mysql",
                    "credentials": {
                        "host": host,
                        "port": conn_utils.port,
                        "user": conn_utils.user,
                        "password": conn_utils.password,
                        "database": second_db,
                        "plugins": "aurora_connection_tracker"
                    }
                }
            },
            "apps": {
                "models": {
                    "models": ["tests.integration.container.tortoise.models.test_models"],
                    "default_connection": "default",
                },
                "models2": {
                    "models": ["tests.integration.container.tortoise.models.test_models_copy"],
                    "default_connection": "second_db",
                }
            }
        }

        await Tortoise.init(config=config)

        # Create second database
        conn = connections.get("second_db")
        try:
            await conn.db_create()
        except Exception:
            pass

        await Tortoise.generate_schemas()

        await self._clear_all_test_models()

        yield second_db
        await self._clear_all_test_models()

        # Drop second database
        conn = connections.get("second_db")
        await conn.db_delete()
        await reset_tortoise()

    @pytest_asyncio.fixture
    async def setup_tortoise_with_router(self, conn_utils):
        """Setup Tortoise with router configuration."""
        host = conn_utils.writer_cluster_host if conn_utils.writer_cluster_host else conn_utils.writer_host
        config = {
            "connections": {
                "default": {
                    "engine": "aws_advanced_python_wrapper.tortoise_orm.backends.mysql",
                    "credentials": {
                        "host": host,
                        "port": conn_utils.port,
                        "user": conn_utils.user,
                        "password": conn_utils.password,
                        "database": conn_utils.dbname,
                        "plugins": "aurora_connection_tracker"
                    }
                }
            },
            "apps": {
                "models": {
                    "models": ["tests.integration.container.tortoise.models.test_models"],
                    "default_connection": "default",
                }
            },
            "routers": ["tests.integration.container.tortoise.router.test_router.TestRouter"]
        }

        await Tortoise.init(config=config)
        await Tortoise.generate_schemas()
        await self._clear_all_test_models()

        yield

        await self._clear_all_test_models()
        await reset_tortoise()

    @pytest.mark.asyncio
    async def test_dict_config_read_operations(self, setup_tortoise_dict_config):
        """Test basic read operations with dictionary configuration."""
        # Create test data
        user = await User.create(name="Dict Config User", email="dict@example.com")

        # Read operations
        found_user = await User.get(id=user.id)
        assert found_user.name == "Dict Config User"
        assert found_user.email == "dict@example.com"

        # Query operations
        users = await User.filter(name="Dict Config User")
        assert len(users) == 1
        assert users[0].id == user.id

    @pytest.mark.asyncio
    async def test_dict_config_write_operations(self, setup_tortoise_dict_config):
        """Test basic write operations with dictionary configuration."""
        # Create
        user = await User.create(name="Dict Write Test", email="dictwrite@example.com")
        assert user.id is not None

        # Update
        user.name = "Updated Dict User"
        await user.save()

        updated_user = await User.get(id=user.id)
        assert updated_user.name == "Updated Dict User"

        # Delete
        await updated_user.delete()

        with pytest.raises(Exception):
            await User.get(id=user.id)

    @disable_on_deployments([DatabaseEngineDeployment.DOCKER])
    @pytest.mark.asyncio
    async def test_multi_db_operations(self, setup_tortoise_multi_db):
        """Test operations with multiple databases using same backend."""
        # Get second connection
        second_conn = connections.get("second_db")

        # Create users in different databases
        user1 = await User.create(name="DB1 User", email="db1@example.com")
        user2 = await User.create(name="DB2 User", email="db2@example.com", using_db=second_conn)

        # Verify users exist in their respective databases
        db1_users = await User.all()
        db2_users = await User.all().using_db(second_conn)

        assert len(db1_users) == 1
        assert len(db2_users) == 1
        assert db1_users[0].name == "DB1 User"
        assert db2_users[0].name == "DB2 User"

        # Verify isolation - users don't exist in the other database
        db1_user_in_db2 = await User.filter(name="DB1 User").using_db(second_conn)
        db2_user_in_db1 = await User.filter(name="DB2 User")

        assert len(db1_user_in_db2) == 0
        assert len(db2_user_in_db1) == 0

        # Test updates in different databases
        user1.name = "Updated DB1 User"
        await user1.save()

        user2.name = "Updated DB2 User"
        await user2.save(using_db=second_conn)

        # Verify updates
        updated_user1 = await User.get(id=user1.id)
        updated_user2 = await User.get(id=user2.id, using_db=second_conn)

        assert updated_user1.name == "Updated DB1 User"
        assert updated_user2.name == "Updated DB2 User"

    @pytest.mark.asyncio
    async def test_router_read_operations(self, setup_tortoise_with_router):
        """Test read operations with router configuration."""
        # Create test data
        user = await User.create(name="Router User", email="router@example.com")

        # Read operations (should be routed by router)
        found_user = await User.get(id=user.id)
        assert found_user.name == "Router User"
        assert found_user.email == "router@example.com"

        # Query operations
        users = await User.filter(name="Router User")
        assert len(users) == 1
        assert users[0].id == user.id

    @pytest.mark.asyncio
    async def test_router_write_operations(self, setup_tortoise_with_router):
        """Test write operations with router configuration."""
        # Create (should be routed by router)
        user = await User.create(name="Router Write Test", email="routerwrite@example.com")
        assert user.id is not None

        # Update (should be routed by router)
        user.name = "Updated Router User"
        await user.save()

        updated_user = await User.get(id=user.id)
        assert updated_user.name == "Updated Router User"

        # Delete (should be routed by router)
        await updated_user.delete()

        with pytest.raises(Exception):
            await User.get(id=user.id)
