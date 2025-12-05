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

import asyncio

import pytest
import pytest_asyncio
from tortoise.transactions import atomic, in_transaction

from tests.integration.container.tortoise.models.test_models import (
    UniqueName, User)
from tests.integration.container.tortoise.test_tortoise_common import \
    setup_tortoise
from tests.integration.container.utils.conditions import (disable_on_engines,
                                                          disable_on_features)
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.test_environment_features import \
    TestEnvironmentFeatures


@disable_on_engines([DatabaseEngine.PG])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestTortoiseBasic:
    """
    Test class for Tortoise ORM integration with AWS Advanced Python Wrapper.
    Contains tests related to basic test operations.
    """
    
    @pytest_asyncio.fixture
    async def setup_tortoise_basic(self, conn_utils):
        """Setup Tortoise with default plugins."""
        async for result in setup_tortoise(conn_utils):
            yield result
    


    @pytest.mark.asyncio
    async def test_basic_crud_operations(self, setup_tortoise_basic):
        """Test basic CRUD operations with AWS wrapper."""
        # Create
        user = await User.create(name="John Doe", email="john@example.com")
        assert user.id is not None
        assert user.name == "John Doe"
        
        # Read
        found_user = await User.get(id=user.id)
        assert found_user.name == "John Doe"
        assert found_user.email == "john@example.com"
        
        # Update
        found_user.name = "Jane Doe"
        await found_user.save()
        
        updated_user = await User.get(id=user.id)
        assert updated_user.name == "Jane Doe"
        
        # Delete
        await updated_user.delete()
        
        with pytest.raises(Exception):
            await User.get(id=user.id)

    @pytest.mark.asyncio
    async def test_basic_crud_operations_config(self, setup_tortoise_basic):
        """Test basic CRUD operations with AWS wrapper."""
        # Create
        user = await User.create(name="John Doe", email="john@example.com")
        assert user.id is not None
        assert user.name == "John Doe"
        
        # Read
        found_user = await User.get(id=user.id)
        assert found_user.name == "John Doe"
        assert found_user.email == "john@example.com"
        
        # Update
        found_user.name = "Jane Doe"
        await found_user.save()
        
        updated_user = await User.get(id=user.id)
        assert updated_user.name == "Jane Doe"
        
        # Delete
        await updated_user.delete()
        
        with pytest.raises(Exception):
            await User.get(id=user.id)

    @pytest.mark.asyncio
    async def test_transaction_support(self, setup_tortoise_basic):
        """Test transaction handling with AWS wrapper."""
        async with in_transaction() as conn:
            await User.create(name="User 1", email="user1@example.com", using_db=conn)
            await User.create(name="User 2", email="user2@example.com", using_db=conn)
            
            # Verify users exist within transaction
            users = await User.filter(name__in=["User 1", "User 2"]).using_db(conn)
            assert len(users) == 2

    @pytest.mark.asyncio
    async def test_transaction_rollback(self, setup_tortoise_basic):
        """Test transaction rollback with AWS wrapper."""
        try:
            async with in_transaction() as conn:
                await User.create(name="Test User", email="test@example.com", using_db=conn)
                # Force rollback by raising exception
                raise ValueError("Test rollback")
        except ValueError:
            pass
        
        # Verify user was not created due to rollback
        users = await User.filter(name="Test User")
        assert len(users) == 0

    @pytest.mark.asyncio
    async def test_bulk_operations(self, setup_tortoise_basic):
        """Test bulk operations with AWS wrapper."""
        # Bulk create
        users_data = [
            {"name": f"User {i}", "email": f"user{i}@example.com"}
            for i in range(5)
        ]
        await User.bulk_create([User(**data) for data in users_data])
        
        # Verify bulk creation
        users = await User.filter(name__startswith="User")
        assert len(users) == 5
        
        # Bulk update
        await User.filter(name__startswith="User").update(name="Updated User")
        
        updated_users = await User.filter(name="Updated User")
        assert len(updated_users) == 5

    @pytest.mark.asyncio
    async def test_query_operations(self, setup_tortoise_basic):
        """Test various query operations with AWS wrapper."""
        # Setup test data
        await User.create(name="Alice", email="alice@example.com")
        await User.create(name="Bob", email="bob@example.com")
        await User.create(name="Charlie", email="charlie@example.com")
        
        # Test filtering
        alice = await User.get(name="Alice")
        assert alice.email == "alice@example.com"
        
        # Test count
        count = await User.all().count()
        assert count >= 3
        
        # Test ordering
        users = await User.all().order_by("name")
        assert users[0].name == "Alice"
        
        # Test exists
        exists = await User.filter(name="Alice").exists()
        assert exists is True
        
        # Test values
        emails = await User.all().values_list("email", flat=True)
        assert "alice@example.com" in emails

    @pytest.mark.asyncio
    async def test_bulk_create_with_ids(self, setup_tortoise_basic):
        """Test bulk create operations with ID verification."""
        # Bulk create 1000 UniqueName objects with no name (null values)
        await UniqueName.bulk_create([UniqueName() for _ in range(1000)])
        
        # Get all created records with id and name
        all_ = await UniqueName.all().values("id", "name")
        
        # Get the starting ID
        inc = all_[0]["id"]
        
        # Sort by ID for comparison
        all_sorted = sorted(all_, key=lambda x: x["id"])
        
        # Verify the IDs are sequential and names are None
        expected = [{"id": val + inc, "name": None} for val in range(1000)]
        
        assert len(all_sorted) == 1000
        assert all_sorted == expected

    @pytest.mark.asyncio
    async def test_concurrency_read(self, setup_tortoise_basic):
        """Test concurrent read operations with AWS wrapper."""
        
        await User.create(name="Test User", email="test@example.com")
        user1 = await User.first()
        
        # Perform 100 concurrent reads
        all_read = await asyncio.gather(*[User.first() for _ in range(100)])
        
        # All reads should return the same user
        assert all_read == [user1 for _ in range(100)]

    @pytest.mark.asyncio
    async def test_concurrency_create(self, setup_tortoise_basic):
        """Test concurrent create operations with AWS wrapper."""
        
        # Perform 100 concurrent creates with unique emails
        all_write = await asyncio.gather(*[
            User.create(name="Test", email=f"test{i}@example.com") 
            for i in range(100)
        ])
        
        # Read all created users
        all_read = await User.all()
        
        # All created users should exist in the database
        assert set(all_write) == set(all_read)

    @pytest.mark.asyncio
    async def test_atomic_decorator(self, setup_tortoise_basic):
        """Test atomic decorator for transaction handling with AWS wrapper."""
        
        @atomic()
        async def create_users_atomically():
            user1 = await User.create(name="Atomic User 1", email="atomic1@example.com")
            user2 = await User.create(name="Atomic User 2", email="atomic2@example.com")
            return user1, user2
        
        # Execute atomic operation
        user1, user2 = await create_users_atomically()
        
        # Verify both users were created
        assert user1.id is not None
        assert user2.id is not None
        
        # Verify users exist in database
        found_users = await User.filter(name__startswith="Atomic User")
        assert len(found_users) == 2

    @pytest.mark.asyncio
    async def test_atomic_decorator_rollback(self, setup_tortoise_basic):
        """Test atomic decorator rollback on exception with AWS wrapper."""
        
        @atomic()
        async def create_users_with_error():
            await User.create(name="Atomic Rollback User", email="rollback@example.com")
            # Force rollback by raising exception
            raise ValueError("Intentional error for rollback test")
        
        # Execute atomic operation that should fail
        with pytest.raises(ValueError):
            await create_users_with_error()
        
        # Verify user was not created due to rollback
        users = await User.filter(name="Atomic Rollback User")
        assert len(users) == 0
