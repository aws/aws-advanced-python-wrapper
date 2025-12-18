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
from tortoise import Tortoise
from tortoise.exceptions import IntegrityError

# Import to register the aws-mysql backend
import aws_advanced_python_wrapper.tortoise_orm  # noqa: F401
from tests.integration.container.tortoise.models.test_models_relationships import (
    RelTestAccount, RelTestAccountProfile, RelTestLearner, RelTestPublication,
    RelTestPublisher, RelTestSubject)
from tests.integration.container.tortoise.test_tortoise_common import \
    reset_tortoise
from tests.integration.container.utils.conditions import disable_on_engines
from tests.integration.container.utils.database_engine import DatabaseEngine
from tests.integration.container.utils.test_environment import TestEnvironment


async def clear_relationship_models():
    """Clear all relationship test models."""
    await RelTestSubject.all().delete()
    await RelTestLearner.all().delete()
    await RelTestPublication.all().delete()
    await RelTestPublisher.all().delete()
    await RelTestAccountProfile.all().delete()
    await RelTestAccount.all().delete()


@disable_on_engines([DatabaseEngine.PG])
class TestTortoiseRelationships:

    @pytest_asyncio.fixture(scope="function")
    async def setup_tortoise_relationships(self, conn_utils):
        """Setup Tortoise with relationship models."""
        db_url = conn_utils.get_aws_tortoise_url(
            TestEnvironment.get_current().get_engine(),
            plugins="aurora_connection_tracker",
        )

        config = {
            "connections": {
                "default": db_url
            },
            "apps": {
                "models": {
                    "models": ["tests.integration.container.tortoise.models.test_models_relationships"],
                    "default_connection": "default",
                }
            }
        }

        await Tortoise.init(config=config)
        await Tortoise.generate_schemas()

        # Clear any existing data
        await clear_relationship_models()

        yield

        # Cleanup
        await clear_relationship_models()
        await reset_tortoise()

    @pytest.mark.asyncio
    async def test_one_to_one_relationship_creation(self, setup_tortoise_relationships):
        """Test creating one-to-one relationships"""
        account = await RelTestAccount.create(username="testuser", email="test@example.com")
        profile = await RelTestAccountProfile.create(account=account, bio="Test bio", avatar_url="http://example.com/avatar.jpg")

        # Test forward relationship
        assert profile.account_id == account.id

        # Test reverse relationship
        account_with_profile = await RelTestAccount.get(id=account.id).prefetch_related("profile")
        assert account_with_profile.profile.bio == "Test bio"

    @pytest.mark.asyncio
    async def test_one_to_one_cascade_delete(self, setup_tortoise_relationships):
        """Test cascade delete in one-to-one relationship"""
        account = await RelTestAccount.create(username="deleteuser", email="delete@example.com")
        await RelTestAccountProfile.create(account=account, bio="Will be deleted")

        # Delete account should cascade to profile
        await account.delete()

        # Profile should be deleted
        profile_count = await RelTestAccountProfile.filter(account_id=account.id).count()
        assert profile_count == 0

    @pytest.mark.asyncio
    async def test_one_to_one_unique_constraint(self, setup_tortoise_relationships):
        """Test unique constraint in one-to-one relationship"""
        account = await RelTestAccount.create(username="uniqueuser", email="unique@example.com")
        await RelTestAccountProfile.create(account=account, bio="First profile")

        # Creating another profile for same account should fail
        with pytest.raises(IntegrityError):
            await RelTestAccountProfile.create(account=account, bio="Second profile")

    @pytest.mark.asyncio
    async def test_one_to_many_relationship_creation(self, setup_tortoise_relationships):
        """Test creating one-to-many relationships"""
        publisher = await RelTestPublisher.create(name="Test Publisher", email="publisher@example.com")
        pub1 = await RelTestPublication.create(title="Publication 1", isbn="1234567890123", publisher=publisher)
        pub2 = await RelTestPublication.create(title="Publication 2", isbn="1234567890124", publisher=publisher)

        # Test forward relationship
        assert pub1.publisher_id == publisher.id
        assert pub2.publisher_id == publisher.id

        # Test reverse relationship
        publisher_with_pubs = await RelTestPublisher.get(id=publisher.id).prefetch_related("publications")
        assert len(publisher_with_pubs.publications) == 2
        assert {pub.title for pub in publisher_with_pubs.publications} == {"Publication 1", "Publication 2"}

    @pytest.mark.asyncio
    async def test_one_to_many_cascade_delete(self, setup_tortoise_relationships):
        """Test cascade delete in one-to-many relationship"""
        publisher = await RelTestPublisher.create(name="Delete Publisher", email="deletepub@example.com")
        await RelTestPublication.create(title="Delete Pub 1", isbn="9999999999999", publisher=publisher)
        await RelTestPublication.create(title="Delete Pub 2", isbn="9999999999998", publisher=publisher)

        # Delete publisher should cascade to publications
        await publisher.delete()

        # Publications should be deleted
        pub_count = await RelTestPublication.filter(publisher_id=publisher.id).count()
        assert pub_count == 0

    @pytest.mark.asyncio
    async def test_foreign_key_constraint(self, setup_tortoise_relationships):
        """Test foreign key constraint enforcement"""
        # Creating publication with non-existent publisher should fail
        with pytest.raises(IntegrityError):
            await RelTestPublication.create(title="Orphan Publication", isbn="0000000000000", publisher_id=99999)

    @pytest.mark.asyncio
    async def test_many_to_many_relationship_creation(self, setup_tortoise_relationships):
        """Test creating many-to-many relationships"""
        learner1 = await RelTestLearner.create(name="Learner 1", learner_id="L001")
        learner2 = await RelTestLearner.create(name="Learner 2", learner_id="L002")
        subject1 = await RelTestSubject.create(name="Math 101", code="MATH101", credits=3)
        subject2 = await RelTestSubject.create(name="Physics 101", code="PHYS101", credits=4)

        # Add learners to subjects
        await subject1.learners.add(learner1, learner2)
        await subject2.learners.add(learner1)

        # Test forward relationship
        subject1_with_learners = await RelTestSubject.get(id=subject1.id).prefetch_related("learners")
        assert len(subject1_with_learners.learners) == 2
        assert {learner.name for learner in subject1_with_learners.learners} == {"Learner 1", "Learner 2"}

        # Test reverse relationship
        learner1_with_subjects = await RelTestLearner.get(id=learner1.id).prefetch_related("subjects")
        assert len(learner1_with_subjects.subjects) == 2
        assert {subject.name for subject in learner1_with_subjects.subjects} == {"Math 101", "Physics 101"}

    @pytest.mark.asyncio
    async def test_many_to_many_remove_relationship(self, setup_tortoise_relationships):
        """Test removing many-to-many relationships"""
        learner = await RelTestLearner.create(name="Remove Learner", learner_id="L003")
        subject = await RelTestSubject.create(name="Remove Subject", code="REM101", credits=2)

        # Add relationship
        await subject.learners.add(learner)
        subject_with_learners = await RelTestSubject.get(id=subject.id).prefetch_related("learners")
        assert len(subject_with_learners.learners) == 1

        # Remove relationship
        await subject.learners.remove(learner)
        subject_with_learners = await RelTestSubject.get(id=subject.id).prefetch_related("learners")
        assert len(subject_with_learners.learners) == 0

    @pytest.mark.asyncio
    async def test_many_to_many_clear_relationships(self, setup_tortoise_relationships):
        """Test clearing all many-to-many relationships"""
        learner1 = await RelTestLearner.create(name="Clear Learner 1", learner_id="L004")
        learner2 = await RelTestLearner.create(name="Clear Learner 2", learner_id="L005")
        subject = await RelTestSubject.create(name="Clear Subject", code="CLR101", credits=1)

        # Add multiple relationships
        await subject.learners.add(learner1, learner2)
        subject_with_learners = await RelTestSubject.get(id=subject.id).prefetch_related("learners")
        assert len(subject_with_learners.learners) == 2

        # Clear all relationships
        await subject.learners.clear()
        subject_with_learners = await RelTestSubject.get(id=subject.id).prefetch_related("learners")
        assert len(subject_with_learners.learners) == 0

    @pytest.mark.asyncio
    async def test_unique_constraints(self, setup_tortoise_relationships):
        """Test unique constraints on model fields"""
        # Test account unique username
        await RelTestAccount.create(username="unique1", email="unique1@example.com")
        with pytest.raises(IntegrityError):
            await RelTestAccount.create(username="unique1", email="different@example.com")

        # Test publication unique ISBN
        publisher = await RelTestPublisher.create(name="ISBN Publisher", email="isbn@example.com")
        await RelTestPublication.create(title="ISBN Pub 1", isbn="1111111111111", publisher=publisher)
        with pytest.raises(IntegrityError):
            await RelTestPublication.create(title="ISBN Pub 2", isbn="1111111111111", publisher=publisher)

        # Test subject unique code
        await RelTestSubject.create(name="Unique Subject 1", code="UNQ101", credits=3)
        with pytest.raises(IntegrityError):
            await RelTestSubject.create(name="Unique Subject 2", code="UNQ101", credits=4)
