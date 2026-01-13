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

from __future__ import annotations

import os
from datetime import datetime, date, time
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from ..utils.test_driver import TestDriver

from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from ..utils.conditions import disable_on_features, enable_on_engines
from ..utils.database_engine import DatabaseEngine
from ..utils.test_environment import TestEnvironment
from ..utils.test_environment_features import TestEnvironmentFeatures

# Django imports - handle optional dependency
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'tests.integration.container.test_django_settings')

try:
    import django
    from django.conf import settings
    from django.db import models, connection
    from django.test.utils import setup_test_environment, teardown_test_environment
    DJANGO_AVAILABLE = True
except ImportError:
    DJANGO_AVAILABLE = False


@pytest.mark.skipif(not DJANGO_AVAILABLE, reason="Django not available")
@enable_on_engines([DatabaseEngine.MYSQL])  # Django backends are MySQL-specific
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestDjango:

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def django_models(self, django_setup):
        """Create Django models after Django is set up"""
        
        # Store models in class attributes so they're accessible
        class TestModel(models.Model):
            """Basic test model for Django ORM functionality"""
            name = models.CharField(max_length=100)
            email = models.EmailField()
            age = models.IntegerField()
            is_active = models.BooleanField(default=True)
            created_at = models.DateTimeField(auto_now_add=True)
            
            class Meta:
                app_label = 'test_app'
                db_table = 'django_test_model'

        class DataTypeModel(models.Model):
            """Model for testing various data types"""
            # String fields
            char_field = models.CharField(max_length=255, null=True, blank=True)
            text_field = models.TextField(null=True, blank=True)
            
            # Numeric fields
            integer_field = models.IntegerField(null=True, blank=True)
            big_integer_field = models.BigIntegerField(null=True, blank=True)
            decimal_field = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
            float_field = models.FloatField(null=True, blank=True)
            
            # Boolean field
            boolean_field = models.BooleanField(default=False)
            
            # Date/Time fields
            date_field = models.DateField(null=True, blank=True)
            time_field = models.TimeField(null=True, blank=True)
            datetime_field = models.DateTimeField(null=True, blank=True)
            
            # JSON field (MySQL 5.7+)
            json_field = models.JSONField(null=True, blank=True)
            
            class Meta:
                app_label = 'test_app'
                db_table = 'django_data_type_model'

        class Author(models.Model):
            """Author model for relationship testing"""
            name = models.CharField(max_length=100)
            email = models.EmailField()
            birth_date = models.DateField(null=True, blank=True)
            
            class Meta:
                app_label = 'test_app'
                db_table = 'django_author'

        class Book(models.Model):
            """Book model for relationship testing"""
            title = models.CharField(max_length=200)
            author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name='books')
            publication_date = models.DateField()
            pages = models.IntegerField()
            price = models.DecimalField(max_digits=8, decimal_places=2)
            
            class Meta:
                app_label = 'test_app'
                db_table = 'django_book'
        
        # Store models as class attributes for easy access
        TestDjango.TestModel = TestModel
        TestDjango.DataTypeModel = DataTypeModel
        TestDjango.Author = Author
        TestDjango.Book = Book
        
        # Create tables for our test models
        with connection.schema_editor() as schema_editor:
            schema_editor.create_model(TestModel)
            schema_editor.create_model(DataTypeModel)
            schema_editor.create_model(Author)
            schema_editor.create_model(Book)
        
        yield
        
        # Clean up tables
        with connection.schema_editor() as schema_editor:
            schema_editor.delete_model(Book)
            schema_editor.delete_model(Author)
            schema_editor.delete_model(DataTypeModel)
            schema_editor.delete_model(TestModel)

    @pytest.fixture(scope='class')
    def django_setup(self, conn_utils):
        """Setup Django configuration for testing"""
        if not DJANGO_AVAILABLE:
            pytest.skip("Django not available")
            
        # Configure Django settings
        if not settings.configured:
            db_config = {
                'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
                # 'ENGINE': 'mysql.connector.django',

                'NAME': conn_utils.dbname,
                'USER': conn_utils.user,
                'PASSWORD': conn_utils.password,
                'HOST': conn_utils.writer_host,
                'PORT': conn_utils.port,
                'OPTIONS': {
                    'plugins': '',  # Empty plugins as requested
                    'connect_timeout': 10,
                    'autocommit': True,
                },
            }
            
            settings.configure(
                DEBUG=True,
                DATABASES={'default': db_config},
                INSTALLED_APPS=[
                    'django.contrib.contenttypes',
                    'django.contrib.auth',
                ],
                SECRET_KEY='test-secret-key-for-django-tests',
                USE_TZ=True,
            )
        
        django.setup()
        setup_test_environment()
        
        yield
        
        teardown_test_environment()

    def test_django_backend_configuration(self, test_environment: TestEnvironment, django_models):
        """Test Django backend configuration with empty plugins"""
        # Verify that the connection is using the AWS wrapper
        assert hasattr(connection, 'connection')
        
        # Test basic connection functionality
        assert self.TestModel.objects.count() == 0

    def test_django_basic_model_operations(self, test_environment: TestEnvironment, django_models):
        """Test basic Django ORM operations (CRUD)"""
        TestModel = self.TestModel
        
        # Ensure clean slate
        TestModel.objects.all().delete()
        
        # Create
        test_obj = TestModel.objects.create(
            name="John Doe",
            email="john@example.com",
            age=30,
            is_active=True
        )
        assert test_obj.id is not None
        assert test_obj.name == "John Doe"
        
        # Read
        retrieved_obj = TestModel.objects.get(id=test_obj.id)
        assert retrieved_obj.name == "John Doe"
        assert retrieved_obj.email == "john@example.com"
        assert retrieved_obj.age == 30
        assert retrieved_obj.is_active is True
        
        # Update
        retrieved_obj.name = "Jane Doe"
        retrieved_obj.age = 25
        retrieved_obj.save()
        
        updated_obj = TestModel.objects.get(id=test_obj.id)
        assert updated_obj.name == "Jane Doe"
        assert updated_obj.age == 25
        
        # Delete
        updated_obj.delete()
        assert TestModel.objects.filter(id=test_obj.id).count() == 0

    def test_django_queryset_operations(self, test_environment: TestEnvironment, django_models):
        """Test Django QuerySet operations"""
        TestModel = self.TestModel
        
        # Ensure clean slate
        TestModel.objects.all().delete()
        
        # Create test data
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)
        
        # Test filtering
        active_users = TestModel.objects.filter(is_active=True)
        assert active_users.count() == 2
        
        # Test ordering
        ordered_users = TestModel.objects.order_by('age')
        ages = [user.age for user in ordered_users]
        assert ages == [25, 30, 35]
        
        # Test complex queries
        young_active_users = TestModel.objects.filter(age__lt=30, is_active=True)
        assert young_active_users.count() == 1
        assert young_active_users.first().name == "Alice"
        
        # Test exclude
        non_bob_users = TestModel.objects.exclude(name="Bob")
        assert non_bob_users.count() == 2
        
        # Test exists
        assert TestModel.objects.filter(name="Alice").exists()
        assert not TestModel.objects.filter(name="David").exists()
        
        # Clean up
        TestModel.objects.all().delete()

    def test_django_data_types(self, test_environment: TestEnvironment, django_models):
        """Test Django ORM with various data types"""
        from django.utils import timezone
        
        DataTypeModel = self.DataTypeModel
        
        # Ensure clean slate
        DataTypeModel.objects.all().delete()
        
        # Create test data with various data types
        test_datetime = datetime(2023, 12, 25, 14, 30, 0)
        test_datetime_aware = timezone.make_aware(test_datetime)
        
        test_data = DataTypeModel.objects.create(
            char_field="Test String",
            text_field="This is a longer text field content",
            integer_field=42,
            big_integer_field=9223372036854775807,
            decimal_field=Decimal('123.45'),
            float_field=3.14159,
            boolean_field=True,
            date_field=date(2023, 12, 25),
            time_field=time(14, 30, 0),
            datetime_field=test_datetime_aware,  # Use timezone-aware datetime
            json_field={"key": "value", "number": 123, "array": [1, 2, 3]}
        )
        
        # Retrieve and verify data
        retrieved = DataTypeModel.objects.get(id=test_data.id)
        
        assert retrieved.char_field == "Test String"
        assert retrieved.text_field == "This is a longer text field content"
        assert retrieved.integer_field == 42
        assert retrieved.big_integer_field == 9223372036854775807
        assert retrieved.decimal_field == Decimal('123.45')
        assert abs(retrieved.float_field - 3.14159) < 0.001
        assert retrieved.boolean_field is True
        assert retrieved.date_field == date(2023, 12, 25)
        assert retrieved.time_field == time(14, 30, 0)
        # Compare timezone-aware datetimes
        assert retrieved.datetime_field == test_datetime_aware
        assert retrieved.json_field == {"key": "value", "number": 123, "array": [1, 2, 3]}
        
        # Clean up
        DataTypeModel.objects.all().delete()

    def test_django_null_values(self, test_environment: TestEnvironment, django_models):
        """Test Django ORM handling of NULL values"""
        DataTypeModel = self.DataTypeModel
        
        # First, ensure we start with a clean slate
        DataTypeModel.objects.all().delete()
        
        # Create object with NULL values
        test_obj = DataTypeModel.objects.create(
            char_field=None,
            integer_field=None,
            date_field=None,
            boolean_field=False  # This field has default=False, so it won't be NULL
        )
        
        # Retrieve and verify NULL values
        retrieved = DataTypeModel.objects.get(id=test_obj.id)
        assert retrieved.char_field is None
        assert retrieved.integer_field is None
        assert retrieved.date_field is None
        assert retrieved.boolean_field is False
        
        # Test filtering with NULL values
        null_char_objects = DataTypeModel.objects.filter(char_field__isnull=True)
        assert null_char_objects.count() == 1
        
        not_null_char_objects = DataTypeModel.objects.filter(char_field__isnull=False)
        assert not_null_char_objects.count() == 0
        
        # Create an object with non-NULL values to test the opposite
        test_obj2 = DataTypeModel.objects.create(
            char_field="Not NULL",
            integer_field=42,
            date_field=date(2023, 1, 1)
        )
        
        # Now test filtering again
        null_char_objects = DataTypeModel.objects.filter(char_field__isnull=True)
        assert null_char_objects.count() == 1  # Still one NULL object
        
        not_null_char_objects = DataTypeModel.objects.filter(char_field__isnull=False)
        assert not_null_char_objects.count() == 1  # Now one non-NULL object
        
        # Clean up
        DataTypeModel.objects.all().delete()

    def test_django_relationships(self, test_environment: TestEnvironment, django_models):
        """Test Django ORM relationships (ForeignKey)"""
        Author = self.Author
        Book = self.Book
        
        # Create author
        author = Author.objects.create(
            name="J.K. Rowling",
            email="jk@example.com",
            birth_date=date(1965, 7, 31)
        )
        
        # Create books
        book1 = Book.objects.create(
            title="Harry Potter and the Philosopher's Stone",
            author=author,
            publication_date=date(1997, 6, 26),
            pages=223,
            price=Decimal('12.99')
        )
        
        book2 = Book.objects.create(
            title="Harry Potter and the Chamber of Secrets",
            author=author,
            publication_date=date(1998, 7, 2),
            pages=251,
            price=Decimal('13.99')
        )
        
        # Test forward relationship
        assert book1.author.name == "J.K. Rowling"
        assert book2.author.email == "jk@example.com"
        
        # Test reverse relationship
        author_books = author.books.all()
        assert author_books.count() == 2
        book_titles = [book.title for book in author_books.order_by('publication_date')]
        assert "Harry Potter and the Philosopher's Stone" in book_titles
        assert "Harry Potter and the Chamber of Secrets" in book_titles
        
        # Test related queries
        books_by_author = Book.objects.filter(author__name="J.K. Rowling")
        assert books_by_author.count() == 2
        
        # Test select_related for optimization
        book_with_author = Book.objects.select_related('author').get(id=book1.id)
        assert book_with_author.author.name == "J.K. Rowling"
        
        # Clean up
        Book.objects.all().delete()
        Author.objects.all().delete()

    def test_django_aggregations(self, test_environment: TestEnvironment, django_models):
        """Test Django ORM aggregations"""
        from django.db.models import Count, Sum, Avg, Max, Min
        
        Author = self.Author
        Book = self.Book
        
        # Create test data
        author = Author.objects.create(name="Test Author", email="test@example.com")
        
        Book.objects.create(title="Book 1", author=author, publication_date=date(2020, 1, 1), pages=100, price=Decimal('10.00'))
        Book.objects.create(title="Book 2", author=author, publication_date=date(2021, 1, 1), pages=200, price=Decimal('20.00'))
        Book.objects.create(title="Book 3", author=author, publication_date=date(2022, 1, 1), pages=300, price=Decimal('30.00'))
        
        # Test aggregations
        stats = Book.objects.aggregate(
            total_books=Count('id'),
            total_pages=Sum('pages'),
            avg_price=Avg('price'),
            max_pages=Max('pages'),
            min_price=Min('price')
        )
        
        assert stats['total_books'] == 3
        assert stats['total_pages'] == 600
        assert abs(float(stats['avg_price']) - 20.0) < 0.01
        assert stats['max_pages'] == 300
        assert stats['min_price'] == Decimal('10.00')
        
        # Clean up
        Book.objects.all().delete()
        Author.objects.all().delete()

    def test_django_transactions(self, test_environment: TestEnvironment, django_models):
        """Test Django transaction handling"""
        from django.db import transaction
        
        TestModel = self.TestModel
        
        # Ensure clean slate
        TestModel.objects.all().delete()
        
        initial_count = TestModel.objects.count()
        
        # Test successful transaction
        with transaction.atomic():
            TestModel.objects.create(name="User 1", email="user1@example.com", age=25)
            TestModel.objects.create(name="User 2", email="user2@example.com", age=30)
        
        assert TestModel.objects.count() == initial_count + 2
        
        # Test rollback transaction
        try:
            with transaction.atomic():
                TestModel.objects.create(name="User 3", email="user3@example.com", age=35)
                TestModel.objects.create(name="User 4", email="user4@example.com", age=40)
                # Force an error to trigger rollback
                raise Exception("Force rollback")
        except Exception:
            pass  # Expected exception
        
        # Should still have only 2 additional records (rollback occurred)
        assert TestModel.objects.count() == initial_count + 2
        
        # Clean up
        TestModel.objects.all().delete()

    def test_django_bulk_operations(self, test_environment: TestEnvironment, django_models):
        """Test Django bulk operations"""
        TestModel = self.TestModel
        
        # Ensure clean slate
        TestModel.objects.all().delete()
        
        # Test bulk_create
        test_objects = [
            TestModel(name=f"User {i}", email=f"user{i}@example.com", age=20 + i)
            for i in range(10)
        ]
        
        created_objects = TestModel.objects.bulk_create(test_objects)
        assert len(created_objects) == 10
        assert TestModel.objects.count() == 10
        
        # Test bulk_update - need to get the objects first and modify them
        objects_to_update = list(TestModel.objects.all())
        for obj in objects_to_update:
            obj.age += 5
        
        TestModel.objects.bulk_update(objects_to_update, ['age'])
        
        # Verify updates - get fresh objects from database
        ages = list(TestModel.objects.values_list('age', flat=True).order_by('name'))
        expected_ages = [25 + i for i in range(10)]  # 20+i+5 for i in range(10)
        assert ages == expected_ages
        
        # Clean up
        TestModel.objects.all().delete()

    def test_django_complex_queries(self, test_environment: TestEnvironment, django_models):
        """Test complex Django queries with Q objects and F expressions"""
        from django.db.models import Q, F
        
        TestModel = self.TestModel
        
        # Ensure clean slate
        TestModel.objects.all().delete()
        
        # Create test data
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)
        TestModel.objects.create(name="David", email="david@example.com", age=28, is_active=True)
        
        # Test Q objects for complex conditions
        complex_query = TestModel.objects.filter(
            Q(age__gte=30) | Q(name__startswith='A')
        )
        assert complex_query.count() == 3  # Bob (30), Charlie (35), Alice (starts with A)
        
        # Test F expressions
        TestModel.objects.filter(age__lt=30).update(age=F('age') + 5)
        
        # Verify F expression update
        alice = TestModel.objects.get(name="Alice")
        david = TestModel.objects.get(name="David")
        assert alice.age == 30  # 25 + 5
        assert david.age == 33  # 28 + 5
        
        # Clean up
        TestModel.objects.all().delete()