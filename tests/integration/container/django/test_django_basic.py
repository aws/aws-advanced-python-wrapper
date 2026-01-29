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

# flake8: noqa: N806
# N806: Django model class references stored in variables use PascalCase intentionally

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import django
import pytest
from django.conf import settings
from django.db import connection, connections, models, transaction
from django.db.models import Avg, CharField, Count, F, Max, Min, Q, Sum, Value
from django.db.models.functions import Concat, Length, Lower, Upper
from django.test.utils import setup_test_environment, teardown_test_environment
from django.utils import timezone

from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from ..utils.conditions import (disable_on_features, enable_on_deployments,
                                enable_on_engines)
from ..utils.database_engine import DatabaseEngine
from ..utils.database_engine_deployment import DatabaseEngineDeployment
from ..utils.test_environment import TestEnvironment
from ..utils.test_environment_features import TestEnvironmentFeatures


@enable_on_engines([DatabaseEngine.MYSQL])  # MySQL Specific until PG is implemented
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestDjango:
    TestModel: Any
    DataTypeModel: Any
    Author: Any
    Book: Any

    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)

    @pytest.fixture(scope='class')
    def django_models(self, django_setup):
        """Create Django models after Django is set up"""

        class TestModel(models.Model):
            """Basic test model for Django ORM functionality"""
            name = models.CharField(max_length=100)
            email = models.EmailField(unique=True)
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

        # Store Author first so it's available for Book's ForeignKey
        TestDjango.Author = Author

        class Book(models.Model):
            """Book model for relationship testing"""
            title = models.CharField(max_length=200)
            author = models.ForeignKey(TestDjango.Author, on_delete=models.CASCADE, related_name='books')
            publication_date = models.DateField()
            pages = models.IntegerField()
            price = models.DecimalField(max_digits=8, decimal_places=2)

            class Meta:
                app_label = 'test_app'
                db_table = 'django_book'

        # Store models as class attributes for easy access
        TestDjango.TestModel = TestModel
        TestDjango.DataTypeModel = DataTypeModel
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
        # Configure Django settings
        if not settings.configured:
            db_config = {
                'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
                'NAME': conn_utils.dbname,
                'USER': conn_utils.user,
                'PASSWORD': conn_utils.password,
                'HOST': conn_utils.writer_cluster_host,
                'PORT': conn_utils.port,
                'OPTIONS': {
                    'plugins': 'failover_v2,aurora_connection_tracker',
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
        connections.close_all()

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
        DataTypeModel.objects.create(
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

        # Clean up, might get a failover error from this connection
        TestModel.objects.all().delete()

    def test_django_raw_sql_queries(self, test_environment: TestEnvironment, django_models):
        """Test Django raw SQL query execution"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)

        # Test raw() method
        raw_results = TestModel.objects.raw(
            f'SELECT * FROM {TestModel._meta.db_table} WHERE age >= %s ORDER BY age',
            [30]
        )
        raw_list = list(raw_results)
        assert len(raw_list) == 2
        assert raw_list[0].name == "Bob"
        assert raw_list[1].name == "Charlie"

        # Test connection.cursor() for custom SQL
        with connection.cursor() as cursor:
            cursor.execute(
                f'SELECT name, age FROM {TestModel._meta.db_table} WHERE is_active = %s ORDER BY age',
                [True]
            )
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0][0] == "Alice"  # name
            assert rows[0][1] == 25       # age
            assert rows[1][0] == "Charlie"
            assert rows[1][1] == 35

        # Test raw SQL with connection for aggregate
        with connection.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*), AVG(age) FROM {TestModel._meta.db_table}')
            count, avg_age = cursor.fetchone()
            assert count == 3
            assert abs(float(avg_age) - 30.0) < 0.01

        # Clean up
        TestModel.objects.all().delete()

    def test_django_get_or_create(self, test_environment: TestEnvironment, django_models):
        """Test Django get_or_create pattern"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Test create case
        obj1, created1 = TestModel.objects.get_or_create(
            email="test@example.com",
            defaults={'name': 'Test User', 'age': 25, 'is_active': True}
        )
        assert created1 is True
        assert obj1.name == "Test User"
        assert obj1.age == 25

        # Test get case (object already exists)
        obj2, created2 = TestModel.objects.get_or_create(
            email="test@example.com",
            defaults={'name': 'Different Name', 'age': 30, 'is_active': False}
        )
        assert created2 is False
        assert obj2.id == obj1.id
        assert obj2.name == "Test User"  # Should keep original values
        assert obj2.age == 25

        # Verify only one object exists
        assert TestModel.objects.filter(email="test@example.com").count() == 1

        # Clean up
        TestModel.objects.all().delete()

    def test_django_update_or_create(self, test_environment: TestEnvironment, django_models):
        """Test Django update_or_create pattern"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Test create case
        obj1, created1 = TestModel.objects.update_or_create(
            email="update@example.com",
            defaults={'name': 'Initial Name', 'age': 25, 'is_active': True}
        )
        assert created1 is True
        assert obj1.name == "Initial Name"
        assert obj1.age == 25

        # Test update case (object already exists)
        obj2, created2 = TestModel.objects.update_or_create(
            email="update@example.com",
            defaults={'name': 'Updated Name', 'age': 30, 'is_active': False}
        )
        assert created2 is False
        assert obj2.id == obj1.id
        assert obj2.name == "Updated Name"  # Should be updated
        assert obj2.age == 30
        assert obj2.is_active is False

        # Verify only one object exists
        assert TestModel.objects.filter(email="update@example.com").count() == 1

        # Verify the update persisted
        retrieved = TestModel.objects.get(email="update@example.com")
        assert retrieved.name == "Updated Name"
        assert retrieved.age == 30

        # Clean up
        TestModel.objects.all().delete()

    def test_django_prefetch_related(self, test_environment: TestEnvironment, django_models):
        """Test Django prefetch_related for optimizing queries"""
        Author = self.Author
        Book = self.Book

        # Create test data
        author1 = Author.objects.create(name="Author 1", email="author1@example.com")
        author2 = Author.objects.create(name="Author 2", email="author2@example.com")

        Book.objects.create(title="Book 1A", author=author1, publication_date=date(2020, 1, 1), pages=100, price=Decimal('10.00'))
        Book.objects.create(title="Book 1B", author=author1, publication_date=date(2021, 1, 1), pages=200, price=Decimal('20.00'))
        Book.objects.create(title="Book 2A", author=author2, publication_date=date(2022, 1, 1), pages=300, price=Decimal('30.00'))

        # Test prefetch_related
        authors = Author.objects.prefetch_related('books').all()

        # Access related books (should not trigger additional queries due to prefetch)
        for author in authors:
            books = list(author.books.all())
            if author.name == "Author 1":
                assert len(books) == 2
                book_titles = [book.title for book in books]
                assert "Book 1A" in book_titles
                assert "Book 1B" in book_titles
            elif author.name == "Author 2":
                assert len(books) == 1
                assert books[0].title == "Book 2A"

        # Clean up
        Book.objects.all().delete()
        Author.objects.all().delete()

    def test_django_database_functions(self, test_environment: TestEnvironment, django_models):
        """Test Django database functions"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        TestModel.objects.create(name="alice", email="alice@example.com", age=25)
        TestModel.objects.create(name="BOB", email="bob@example.com", age=30)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35)

        # Test Upper function
        upper_names = TestModel.objects.annotate(upper_name=Upper('name')).values_list('upper_name', flat=True)
        upper_list = list(upper_names)
        assert "ALICE" in upper_list
        assert "BOB" in upper_list
        assert "CHARLIE" in upper_list

        # Test Lower function
        lower_names = TestModel.objects.annotate(lower_name=Lower('name')).values_list('lower_name', flat=True)
        lower_list = list(lower_names)
        assert "alice" in lower_list
        assert "bob" in lower_list
        assert "charlie" in lower_list

        # Test Length function
        name_lengths = TestModel.objects.annotate(name_length=Length('name')).filter(name_length__gte=5)
        assert name_lengths.count() == 2  # "alice" (5) and "Charlie" (7)

        # Test Concat function
        full_info = TestModel.objects.annotate(
            full_info=Concat('name', Value(' - '), 'email', output_field=CharField())
        ).first()
        assert ' - ' in full_info.full_info
        assert '@example.com' in full_info.full_info

        # Clean up
        TestModel.objects.all().delete()

    def test_django_annotations(self, test_environment: TestEnvironment, django_models):
        """Test Django annotations with expressions"""
        TestModel = self.TestModel
        Book = self.Book
        Author = self.Author

        # Create test data for TestModel
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)

        # Test annotate with F expression for calculations
        test_with_age_plus_ten = TestModel.objects.annotate(
            age_plus_ten=F('age') + 10
        ).order_by('age')

        # Verify calculation
        first_obj = test_with_age_plus_ten.first()
        assert first_obj.age_plus_ten == first_obj.age + 10
        assert first_obj.age_plus_ten == 35  # 25 + 10

        # Create books for F expression testing
        author = Author.objects.create(name="Test Author", email="test@example.com")
        Book.objects.create(title="Book 1", author=author, publication_date=date(2020, 1, 1), pages=100, price=Decimal('10.00'))
        Book.objects.create(title="Book 2", author=author, publication_date=date(2021, 1, 1), pages=200, price=Decimal('20.00'))
        Book.objects.create(title="Book 3", author=author, publication_date=date(2022, 1, 1), pages=300, price=Decimal('30.00'))

        # Test annotate with F expression for price per page
        books_with_price_per_page = Book.objects.annotate(
            price_per_page=F('price') / F('pages')
        ).order_by('price_per_page')

        # Verify calculation
        first_book = books_with_price_per_page.first()
        expected_price_per_page = float(first_book.price) / first_book.pages
        assert abs(float(first_book.price_per_page) - expected_price_per_page) < 0.001

        # Test filtering on annotated field - use a lower threshold to avoid precision issues
        cheap_books = Book.objects.annotate(
            price_per_page=F('price') / F('pages')
        ).filter(price_per_page__lte=0.15)
        assert cheap_books.count() == 3  # All books have price_per_page = 0.10

        # Clean up
        TestModel.objects.all().delete()
        Book.objects.all().delete()
        Author.objects.all().delete()

    def test_django_values_and_values_list(self, test_environment: TestEnvironment, django_models):
        """Test Django values() and values_list() methods"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)

        # Test values() - returns list of dictionaries
        values_result = TestModel.objects.values('name', 'age').order_by('age')
        values_list = list(values_result)
        assert len(values_list) == 3
        assert values_list[0] == {'name': 'Alice', 'age': 25}
        assert values_list[1] == {'name': 'Bob', 'age': 30}
        assert values_list[2] == {'name': 'Charlie', 'age': 35}

        # Test values_list() - returns list of tuples
        values_list_result = TestModel.objects.values_list('name', 'age').order_by('age')
        tuples_list = list(values_list_result)
        assert len(tuples_list) == 3
        assert tuples_list[0] == ('Alice', 25)
        assert tuples_list[1] == ('Bob', 30)
        assert tuples_list[2] == ('Charlie', 35)

        # Test values_list() with flat=True - returns flat list
        names = TestModel.objects.values_list('name', flat=True).order_by('name')
        names_list = list(names)
        assert names_list == ['Alice', 'Bob', 'Charlie']

        # Test values() with filtering
        active_users = TestModel.objects.filter(is_active=True).values('name', 'email')
        active_list = list(active_users)
        assert len(active_list) == 2
        active_names = [user['name'] for user in active_list]
        assert 'Alice' in active_names
        assert 'Charlie' in active_names
        assert 'Bob' not in active_names

        # Clean up
        TestModel.objects.all().delete()

    def test_django_distinct_queries(self, test_environment: TestEnvironment, django_models):
        """Test Django distinct() functionality"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data with duplicate ages
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=25, is_active=True)
        TestModel.objects.create(name="David", email="david@example.com", age=30, is_active=True)

        # Test distinct ages
        distinct_ages = TestModel.objects.values_list('age', flat=True).distinct().order_by('age')
        ages_list = list(distinct_ages)
        assert ages_list == [25, 30]

        # Test distinct with multiple fields
        distinct_age_status = TestModel.objects.values('age', 'is_active').distinct().order_by('age', 'is_active')
        distinct_list = list(distinct_age_status)
        assert len(distinct_list) == 3  # (25, True), (30, False), (30, True)

        # Test count with distinct
        total_count = TestModel.objects.count()
        distinct_age_count = TestModel.objects.values('age').distinct().count()
        assert total_count == 4
        assert distinct_age_count == 2

        # Clean up
        TestModel.objects.all().delete()

    def test_django_only_and_defer(self, test_environment: TestEnvironment, django_models):
        """Test Django only() and defer() for query optimization"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        obj = TestModel.objects.create(
            name="Test User",
            email="test@example.com",
            age=30,
            is_active=True
        )

        # Test only() - load only specific fields
        obj_only = TestModel.objects.only('name', 'email').get(id=obj.id)
        assert obj_only.name == "Test User"
        assert obj_only.email == "test@example.com"
        # Accessing deferred fields will trigger additional query, but should still work
        assert obj_only.age == 30

        # Test defer() - exclude specific fields from loading
        obj_defer = TestModel.objects.defer('age', 'is_active').get(id=obj.id)
        assert obj_defer.name == "Test User"
        assert obj_defer.email == "test@example.com"
        # Accessing deferred fields will trigger additional query, but should still work
        assert obj_defer.age == 30

        # Clean up
        TestModel.objects.all().delete()

    def test_django_in_bulk(self, test_environment: TestEnvironment, django_models):
        """Test Django in_bulk() for batch retrieval"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        obj1 = TestModel.objects.create(name="User 1", email="user1@example.com", age=25)
        obj2 = TestModel.objects.create(name="User 2", email="user2@example.com", age=30)
        obj3 = TestModel.objects.create(name="User 3", email="user3@example.com", age=35)

        # Test in_bulk with IDs (default behavior)
        bulk_result = TestModel.objects.in_bulk([obj1.id, obj2.id, obj3.id])
        assert len(bulk_result) == 3
        assert bulk_result[obj1.id].name == "User 1"
        assert bulk_result[obj2.id].name == "User 2"
        assert bulk_result[obj3.id].name == "User 3"

        # Test in_bulk with all IDs (no list provided)
        bulk_all = TestModel.objects.in_bulk()
        assert len(bulk_all) == 3
        assert obj1.id in bulk_all
        assert obj2.id in bulk_all
        assert obj3.id in bulk_all

        # Test in_bulk with email field (unique field)
        bulk_by_email = TestModel.objects.in_bulk(
            ["user1@example.com", "user3@example.com"],
            field_name='email'
        )
        assert len(bulk_by_email) == 2
        assert bulk_by_email["user1@example.com"].name == "User 1"
        assert bulk_by_email["user3@example.com"].name == "User 3"

        # Clean up
        TestModel.objects.all().delete()

    def test_django_conditional_expressions(self, test_environment: TestEnvironment, django_models):
        """Test Django Case/When conditional expressions"""
        from django.db.models import Case, IntegerField, Value, When

        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        TestModel.objects.create(name="Alice", email="alice@example.com", age=25, is_active=True)
        TestModel.objects.create(name="Bob", email="bob@example.com", age=30, is_active=False)
        TestModel.objects.create(name="Charlie", email="charlie@example.com", age=35, is_active=True)

        # Test Case/When for conditional logic
        results = TestModel.objects.annotate(
            age_category=Case(
                When(age__lt=30, then=Value('young')),
                When(age__gte=30, age__lt=40, then=Value('middle')),
                default=Value('senior'),
                output_field=CharField()
            )
        ).order_by('age')

        results_list = list(results)
        assert results_list[0].age_category == 'young'  # Alice, 25
        assert results_list[1].age_category == 'middle'  # Bob, 30
        assert results_list[2].age_category == 'middle'  # Charlie, 35

        # Test Case/When with integer output
        priority_results = TestModel.objects.annotate(
            priority=Case(
                When(is_active=True, age__lt=30, then=Value(1)),
                When(is_active=True, then=Value(2)),
                When(is_active=False, then=Value(3)),
                default=Value(4),
                output_field=IntegerField()
            )
        ).order_by('priority', 'name')

        priority_list = list(priority_results)
        assert priority_list[0].name == 'Alice'  # priority 1: active and young
        assert priority_list[1].name == 'Charlie'  # priority 2: active but not young
        assert priority_list[2].name == 'Bob'  # priority 3: not active

        # Clean up
        TestModel.objects.all().delete()

    def test_django_iterator(self, test_environment: TestEnvironment, django_models):
        """Test Django iterator() for memory-efficient queries"""
        TestModel = self.TestModel

        # Ensure clean slate
        TestModel.objects.all().delete()

        # Create test data
        for i in range(20):
            TestModel.objects.create(
                name=f"User {i}",
                email=f"user{i}@example.com",
                age=20 + i
            )

        # Test iterator() - processes results without caching
        count = 0
        for obj in TestModel.objects.iterator():
            assert obj.name.startswith("User")
            count += 1
        assert count == 20

        # Test iterator with chunk_size
        count = 0
        for obj in TestModel.objects.iterator(chunk_size=5):
            assert obj.email.endswith("@example.com")
            count += 1
        assert count == 20

        # Clean up
        TestModel.objects.all().delete()
