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

from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.sql import func
from sqlalchemy.orm import (
    declarative_base, sessionmaker, relationship, Session, joinedload,
    subqueryload
)
from sqlalchemy import (
    create_engine, Column, ForeignKey, Integer, BigInteger, SmallInteger,
    Float, Numeric, String, Boolean, Date, Time, DateTime, Text, JSON, or_,
    and_, text
)

from tests.integration.container.utils.rds_test_utility import RdsTestUtility
from ..utils.conditions import (disable_on_features, enable_on_deployments,
                                enable_on_engines)
from ..utils.database_engine import DatabaseEngine
from ..utils.database_engine_deployment import DatabaseEngineDeployment
from ..utils.test_environment import TestEnvironment
from ..utils.test_environment_features import TestEnvironmentFeatures

Base = declarative_base()

class TestModel(Base):
    """Basic test model for SQLAlchemy ORM functionality"""
    __tablename__ = 'sqlalchemy_test_model'

    id = Column(Integer, primary_key=True)

    name = Column(String(100), nullable=False)
    email = Column(String(254), nullable=False, unique=True)
    age = Column(Integer, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.now(timezone.utc))

class DataTypeModel(Base):
    """Model for testing various data types"""
    __tablename__ = 'sqlalchemy_data_type_model'

    id = Column(Integer, primary_key=True)

    # String fields
    string_field = Column(String(255))
    text_field = Column(Text)

    # Numeric fields
    integer_field = Column(Integer)
    small_integer_field = Column(SmallInteger)
    big_integer_field = Column(BigInteger)
    numeric_field = Column(Numeric(10, 2))
    float_field = Column(Float)

    # Boolean field
    boolean_field = Column(Boolean, default=False)

    # Date/Time fields
    date_field = Column(Date)
    time_field = Column(Time)
    datetime_field = Column(DateTime)

    # JSON field (MySQL 5.7+)
    json_field = Column(JSON)

class Author(Base):
    """Author model for relationship testing"""
    __tablename__ = 'sqlalchemy_author'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(254), nullable=False)
    birth_date = Column(Date)

    books = relationship('Book', back_populates='author', cascade='all, delete-orphan')

class Book(Base):
    """Book model for relationship testing"""
    __tablename__ = 'sqlalchemy_book'

    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    author_id = Column(Integer, ForeignKey("sqlalchemy_author.id"), nullable=False)
    publication_date = Column(Date, nullable=False)
    pages = Column(Integer, nullable=False)
    price = Column(Numeric(8, 2), nullable=False)

    author = relationship('Author', back_populates='books')

@enable_on_engines([DatabaseEngine.MYSQL])  # MySQL Specific until PG is implemented
@enable_on_deployments([DatabaseEngineDeployment.AURORA, DatabaseEngineDeployment.RDS_MULTI_AZ_CLUSTER])
@disable_on_features([TestEnvironmentFeatures.RUN_AUTOSCALING_TESTS_ONLY,
                      TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT,
                      TestEnvironmentFeatures.PERFORMANCE])
class TestSqlAlchemy:
    @pytest.fixture(scope='class')
    def rds_utils(self):
        region: str = TestEnvironment.get_current().get_info().get_region()
        return RdsTestUtility(region)


    @pytest.fixture(scope="class")
    def engine(self, conn_utils):
        conn_str = f'mysql+aws_wrapper_mysqlconnector://{conn_utils.user}:{conn_utils.password}@{conn_utils.writer_cluster_host}:{conn_utils.port}/{conn_utils.dbname}'
        engine = create_engine(conn_str)
        Base.metadata.create_all(engine)
        yield engine
        Base.metadata.drop_all(engine)

    @pytest.fixture(scope="class")
    def Session(self, engine):
        Session = sessionmaker(bind=engine)
        yield Session

    @pytest.fixture(scope="class")
    def session(self, Session):
        session = Session()
        yield session
        session.rollback()
        session.close()

    def test_sqlalchemy_backend_configuration(self, test_environment: TestEnvironment, engine):
        """Test SQLAlchemy backend configuration with empty plugins"""
        # Verify that the connection is using the AWS wrapper
        with engine.connect() as connection:
            assert connection.connection is not None

        # Test basic connection functionality
        with Session(engine) as session:
            assert session.query(TestModel).count() == 0

    def test_sqlalchemy_basic_model_operations(self, session, test_environment: TestEnvironment):
        """Test basic SQLAlchemy ORM operations (CRUD)"""

        # Create
        test_obj = TestModel(
            name="John Doe",
            email="john@example.com",
            age=30,
            is_active=True
        )
        session.add(test_obj)
        session.commit()
        assert test_obj.id is not None
        assert test_obj.name == "John Doe"

        # Read
        retrieved_obj = session.query(TestModel).filter_by(id = test_obj.id).one()
        assert retrieved_obj.name == "John Doe"
        assert retrieved_obj.email == "john@example.com"
        assert retrieved_obj.age == 30
        assert retrieved_obj.is_active is True

        # Update
        retrieved_obj.name = "Jane Doe"
        retrieved_obj.age = 25
        session.commit()

        updated_obj = session.query(TestModel).filter_by(id = test_obj.id).one()
        assert updated_obj.name == "Jane Doe"
        assert updated_obj.age == 25

        # Delete
        session.delete(updated_obj)
        session.commit()
        assert session.query(TestModel).filter(TestModel.id == test_obj.id).count() == 0

    def test_sqlalchemy_query_operations(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy query operations"""
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=35, is_active=True),
        ])
        session.commit()
        # Test filtering
        active_users = session.query(TestModel).filter(TestModel.is_active == True).all()
        assert len(active_users) == 2
        # Test ordering
        ordered_users = session.query(TestModel).order_by(TestModel.age).all()
        ages = [user.age for user in ordered_users]
        assert ages == [25, 30, 35]
        # Test complex queries
        young_active_users = session.query(TestModel).filter(
            TestModel.age < 30, TestModel.is_active == True
        ).all()
        assert len(young_active_users) == 1
        assert young_active_users[0].name == "Alice"
        # Test exclude (using NOT)
        non_bob_users = session.query(TestModel).filter(TestModel.name != "Bob").all()
        assert len(non_bob_users) == 2
        # Test exists
        assert session.query(TestModel).filter(TestModel.name == "Alice").first() is not None
        assert session.query(TestModel).filter(TestModel.name == "David").first() is None
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_data_types(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy with various data types"""
        # Ensure clean slate
        session.query(DataTypeModel).delete()
        session.commit()
        # Create test data with various data types
        test_datetime = datetime(2023, 12, 25, 14, 30, 0)
        test_data = DataTypeModel(
            string_field="Test String",
            text_field="This is a longer text field content",
            integer_field=42,
            small_integer_field=5,
            big_integer_field=9223372036854775807,
            numeric_field=Decimal('123.45'),
            float_field=3.14159,
            boolean_field=True,
            date_field=date(2023, 12, 25),
            time_field=time(14, 30, 0),
            datetime_field=test_datetime,
            json_field={"key": "value", "number": 123, "array": [1, 2, 3]},
        )
        session.add(test_data)
        session.commit()
        # Retrieve and verify data
        retrieved = session.query(DataTypeModel).get(test_data.id)
        assert retrieved.string_field == "Test String"
        assert retrieved.text_field == "This is a longer text field content"
        assert retrieved.integer_field == 42
        assert retrieved.small_integer_field == 5
        assert retrieved.big_integer_field == 9223372036854775807
        assert retrieved.numeric_field == Decimal('123.45')
        assert abs(retrieved.float_field - 3.14159) < 0.001
        assert retrieved.boolean_field is True
        assert retrieved.date_field == date(2023, 12, 25)
        assert retrieved.time_field == time(14, 30, 0)
        assert retrieved.datetime_field == test_datetime
        assert retrieved.json_field == {"key": "value", "number": 123, "array": [1, 2, 3]}
        # Clean up
        session.query(DataTypeModel).delete()
        session.commit()

    def test_sqlalchemy_null_values(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy handling of NULL values"""
        # Ensure clean slate
        session.query(DataTypeModel).delete()
        session.commit()
        # Create object with NULL values
        test_obj = DataTypeModel(
            string_field=None,
            integer_field=None,
            date_field=None,
            boolean_field=False,
        )
        session.add(test_obj)
        session.commit()
        # Retrieve and verify NULL values
        retrieved = session.query(DataTypeModel).get(test_obj.id)
        assert retrieved.string_field is None
        assert retrieved.integer_field is None
        assert retrieved.date_field is None
        assert retrieved.boolean_field is False
        # Test filtering with NULL values
        null_char_objects = session.query(DataTypeModel).filter(DataTypeModel.string_field.is_(None)).all()
        assert len(null_char_objects) == 1
        not_null_char_objects = session.query(DataTypeModel).filter(DataTypeModel.string_field.isnot(None)).all()
        assert len(not_null_char_objects) == 0
        # Create an object with non-NULL values to test the opposite
        session.add(DataTypeModel(
            string_field="Not NULL",
            integer_field=42,
            date_field=date(2023, 1, 1),
        ))
        session.commit()
        # Now test filtering again
        null_string_objects = session.query(DataTypeModel).filter(DataTypeModel.string_field.is_(None)).all()
        # Still one NULL object
        assert len(null_string_objects) == 1
        not_null_string_objects = session.query(DataTypeModel).filter(DataTypeModel.string_field.isnot(None)).all()
        # Now one non-NULL object
        assert len(not_null_string_objects) == 1
        # Clean up
        session.query(DataTypeModel).delete()
        session.commit()

    def test_sqlalchemy_relationships(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy relationships (ForeignKey)"""
        # Create author
        author = Author(
            name="J.K. Rowling",
            email="jk@example.com",
            birth_date=date(1965, 7, 31),
        )
        session.add(author)
        session.commit()
        # Create books
        book1 = Book(
            title="Harry Potter and the Philosopher's Stone",
            author_id=author.id,
            publication_date=date(1997, 6, 26),
            pages=223,
            price=Decimal('12.99'),
        )
        book2 = Book(
            title="Harry Potter and the Chamber of Secrets",
            author_id=author.id,
            publication_date=date(1998, 7, 2),
            pages=251,
            price=Decimal('13.99'),
        )
        session.add_all([book1, book2])
        session.commit()
        # Test forward relationship
        assert book1.author.name == "J.K. Rowling"
        assert book2.author.email == "jk@example.com"
        # Test reverse relationship
        assert len(author.books) == 2
        book_titles = [book.title for book in sorted(author.books, key=lambda b: b.publication_date)]
        assert "Harry Potter and the Philosopher's Stone" in book_titles
        assert "Harry Potter and the Chamber of Secrets" in book_titles
        # Test related queries
        books_by_author = session.query(Book).join(Author).filter(Author.name == "J.K. Rowling").all()
        assert len(books_by_author) == 2
        # Test joinedload for optimization
        book_with_author = session.query(Book).options(
            joinedload(Book.author)
        ).filter(Book.id == book1.id).one()
        assert book_with_author.author.name == "J.K. Rowling"
        # Clean up
        session.query(Book).delete()
        session.query(Author).delete()
        session.commit()

    def test_sqlalchemy_aggregations(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy aggregations"""
        author = Author(name="Test Author", email="test@example.com")
        session.add(author)
        session.flush()
        books = [
            Book(title="Book 1", author_id=author.id, publication_date=date(2020, 1, 1), pages=100, price=Decimal('10.00')),
            Book(title="Book 2", author_id=author.id, publication_date=date(2021, 1, 1), pages=200, price=Decimal('20.00')),
            Book(title="Book 3", author_id=author.id, publication_date=date(2022, 1, 1), pages=300, price=Decimal('30.00')),
        ]
        session.add_all(books)
        session.flush()
        stats = session.query(
            func.count(Book.id).label('total_books'),
            func.sum(Book.pages).label('total_pages'),
            func.avg(Book.price).label('avg_price'),
            func.max(Book.pages).label('max_pages'),
            func.min(Book.price).label('min_price'),
        ).one()
        assert stats.total_books == 3
        assert stats.total_pages == 600
        assert abs(float(stats.avg_price) - 20.0) < 0.01
        assert stats.max_pages == 300
        assert stats.min_price == Decimal('10.00')
        session.rollback()

    def test_sqlalchemy_transactions(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy transaction handling"""
        session.query(TestModel).delete()
        session.commit()
        initial_count = session.query(TestModel).count()
        # Test successful transaction
        session.add(TestModel(name="User 1", email="user1@example.com", age=25))
        session.add(TestModel(name="User 2", email="user2@example.com", age=30))
        session.commit()
        assert session.query(TestModel).count() == initial_count + 2
        # Test rollback transaction
        try:
            session.add(TestModel(name="User 3", email="user3@example.com", age=35))
            session.add(TestModel(name="User 4", email="user4@example.com", age=40))
            session.flush()
            raise Exception("Force rollback")
        except Exception:
            session.rollback()
        assert session.query(TestModel).count() == initial_count + 2
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_bulk_operations(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy bulk operations"""
        session.query(TestModel).delete()
        session.commit()
        # Test bulk insert
        session.bulk_save_objects([
            TestModel(name=f"User {i}", email=f"user{i}@example.com", age=20 + i)
            for i in range(10)
        ])
        session.commit()
        assert session.query(TestModel).count() == 10
        # Test bulk update
        session.query(TestModel).update({TestModel.age: TestModel.age + 5})
        session.commit()
        ages = [r.age for r in session.query(TestModel).order_by(TestModel.name).all()]
        expected_ages = [25 + i for i in range(10)]
        assert ages == expected_ages
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_complex_queries(self, test_environment: TestEnvironment, session):
        """Test complex SQLAlchemy queries with or_/and_ and column expressions"""
        session.query(TestModel).delete()
        session.commit()
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=35, is_active=True),
            TestModel(name="David", email="david@example.com", age=28, is_active=True),
        ])
        session.commit()
        # Test or_ for complex conditions
        results = session.query(TestModel).filter(
            or_(TestModel.age >= 30, TestModel.name.like('A%'))
        ).all()
        assert len(results) == 3
        # Test column expression update (equivalent to Django's F expressions)
        session.query(TestModel).filter(TestModel.age < 30).update(
            {TestModel.age: TestModel.age + 5}, synchronize_session='fetch'
        )
        session.commit()
        alice = session.query(TestModel).filter_by(name="Alice").one()
        david = session.query(TestModel).filter_by(name="David").one()
        assert alice.age == 30
        assert david.age == 33
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_raw_sql_queries(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy raw SQL query execution"""
        session.query(TestModel).delete()
        session.commit()
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=35, is_active=True),
        ])
        session.commit()
        table = TestModel.__tablename__
        # Test raw SQL with text()
        rows = session.execute(
            text(f'SELECT * FROM {table} WHERE age >= :age ORDER BY age'),
            {'age': 30}
        ).fetchall()
        assert len(rows) == 2
        # Test raw SQL for specific columns
        rows = session.execute(
            text(f'SELECT name, age FROM {table} WHERE is_active = :active ORDER BY age'),
            {'active': True}
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "Alice"
        assert rows[0][1] == 25
        assert rows[1][0] == "Charlie"
        assert rows[1][1] == 35
        # Test raw SQL aggregate
        result = session.execute(
            text(f'SELECT COUNT(*), AVG(age) FROM {table}')
        ).fetchone()
        assert result[0] == 3
        assert abs(float(result[1]) - 30.0) < 0.01
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_get_or_create(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy get-or-create pattern"""
        session.query(TestModel).delete()
        session.commit()
        # Test create case
        obj1 = session.query(TestModel).filter_by(email="test@example.com").first()
        created1 = obj1 is None
        if created1:
            obj1 = TestModel(name="Test User", email="test@example.com", age=25, is_active=True)
            session.add(obj1)
            session.commit()
        assert created1 is True
        assert obj1.name == "Test User"
        assert obj1.age == 25
        # Test get case
        obj2 = session.query(TestModel).filter_by(email="test@example.com").first()
        created2 = obj2 is None
        if created2:
            obj2 = TestModel(name="Different Name", email="test@example.com", age=30, is_active=False)
            session.add(obj2)
            session.commit()
        assert created2 is False
        assert obj2.id == obj1.id
        assert obj2.name == "Test User"
        assert obj2.age == 25
        assert session.query(TestModel).filter_by(email="test@example.com").count() == 1
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_update_or_create(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy update-or-create pattern"""
        session.query(TestModel).delete()
        session.commit()
        # Test create case
        obj1 = session.query(TestModel).filter_by(email="update@example.com").first()
        created1 = obj1 is None
        if created1:
            obj1 = TestModel(name="Initial Name", email="update@example.com", age=25, is_active=True)
            session.add(obj1)
        session.commit()
        assert created1 is True
        assert obj1.name == "Initial Name"
        assert obj1.age == 25
        # Test update case
        obj2 = session.query(TestModel).filter_by(email="update@example.com").first()
        created2 = obj2 is None
        if created2:
            obj2 = TestModel(name="Updated Name", email="update@example.com", age=30, is_active=False)
            session.add(obj2)
        else:
            obj2.name = "Updated Name"
            obj2.age = 30
            obj2.is_active = False
        session.commit()
        assert created2 is False
        assert obj2.id == obj1.id
        assert obj2.name == "Updated Name"
        assert obj2.age == 30
        assert obj2.is_active is False
        assert session.query(TestModel).filter_by(email="update@example.com").count() == 1
        retrieved = session.query(TestModel).filter_by(email="update@example.com").one()
        assert retrieved.name == "Updated Name"
        assert retrieved.age == 30
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_eager_loading(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy eager loading for optimizing queries"""
        author1 = Author(name="Author 1", email="author1@example.com")
        author2 = Author(name="Author 2", email="author2@example.com")
        session.add_all([author1, author2])
        session.flush()
        session.add_all([
            Book(title="Book 1A", author_id=author1.id, publication_date=date(2020, 1, 1), pages=100, price=Decimal('10.00')),
            Book(title="Book 1B", author_id=author1.id, publication_date=date(2021, 1, 1), pages=200, price=Decimal('20.00')),
            Book(title="Book 2A", author_id=author2.id, publication_date=date(2022, 1, 1), pages=300, price=Decimal('30.00')),
        ])
        session.commit()
        # Test subqueryload (equivalent to Django's prefetch_related)
        authors = session.query(Author).options(subqueryload(Author.books)).all()
        for author in authors:
            if author.name == "Author 1":
                assert len(author.books) == 2
                titles = [b.title for b in author.books]
                assert "Book 1A" in titles
                assert "Book 1B" in titles
            elif author.name == "Author 2":
                assert len(author.books) == 1
                assert author.books[0].title == "Book 2A"
        session.rollback()

    def test_sqlalchemy_database_functions(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy database functions"""
        session.query(TestModel).delete()
        session.commit()
        session.add_all([
            TestModel(name="alice", email="alice@example.com", age=25),
            TestModel(name="BOB", email="bob@example.com", age=30),
            TestModel(name="Charlie", email="charlie@example.com", age=35),
        ])
        session.commit()
        # Test upper
        upper_names = [r[0] for r in session.query(func.upper(TestModel.name)).all()]
        assert "ALICE" in upper_names
        assert "BOB" in upper_names
        assert "CHARLIE" in upper_names
        # Test lower
        lower_names = [r[0] for r in session.query(func.lower(TestModel.name)).all()]
        assert "alice" in lower_names
        assert "bob" in lower_names
        assert "charlie" in lower_names
        # Test length
        results = session.query(TestModel).filter(func.length(TestModel.name) >= 5).all()
        assert len(results) == 2  # "alice" (5) and "Charlie" (7)
        # Test concat
        result = session.query(
            func.concat(TestModel.name, ' - ', TestModel.email)
        ).first()
        assert ' - ' in result[0]
        assert '@example.com' in result[0]
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_values_and_values_list(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy equivalents of Django's values() and values_list() functions"""
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=35, is_active=True),
        ])
        session.commit()
        # Convert values to dicts (equivalent to Django's values())
        values_result = session.query(TestModel.name, TestModel.age).order_by(TestModel.age).all()
        assert len(values_result) == 3
        assert values_result[0] == ('Alice', 25)
        assert values_result[1] == ('Bob', 30)
        assert values_result[2] == ('Charlie', 35)
        values_dicts = [{'name': r.name, 'age': r.age} for r in values_result]
        assert values_dicts[0] == {'name': 'Alice', 'age': 25}
        assert values_dicts[1] == {'name': 'Bob', 'age': 30}
        assert values_dicts[2] == {'name': 'Charlie', 'age': 35}
        # Test flat list (equivalent to Django's values_list with flat=True)
        names = [r[0] for r in session.query(TestModel.name).order_by(TestModel.name).all()]
        assert names == ['Alice', 'Bob', 'Charlie']
        # Test with filtering
        active_users = session.query(TestModel.name, TestModel.email).filter(
            TestModel.is_active == True
        ).all()
        assert len(active_users) == 2
        active_names = [r.name for r in active_users]
        assert 'Alice' in active_names
        assert 'Charlie' in active_names
        assert 'Bob' not in active_names
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_distinct_queries(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy distinct() functionality"""
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data with duplicate ages
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=25, is_active=True),
            TestModel(name="David", email="david@example.com", age=30, is_active=True),
        ])
        session.commit()
        # Test distinct ages
        ages_list = [r[0] for r in session.query(TestModel.age).distinct().order_by(TestModel.age).all()]
        assert ages_list == [25, 30]
        # Test distinct with multiple fields
        distinct_list = session.query(TestModel.age, TestModel.is_active).distinct().order_by(
            TestModel.age, TestModel.is_active
        ).all()
        assert len(distinct_list) == 3  # (25, True), (30, False), (30, True)
        # Test count with distinct
        total_count = session.query(TestModel).count()
        distinct_age_count = session.query(TestModel.age).distinct().count()
        assert total_count == 4
        assert distinct_age_count == 2
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_load_only_and_defer(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy load_only() and defer() for query optimization"""
        from sqlalchemy.orm import defer, load_only
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        obj = TestModel(name="Test User", email="test@example.com", age=30, is_active=True)
        session.add(obj)
        session.commit()
        obj_id = obj.id
        session.expire_all()
        # Test load_only() - load only specific fields
        obj_only = session.query(TestModel).options(
            load_only(TestModel.name, TestModel.email)
        ).get(obj_id)
        assert obj_only.name == "Test User"
        assert obj_only.email == "test@example.com"
        assert obj_only.age == 30
        session.expire_all()
        # Test defer() - exclude specific fields from loading
        obj_defer = session.query(TestModel).options(
            defer(TestModel.age), defer(TestModel.is_active)
        ).get(obj_id)
        assert obj_defer.name == "Test User"
        assert obj_defer.email == "test@example.com"
        assert obj_defer.age == 30
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_batch_retrieval(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy batch retrieval (equivalent to Django's in_bulk)"""
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        obj1 = TestModel(name="User 1", email="user1@example.com", age=25)
        obj2 = TestModel(name="User 2", email="user2@example.com", age=30)
        obj3 = TestModel(name="User 3", email="user3@example.com", age=35)
        session.add_all([obj1, obj2, obj3])
        session.commit()
        # Test bulk retrieval by IDs
        ids = [obj1.id, obj2.id, obj3.id]
        bulk_result = {o.id: o for o in session.query(TestModel).filter(TestModel.id.in_(ids)).all()}
        assert len(bulk_result) == 3
        assert bulk_result[obj1.id].name == "User 1"
        assert bulk_result[obj2.id].name == "User 2"
        assert bulk_result[obj3.id].name == "User 3"
        # Test bulk retrieval of all
        bulk_all = {o.id: o for o in session.query(TestModel).all()}
        assert len(bulk_all) == 3
        assert obj1.id in bulk_all
        assert obj2.id in bulk_all
        assert obj3.id in bulk_all
        # Test bulk retrieval by email field
        emails = ["user1@example.com", "user3@example.com"]
        bulk_by_email = {
            o.email: o for o in session.query(TestModel).filter(TestModel.email.in_(emails)).all()
        }
        assert len(bulk_by_email) == 2
        assert bulk_by_email["user1@example.com"].name == "User 1"
        assert bulk_by_email["user3@example.com"].name == "User 3"
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_conditional_expressions(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy case() conditional expressions"""
        from sqlalchemy import String, case
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        session.add_all([
            TestModel(name="Alice", email="alice@example.com", age=25, is_active=True),
            TestModel(name="Bob", email="bob@example.com", age=30, is_active=False),
            TestModel(name="Charlie", email="charlie@example.com", age=35, is_active=True),
        ])
        session.commit()
        # Test case() for conditional logic
        age_category = case(
            (TestModel.age < 30, 'young'),
            (TestModel.age.between(30, 39), 'middle'),
            else_='senior'
        ).label('age_category')
        results = session.query(TestModel, age_category).order_by(TestModel.age).all()
        assert results[0].age_category == 'young'   # Alice, 25
        assert results[1].age_category == 'middle'   # Bob, 30
        assert results[2].age_category == 'middle'   # Charlie, 35
        # Test case() with integer output
        from sqlalchemy import Integer
        priority = case(
            (and_(TestModel.is_active == True, TestModel.age < 30), 1),
            (TestModel.is_active == True, 2),
            (TestModel.is_active == False, 3),
            else_=4
        ).label('priority')
        results = session.query(TestModel, priority).order_by('priority', TestModel.name).all()
        assert results[0].TestModel.name == 'Alice'    # priority 1
        assert results[1].TestModel.name == 'Charlie'  # priority 2
        assert results[2].TestModel.name == 'Bob'      # priority 3
        # Clean up
        session.query(TestModel).delete()
        session.commit()

    def test_sqlalchemy_yield_per(self, test_environment: TestEnvironment, session):
        """Test SQLAlchemy yield_per() for memory-efficient queries"""
        # Ensure clean slate
        session.query(TestModel).delete()
        session.commit()
        # Create test data
        session.add_all([
            TestModel(name=f"User {i}", email=f"user{i}@example.com", age=20 + i)
            for i in range(20)
        ])
        session.commit()
        # Test yield_per() - processes results without caching all at once
        count = 0
        for obj in session.query(TestModel).yield_per(100):
            assert obj.name.startswith("User")
            count += 1
        assert count == 20
        # Test yield_per with smaller chunk size
        count = 0
        for obj in session.query(TestModel).yield_per(5):
            assert obj.email.endswith("@example.com")
            count += 1
        assert count == 20
        # Clean up
        session.query(TestModel).delete()
        session.commit()

