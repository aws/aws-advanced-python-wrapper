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

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

class TestSqlAlchemyORM:
    def test_basic_workflow(self):
        # Step 1: Create engine (connection to database)
        engine = create_engine('postgresql+aws_wrapper://pguser:pgpassword@mydb.cluster-XYZ.us-west-1.rds.amazonaws.com:5432/somedb')

        # Step 2: Define base class for declarative models
        Base = declarative_base()

        # Step 3: Define model class (separate from database operations)
        class User(Base):
            __tablename__ = 'users'

            id = Column(Integer, primary_key=True)
            name = Column(String(50))
            email = Column(String(100))

        # Step 4: Create tables
        Base.metadata.create_all(engine)

        # Step 5: Create session factory
        Session = sessionmaker(bind=engine)

        # Step 6: Use session for database operations
        with Session() as session:
            # INSERT - Create new object and add to session
            new_user = User(name='John Doe', email='john@example.com')
            session.add(new_user)
            session.commit()  # Explicit commit required

            # SELECT - Query using session
            users = session.query(User).filter(User.name == 'John Doe').all()
            for user in users:
                print(f"{user.name}: {user.email}")


            # UPDATE - Modify object and commit
            user = session.query(User).filter(User.name == "John Doe").first()
            user.email = 'newemail@example.com'
            session.commit()

            # DELETE - Remove object from session
            user_to_delete = session.query(User).filter(User.name == "John Doe").first()
            session.delete(user_to_delete)
            session.commit()
