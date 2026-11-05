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

"""The KMS Encryption Plugin through SQLAlchemy's wrapper dialects on a live server and KMS."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, List, Optional, Type

import pytest
from sqlalchemy import (Column, ForeignKey, Integer, MetaData, String, Table,
                        create_engine, exc, insert, select, text)
from sqlalchemy.engine import URL, Connection, Engine
from sqlalchemy.orm import (DeclarativeBase, Mapped, Session, joinedload,
                            mapped_column, relationship)
from sqlalchemy.types import UserDefinedType

from aws_advanced_python_wrapper.encryption.key_management_utility import \
    KeyManagementUtility
from aws_advanced_python_wrapper.errors import (EncryptionFailedError,
                                                IntegrityCheckError,
                                                KeyManagementError,
                                                MetadataError)
from ..utils.conditions import enable_on_features
from ..utils.kms_encryption_helper import (DbUser, KmsSettings,
                                           PayloadVerifier,
                                           create_column_trigger,
                                           encrypt_column, encryption_config,
                                           is_pg, kms_builds, kms_client,
                                           plain_connect)
from ..utils.test_environment import TestEnvironment
from ..utils.test_environment_features import TestEnvironmentFeatures

pytestmark = [pytest.mark.kms_encryption,
              enable_on_features([TestEnvironmentFeatures.RUN_KMS_ENCRYPTION_TESTS_ONLY,
                                  TestEnvironmentFeatures.KMS_LOCALSTACK])]

_USERS, _CARDS, _GUARDED = "kms_sa_users", "kms_sa_cards", "kms_sa_guarded"


class EncryptedValue(UserDefinedType):
    """The guide's pass-through type for an encrypted column, so PostgreSQL gets no cast on the parameter."""
    cache_ok = True

    def get_col_spec(self, **kw: Any) -> str:
        return "BYTEA" if is_pg() else "VARBINARY(1024)"


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = _USERS
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    ssn: Mapped[Optional[str]] = mapped_column(EncryptedValue())
    cards: Mapped[List[Card]] = relationship(back_populates="user", order_by="Card.id")


class Card(Base):
    __tablename__ = _CARDS
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey(f"{_USERS}.id"))
    number: Mapped[Optional[str]] = mapped_column(EncryptedValue())
    user: Mapped[User] = relationship(back_populates="cards")


_guarded = Table(_GUARDED, MetaData(), Column("id", Integer, primary_key=True), Column("name", String(100)),
                 Column("ssn", EncryptedValue()))


def _url(user: DbUser, **query: str) -> URL:
    env = TestEnvironment.get_current()
    info, writer = env.get_database_info(), env.get_writer()
    return URL.create("postgresql+aws_wrapper_psycopg" if is_pg() else "mysql+aws_wrapper_mysqlconnector",
                      username=user.name, password=user.password, host=writer.get_host(), port=writer.get_port(),
                      database=info.get_default_db_name(), query=query)


@pytest.fixture
def tables(kms_trigger_installed: None, kms_settings: KmsSettings, kms_schema: str,
           kms_master_key_arn: str) -> Iterator[Dict[str, int]]:
    """New ``kms_sa_users.ssn`` and ``kms_sa_cards.number`` encrypted without a trigger, so a test can tamper with a
    stored value, and ``kms_sa_guarded.ssn`` encrypted with the trigger. Returns each column's key row."""
    id_type = "SERIAL PRIMARY KEY" if is_pg() else "INT AUTO_INCREMENT PRIMARY KEY"
    binary = EncryptedValue().get_col_spec()
    with plain_connect() as conn:
        with conn.cursor() as cur:
            for name in (_CARDS, _USERS, _GUARDED):  # tables a crashed run left behind
                cur.execute(f"DROP TABLE IF EXISTS {name}")
            cur.execute(f"CREATE TABLE {_USERS} (id {id_type}, name VARCHAR(100), ssn {binary})")
            cur.execute(f"CREATE TABLE {_CARDS} (id {id_type}, user_id INT REFERENCES {_USERS} (id), number {binary})")
            cur.execute(f"CREATE TABLE {_GUARDED} (id {id_type}, name VARCHAR(100), ssn {binary})")
        conn.commit()
        create_column_trigger(conn, _GUARDED, "ssn")
        key_ids = {f"{table}.{column}": encrypt_column(conn, kms_settings, kms_schema, table, column,
                                                       kms_master_key_arn)
                   for table, column in ((_USERS, "ssn"), (_CARDS, "number"), (_GUARDED, "ssn"))}
    try:
        yield key_ids
    finally:
        with plain_connect() as conn:
            with conn.cursor() as cur:
                for name in (_CARDS, _USERS, _GUARDED):
                    cur.execute(f"DROP TABLE IF EXISTS {name}")
                cur.execute(f"DELETE FROM {kms_schema}.encryption_metadata WHERE table_name IN (%s, %s, %s)",
                            (_CARDS, _USERS, _GUARDED))
            conn.commit()


@pytest.fixture
def plugin_props(kms_settings: KmsSettings, kms_schema: str, kms_master_key_arn: str) -> Dict[str, str]:
    """The plugin's properties other than ``plugins``."""
    return {"encryption_metadata_schema": kms_schema, "encryption_allowed_master_key_arns": kms_master_key_arn,
            **kms_settings.props()}


@pytest.fixture
def make_engine(build: str, tables: Dict[str, int], kms_app_user: DbUser,
                plugin_props: Dict[str, str]) -> Iterator[Callable[..., Engine]]:
    """Creates engines on the wrapper dialect as the application user, with the plugin and its properties in
    ``connect_args`` unless a test passes its own; every engine is disposed of afterwards."""
    engines: List[Engine] = []

    def create(*, query: Optional[Dict[str, str]] = None, connect_args: Optional[Dict[str, Any]] = None) -> Engine:
        args: Dict[str, Any] = {"plugins": "kms_encryption", **plugin_props} if connect_args is None \
            else dict(connect_args)
        if not is_pg():
            args["use_pure"] = build == "mysql-pure"
        engine = create_engine(_url(kms_app_user, **(query or {})), connect_args=args, hide_parameters=True)
        engines.append(engine)
        return engine

    yield create
    for engine in engines:
        engine.dispose()


def _stored_names(verifier: PayloadVerifier) -> List[str]:
    return verifier.stored(f"SELECT name FROM {_GUARDED} ORDER BY id")


@kms_builds()
def test_wrapper_plugins_query_parameter_loads_the_plugin(build: str, make_engine: Callable[..., Engine],
                                                          tables: Dict[str, int], plugin_props: Dict[str, str],
                                                          kms_verifier: PayloadVerifier) -> None:
    props = plugin_props
    dropped = make_engine(query={"plugins": "kms_encryption", **props}, connect_args={})
    with pytest.raises(exc.DBAPIError, match="does not carry a valid HMAC tag"), dropped.begin() as conn:
        conn.execute(insert(_guarded), {"name": "plugins-query", "ssn": "pt-sa-dropped"})
    assert _stored_names(kms_verifier) == [], \
        "with ?plugins= SQLAlchemy drops the parameter, the plugin isn't loaded and the trigger rejects the plaintext"

    for label, engine in (("wrapper-plugins", make_engine(query={"wrapper_plugins": "kms_encryption", **props},
                                                          connect_args={})),
                          ("connect-args", make_engine())):
        with engine.begin() as conn:
            conn.execute(insert(_guarded), {"name": label, "ssn": f"pt-sa-{label}"})
        (payload,) = kms_verifier.stored(f"SELECT ssn FROM {_GUARDED} WHERE name = %s", (label,))
        assert kms_verifier.decode(_GUARDED, "ssn", payload) == (tables[f"{_GUARDED}.ssn"], f"pt-sa-{label}"), \
            f"with {label} the plugin is loaded and encrypts the write"


@kms_builds()
def test_sqlalchemy_core_and_orm_round_trip(build: str, make_engine: Callable[..., Engine], tables: Dict[str, int],
                                            kms_verifier: PayloadVerifier) -> None:
    engine = make_engine(query={"plugins": "kms_encryption"})
    with engine.begin() as conn:
        conn.execute(insert(User), {"name": "core-1", "ssn": "pt-sa-core-1"})
        conn.execute(insert(User), [{"name": "core-2", "ssn": "pt-sa-core-2"},
                                    {"name": "core-3", "ssn": "pt-sa-core-3"}])
        rows = [(row.name, row.ssn) for row in conn.execute(select(User.name, User.ssn).order_by(User.id))]
    assert rows == [("core-1", "pt-sa-core-1"), ("core-2", "pt-sa-core-2"), ("core-3", "pt-sa-core-3")], \
        "Core insert, executemany and result iteration round-trip with ?plugins= dropped and connect_args used"

    with Session(engine) as session:
        session.add_all([User(name="orm-1", ssn="pt-sa-orm-1",
                              cards=[Card(number="pt-sa-card-1a"), Card(number="pt-sa-card-1b")]),
                         User(name="orm-2", ssn="pt-sa-orm-2", cards=[Card(number="pt-sa-card-2a")])])
        session.commit()
        first = session.scalars(select(User).filter_by(name="orm-1")).one()
        first_id = first.id
        assert first.ssn == "pt-sa-orm-1", "an ORM filter_by reads the decrypted value"
        first.ssn = "pt-sa-orm-1-updated"
        session.commit()
    with Session(engine) as session:
        got = session.get(User, first_id)
        assert got is not None and got.ssn == "pt-sa-orm-1-updated", "an ORM attribute update round-trips"
        loaded = session.scalars(select(User).options(joinedload(User.cards)).where(User.name.like("orm-%"))
                                 .order_by(User.id).limit(2)).unique().all()
        assert [(u.ssn, [c.number for c in u.cards]) for u in loaded] == [
            ("pt-sa-orm-1-updated", ["pt-sa-card-1a", "pt-sa-card-1b"]), ("pt-sa-orm-2", ["pt-sa-card-2a"])], \
            "joinedload with LIMIT decrypts both tables' columns"

    stored = kms_verifier.stored(f"SELECT ssn FROM {_USERS} ORDER BY id")
    assert [kms_verifier.decode(_USERS, "ssn", payload)[1] for payload in stored] == [
        "pt-sa-core-1", "pt-sa-core-2", "pt-sa-core-3", "pt-sa-orm-1-updated", "pt-sa-orm-2"], \
        "every value SQLAlchemy wrote is stored encrypted"


@kms_builds()
def test_plugin_errors_arrive_as_the_sqlalchemy_classes_the_guide_names(
        build: str, make_engine: Callable[..., Engine], tables: Dict[str, int], kms_settings: KmsSettings,
        kms_schema: str) -> None:
    other_key = kms_client(kms_settings).create_key(Description="kms container test key outside the allow-list")
    with plain_connect() as admin:
        with admin.cursor() as cur:
            cur.execute(f"ALTER TABLE {_USERS} ADD COLUMN pin {EncryptedValue().get_col_spec()}")
        admin.commit()
        encrypt_column(admin, kms_settings, kms_schema, _USERS, "pin", other_key["KeyMetadata"]["Arn"])
    engine = make_engine()
    with engine.begin() as conn:
        conn.execute(insert(User), {"name": "tamper", "ssn": "pt-sa-tamper"})
    with plain_connect() as admin, admin.cursor() as cur:
        cur.execute(f"SELECT ssn FROM {_USERS} WHERE name = 'tamper'")
        payload = bytes(cur.fetchone()[0])
        cur.execute(f"UPDATE {_USERS} SET ssn = %s WHERE name = 'tamper'", (payload[:-1] + bytes([payload[-1] ^ 1]),))
        admin.commit()

    def assert_wrapped(caught: pytest.ExceptionInfo, wrapper_class: Type[exc.DBAPIError], plugin_class: type,
                       conn: Connection) -> None:
        conn.rollback()
        assert type(caught.value) is wrapper_class and isinstance(caught.value.orig, plugin_class), \
            f"{plugin_class.__name__} arrives as sqlalchemy.exc.{wrapper_class.__name__} with the plugin's error as orig"
        assert conn.execute(text("SELECT 1")).scalar() == 1, "the connection stays usable after the error"

    with engine.connect() as conn:
        with pytest.raises(exc.IntegrityError) as tampered:
            conn.execute(text(f"SELECT ssn FROM {_USERS} WHERE name = 'tamper'")).fetchall()
        assert_wrapped(tampered, exc.IntegrityError, IntegrityCheckError, conn)

        with pytest.raises(exc.OperationalError) as literal:
            conn.execute(text(f"INSERT INTO {_USERS} (name, ssn) VALUES ('literal', 'pt-sa-literal')"))
        assert_wrapped(literal, exc.OperationalError, MetadataError, conn)

        with pytest.raises(exc.OperationalError) as no_exact_form:
            conn.execute(text(f"INSERT INTO {_USERS} (name, ssn) VALUES (:name, :ssn)"),
                         {"name": "generic", "ssn": complex(1, 2)})
        assert_wrapped(no_exact_form, exc.OperationalError, EncryptionFailedError, conn)

        with pytest.raises(exc.OperationalError) as unauthorized:
            conn.execute(text(f"INSERT INTO {_USERS} (name, pin) VALUES (:name, :pin)"),
                         {"name": "unauthorized", "pin": "pt-sa-pin"})
        assert_wrapped(unauthorized, exc.OperationalError, KeyManagementError, conn)


@kms_builds()
def test_utility_runs_on_an_engine_raw_connection(build: str, make_engine: Callable[..., Engine],
                                                  kms_settings: KmsSettings, kms_schema: str,
                                                  kms_master_key_arn: str, kms_verifier: PayloadVerifier) -> None:
    with plain_connect() as admin:
        with admin.cursor() as cur:
            for column in ("raw_a", "raw_b"):
                cur.execute(f"ALTER TABLE {_GUARDED} ADD COLUMN {column} {EncryptedValue().get_col_spec()}")
        admin.commit()
    client, config = kms_client(kms_settings), encryption_config(kms_settings, kms_schema)
    # A setup script's engine on SQLAlchemy's own dialect: its raw connection is a pool proxy around the driver's.
    setup_engine = create_engine("postgresql+psycopg://" if is_pg() else "mysql+mysqlconnector://",
                                 creator=lambda: plain_connect(build=build))
    try:
        for column, utility_for in (
                ("raw_a", lambda raw: KeyManagementUtility(raw.driver_connection, client, config)),
                ("raw_b", lambda raw: KeyManagementUtility(raw, client, config,
                                                           engine="postgresql" if is_pg() else "mysql"))):
            raw = setup_engine.raw_connection()
            try:
                utility_for(raw).initialize_encryption_for_column(_GUARDED, column, kms_master_key_arn)
                raw.commit()
            finally:
                raw.close()
        with setup_engine.connect() as conn:
            assert conn.execute(text("SELECT 1")).scalar() == 1, \
                "closing the raw connection after commit gave the pool a working connection back"
    finally:
        setup_engine.dispose()
    with make_engine().begin() as conn:
        conn.execute(text(f"INSERT INTO {_GUARDED} (name, raw_a, raw_b) VALUES (:name, :a, :b)"),
                     {"name": "raw", "a": "pt-sa-raw-a", "b": "pt-sa-raw-b"})
    for column in ("raw_a", "raw_b"):
        (payload,) = kms_verifier.stored(f"SELECT {column} FROM {_GUARDED} WHERE name = 'raw'")
        assert kms_verifier.decode(_GUARDED, column, payload)[1] == f"pt-sa-raw-{column[-1]}", \
            f"the utility set up {column} on the setup engine's raw connection, so the plugin encrypts it"
