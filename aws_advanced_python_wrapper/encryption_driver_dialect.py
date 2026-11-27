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

from typing import (TYPE_CHECKING, Any, ClassVar, FrozenSet, Mapping, Optional,
                    Sequence, Tuple)

if TYPE_CHECKING:
    from re import Pattern

    from aws_advanced_python_wrapper.pep249 import Connection, Cursor

from abc import ABC, abstractmethod

from aws_advanced_python_wrapper.driver_dialect import DriverDialect


class EncryptionDriverDialect(DriverDialect, ABC):
    """
    The driver dialect members the KMS Encryption Plugin uses.
    The plugin refuses any other driver dialect.
    """

    # The equality operator the encryption plugin writes in its own statements.
    equals_operator: ClassVar[str] = "="
    # The leading words of the statements the encryption plugin parses; it reports any other statement without parsing it.
    analyzed_statements: ClassVar[FrozenSet[str]] = frozenset()
    # The sqlglot dialect the encryption plugin parses statements with.
    sqlglot_dialect: ClassVar[str] = ""
    # Whether the engine accepts a qualified column (t.c) in an INSERT column list, which the parser rejects.
    qualified_insert_columns: ClassVar[bool] = False
    # The name upsert_clause gives the incoming row (EXCLUDED.c), or None when the clause names no row.
    upsert_row_alias: ClassVar[Optional[str]] = None
    # Whether upsert_clause reads the incoming row through the VALUES(c) function.
    upsert_values_function: ClassVar[bool] = False
    # The character that quotes an identifier.
    identifier_quote: ClassVar[str] = '"'
    # The modifiers that can follow a statement keyword and change how the statement runs, not what it writes.
    statement_modifiers: ClassVar[Mapping[str, FrozenSet[str]]] = {}
    # The character sets whose introducer (_binary) can come directly before a placeholder.
    charset_introducers: ClassVar[FrozenSet[str]] = frozenset()
    # The SQL token pattern, tried in order at each token boundary, or None when the encryption plugin can't lex SQL.
    # Its groups: line_comment, block_comment (with block_comment_end when comments don't nest), string, estring,
    # dqstring and ident (each with <name>_end, empty when the text ends first), dollar, param, word, number,
    # semicolon, space and other, which matches any character.
    sql_token_pattern: ClassVar[Optional[Pattern[str]]] = None
    # Whether block comments nest.
    nested_block_comments: ClassVar[bool] = False
    # Whether the server runs the body of a /*! ... */ comment.
    executable_comments: ClassVar[bool] = False
    # Whether the driver reads every % as a placeholder or %%, and rejects the rest, when it gets parameters.
    strict_placeholders: ClassVar[bool] = False
    _ASCII: ClassVar[bytes] = bytes(range(0x80))

    @classmethod
    def binds_parameters(cls, params: Any) -> bool:
        """
        Returns whether the driver substitutes the placeholders of a statement when ``execute`` gets ``params``.

        :param params: the second argument of ``execute``.
        :return: True when the driver substitutes the placeholders.
        """
        return params is not None

    @classmethod
    def fold_identifier(cls, name: str, quoted: bool) -> str:
        """
        Returns the name the server resolves an identifier written in SQL to.

        :param name: the identifier without its quotes, with doubled quotes undone.
        :param quoted: whether the identifier was quoted.
        :return: the name the server resolves the identifier to.
        """
        return name

    @classmethod
    def metadata_key(cls, name: str) -> str:
        """
        Returns the form in which a table or column name is compared when the encryption plugin looks it up in
        ``encryption_metadata``.
        Names with the same key share encryption metadata.

        :param name: the table or column name.
        :return: the key the name is compared by.
        """
        return name

    @abstractmethod
    def upsert_clause(
            self, conflict_columns: Sequence[str], update_columns: Sequence[str], extra_assignments: Sequence[str] = ()) -> str:
        """
        Returns the trailing upsert clause for an ``INSERT``.
        On a conflict over ``conflict_columns``, the clause overwrites ``update_columns`` from the incoming row and
        applies each of ``extra_assignments``.

        :param conflict_columns: the columns of the unique key the clause matches on.
        :param update_columns: the columns the clause overwrites from the incoming row.
        :param extra_assignments: literal assignments the clause also applies.
        :return: the upsert clause.
        """
        ...

    @abstractmethod
    def insert_returning_id(self, conn: Connection, sql: str, params: Sequence[Any]) -> int:
        """
        Runs an ``INSERT`` written without ``RETURNING`` and returns the generated ``id``.

        :param conn: the connection to run the statement on.
        :param sql: the ``INSERT`` statement.
        :param params: the statement's parameters.
        :return: the generated ``id``.
        """
        ...

    def analyzable_sql(self, sql: Any, cursor: Cursor) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns the text the encryption plugin analyzes for ``sql``, and the codec of the connection's client encoding
        when ``sql`` is ``bytes``.

        :param sql: the SQL the application passed to the cursor.
        :param cursor: the driver cursor the SQL runs on.
        :return: the text to analyze, or None when the SQL runs unanalyzed, and the codec ``bytes`` SQL was decoded
            with, or None.
        """
        if isinstance(sql, str):
            return sql, None
        return None, None

    def binary_placeholder_prefix(self, cursor: Cursor) -> Optional[str]:
        """
        Returns the text the encryption plugin puts before each placeholder whose value it encrypts, so the server
        reads the payload as binary data.

        :param cursor: the driver cursor the statement runs on.
        :return: the prefix, or None when the server already reads a bound ``bytes`` value as binary data.
        """
        return None

    def unsupported_row_factory(self, cursor: Cursor) -> Optional[Any]:
        """
        Returns the cursor's row factory when the encryption plugin can't decode the rows it builds.

        :param cursor: the driver cursor the rows come from.
        :return: the unsupported row factory, or None.
        """
        return None

    @staticmethod
    def _decode_sql(sql: bytes, codec: Optional[str]) -> Optional[str]:
        """
        Returns the text the encryption plugin analyzes in ``bytes`` SQL.

        :param sql: the SQL the application passed to the cursor.
        :param codec: the codec of the connection's client encoding, or None when the connection is unknown.
        :return: the text, or None when ``codec`` is None, reads an ASCII byte as another character, can't decode
            ``sql``, or doesn't encode the text back to ``sql``.
        """
        if codec is None:
            return None
        try:
            # A codec that reads an ASCII byte as another character (shift_jis_2004 reads 0x5C and 0x7E as a yen sign
            # and an overline), or text that doesn't encode back to the same bytes, would show the analysis another
            # statement.
            if EncryptionDriverDialect._ASCII.decode(codec) != EncryptionDriverDialect._ASCII.decode("ascii"):
                return None
            text = sql.decode(codec)
            return text if text.encode(codec) == sql else None
        # Their .object is the whole statement: keep nothing of it.
        except (UnicodeDecodeError, UnicodeEncodeError):
            return None
