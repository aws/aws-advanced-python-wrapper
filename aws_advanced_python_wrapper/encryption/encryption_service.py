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

import hashlib
import hmac
import os
import struct
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (DecryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyError,
                                                TypeConversionError)

BytesLike = Union[bytes, bytearray, memoryview]


class EncryptionService:
    """Encrypts and decrypts single column values with AES-GCM, signed with a
    separate HMAC-SHA256 key.

    The payload uses the on-the-wire format shared by the AWS Advanced Wrappers
    and checked by the database-side trigger, so each wrapper can read values
    the others wrote::

        [ HMAC-SHA256 tag : 32 ][ key id : 4 ][ type marker : 1 ][ GCM IV : 12 ][ ciphertext ][ GCM tag : 16 ]

    The HMAC covers everything after itself, so the database can detect a
    tampered payload (the ``verify_encrypted_data_hmac`` SQL function) without
    holding the data key. The key id is the ``key_storage`` row the value was
    encrypted with, which keeps the value decryptable after its column's data
    key is rotated. It sits inside the HMAC-covered region, so tampering with
    it is detected too. The type marker records how the plaintext was
    serialized, so the original Python type can be restored on read.
    """

    HMAC_TAG_LENGTH = 32
    KEY_ID_LENGTH = 4
    TYPE_MARKER_LENGTH = 1
    GCM_IV_LENGTH = 12
    GCM_TAG_LENGTH = 16
    MIN_ENCRYPTED_LENGTH = (
        HMAC_TAG_LENGTH + KEY_ID_LENGTH + TYPE_MARKER_LENGTH + GCM_IV_LENGTH + GCM_TAG_LENGTH
    )

    _MILLIS_PER_SECOND = 1000.0

    # ---- public API ----

    @classmethod
    def encrypt(cls, value: Any, data_key: BytesLike, hmac_key: BytesLike,
                algorithm: str = EncryptionAlgorithm.DEFAULT, *, key_id: int) -> Optional[bytes]:
        """Encrypt a single value.

        :param value: the plaintext value; ``None`` is returned unchanged.
        :param data_key: the plaintext data key, binary.
        :param hmac_key: the HMAC-SHA256 key, binary.
        :param algorithm: an :class:`EncryptionAlgorithm` name.
        :param key_id: the ``key_storage`` id of the data key, recorded in the
            payload so the value stays decryptable after the column's key is
            rotated.
        :returns: the binary payload to store, or ``None`` when value is ``None``.
        :raises EncryptionError: if the key or algorithm is unusable.
        """
        if value is None:
            return None

        cls._validate_key(data_key, algorithm)
        cls._validate_hmac_key(hmac_key)
        cls._validate_key_id(key_id)

        marker = TypeMarker.from_object(value)
        plaintext = bytearray(cls.serialize_value(value, marker))
        try:
            # body = [ key id : 4 ][ type marker : 1 ][ IV : 12 ][ ciphertext ][ GCM tag : 16 ]
            body = struct.pack(">I", key_id) + cls._seal(bytes(plaintext), marker, data_key, algorithm)
            tag = hmac.new(cls._to_bytes(hmac_key), body, hashlib.sha256).digest()
            return tag + body
        finally:
            cls.wipe(plaintext)

    @classmethod
    def key_id_from_payload(cls, encrypted: Optional[BytesLike]) -> Optional[int]:
        """Read the key id recorded in a payload, without verifying or
        decrypting it. A payload with no key id (too short) yields ``None``.
        """
        if encrypted is None:
            return None
        data = cls._to_bytes(encrypted)
        if len(data) < cls.HMAC_TAG_LENGTH + cls.KEY_ID_LENGTH:
            return None
        return struct.unpack(">I", data[cls.HMAC_TAG_LENGTH:cls.HMAC_TAG_LENGTH + cls.KEY_ID_LENGTH])[0]

    @classmethod
    def decrypt(cls, encrypted: Optional[BytesLike], data_key: BytesLike, hmac_key: BytesLike,
                algorithm: str = EncryptionAlgorithm.DEFAULT, *, target_type: Optional[type] = None) -> Any:
        """Decrypt a payload produced by :meth:`encrypt`.

        :param target_type: the type to coerce the result to; defaults to the
            type recorded by the payload's marker.
        :raises EncryptionError: if the payload is malformed, has been tampered
            with, or cannot be decrypted with the given key.
        """
        if encrypted is None:
            return None

        data = cls._to_bytes(encrypted)
        if len(data) < cls.MIN_ENCRYPTED_LENGTH:
            raise IntegrityCheckError(
                f"Encrypted data is too short: {len(data)} bytes, "
                f"expected at least {cls.MIN_ENCRYPTED_LENGTH}",
                algorithm=algorithm,
            )

        cls._validate_key(data_key, algorithm)
        cls._validate_hmac_key(hmac_key)

        # The HMAC covers the key id and everything after it.
        body = data[cls.HMAC_TAG_LENGTH:]
        expected = data[:cls.HMAC_TAG_LENGTH]
        actual = hmac.new(cls._to_bytes(hmac_key), body, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, actual):
            raise IntegrityCheckError(
                "Integrity check failed: the encrypted value has been tampered with",
                algorithm=algorithm,
            )

        payload = body[cls.KEY_ID_LENGTH:]
        marker = cls._read_marker(payload)
        plaintext = bytearray(cls._unseal(payload, data_key, algorithm, marker))
        try:
            return cls._convert_to_target_type(cls.deserialize_value(bytes(plaintext), marker), target_type)
        finally:
            cls.wipe(plaintext)

    # ---- serialization ----

    @classmethod
    def serialize_value(cls, value: Any, marker: Optional[int] = None) -> bytes:
        """Serialize a value to the bytes that get encrypted."""
        if marker is None:
            marker = TypeMarker.from_object(value)

        if marker in (TypeMarker.STRING, TypeMarker.GENERIC):
            return cls._utf8_bytes(str(value))
        if marker == TypeMarker.LOCAL_TIME:
            # No native wire type for time-of-day; serialize it as ISO text.
            return cls._utf8_bytes(value.isoformat() if isinstance(value, time) else str(value))
        if marker == TypeMarker.BYTE_ARRAY:
            return cls._to_bytes(value)
        if marker == TypeMarker.INTEGER:
            return struct.pack(">i", value)
        if marker == TypeMarker.LONG:
            # Check the range up front instead of catching struct.error. The
            # struct message quotes the bounds, which share digits with the
            # plaintext, and raising inside the handler would attach that error
            # as __context__. bool is handled earlier, so value is an int.
            if not (-(2 ** 63) <= value <= 2 ** 63 - 1):
                raise EncryptionFailedError(
                    "Integer value exceeds the signed 64-bit range", data_type="LONG"
                )
            return struct.pack(">q", value)
        if marker == TypeMarker.DOUBLE:
            return struct.pack(">d", value)
        if marker == TypeMarker.FLOAT:
            return struct.pack(">f", value)
        if marker == TypeMarker.BOOLEAN:
            return b"\x01" if value else b"\x00"
        if marker == TypeMarker.BIG_DECIMAL:
            # Stored as a plain (non-scientific) decimal string so the bytes are
            # identical regardless of how the value was constructed.
            return cls._utf8_bytes(_decimal_to_plain_string(value))
        if marker in (TypeMarker.DATE, TypeMarker.TIME, TypeMarker.TIMESTAMP):
            return struct.pack(">q", cls._to_millis(value))
        if marker == TypeMarker.LOCAL_DATE:
            return cls._utf8_bytes(value.strftime("%Y-%m-%d"))
        if marker == TypeMarker.LOCAL_DATE_TIME:
            return cls._utf8_bytes(value.strftime("%Y-%m-%dT%H:%M:%S"))
        raise EncryptionFailedError(
            f"Unsupported value type: {type(value).__name__}",
            data_type=type(value).__name__,
        )

    @classmethod
    def deserialize_value(cls, data: bytes, marker: int) -> Any:
        """Turn decrypted bytes back into a Python value."""
        try:
            if marker in (TypeMarker.STRING, TypeMarker.GENERIC, TypeMarker.LOCAL_TIME):
                return cls._utf8(data)
            if marker == TypeMarker.BYTE_ARRAY:
                return bytes(data)
            if marker == TypeMarker.INTEGER:
                return struct.unpack(">i", cls._expect_length(data, 4, marker))[0]
            if marker == TypeMarker.LONG:
                return struct.unpack(">q", cls._expect_length(data, 8, marker))[0]
            if marker == TypeMarker.DOUBLE:
                return struct.unpack(">d", cls._expect_length(data, 8, marker))[0]
            if marker == TypeMarker.FLOAT:
                return struct.unpack(">f", cls._expect_length(data, 4, marker))[0]
            if marker == TypeMarker.BOOLEAN:
                return cls._expect_length(data, 1, marker)[0] != 0
            if marker == TypeMarker.BIG_DECIMAL:
                return Decimal(cls._utf8(data))
            if marker == TypeMarker.DATE:
                return cls._from_millis(struct.unpack(">q", cls._expect_length(data, 8, marker))[0]).date()
            if marker in (TypeMarker.TIME, TypeMarker.TIMESTAMP):
                return cls._from_millis(struct.unpack(">q", cls._expect_length(data, 8, marker))[0])
            if marker == TypeMarker.LOCAL_DATE:
                return datetime.strptime(cls._utf8(data), "%Y-%m-%d").date()
            if marker == TypeMarker.LOCAL_DATE_TIME:
                return cls._parse_local_datetime(cls._utf8(data))
        except (ValueError, struct.error, InvalidOperation):
            # Don't interpolate or chain the original exception. For the text
            # markers (LOCAL_DATE, LOCAL_DATE_TIME) its message quotes the
            # decrypted plaintext, and `__cause__` or `__context__` would carry
            # it too, so the error reports only the marker. The raise happens
            # below, outside the except block, because `from None` inside the
            # handler would still set `__context__`.
            failed_marker = marker
        else:
            raise DecryptionError(
                f"Unsupported type marker: {marker!r}", data_type=str(marker)
            )
        raise TypeConversionError(
            f"Failed to deserialize {TypeMarker.name_for(failed_marker) or failed_marker}",
            data_type=TypeMarker.name_for(failed_marker),
        )

    @classmethod
    def wipe(cls, buffer: Any) -> None:
        """Overwrite a mutable ``bytearray`` in place. Best effort: immutable
        ``bytes`` cannot be zeroed, so hold key material in ``bytearray`` where
        wiping matters. CPython may still leave copies elsewhere.
        """
        if isinstance(buffer, bytearray):
            for i in range(len(buffer)):
                buffer[i] = 0

    # ---- AES-GCM ----

    @classmethod
    def _seal(cls, plaintext: bytes, marker: int, data_key: BytesLike, algorithm: str) -> bytes:
        try:
            iv = os.urandom(cls.GCM_IV_LENGTH)
            # AESGCM appends the 16-byte tag to the ciphertext, giving the
            # [ciphertext][GCM tag] tail the wire format expects.
            sealed = AESGCM(cls._to_bytes(data_key)).encrypt(iv, plaintext, None)
            return bytes([marker]) + iv + sealed
        except Exception as e:  # pragma: no cover - cryptography rarely fails here
            raise EncryptionFailedError(
                f"Failed to encrypt value: {e}",
                algorithm=algorithm, data_type=TypeMarker.name_for(marker),
            ) from e

    @classmethod
    def _unseal(cls, payload: bytes, data_key: BytesLike, algorithm: str, marker: int) -> bytes:
        iv = payload[cls.TYPE_MARKER_LENGTH:cls.TYPE_MARKER_LENGTH + cls.GCM_IV_LENGTH]
        sealed = payload[cls.TYPE_MARKER_LENGTH + cls.GCM_IV_LENGTH:]
        try:
            return AESGCM(cls._to_bytes(data_key)).decrypt(iv, sealed, None)
        except InvalidTag as e:
            # The HMAC has already been checked, so a GCM failure here means the
            # data key is wrong rather than the data being corrupt.
            raise DecryptionError(
                "Failed to decrypt value: the authentication tag does not match this data key",
                algorithm=algorithm, data_type=TypeMarker.name_for(marker),
            ) from e

    # ---- helpers ----

    @classmethod
    def _convert_to_target_type(cls, value: Any, target_type: Optional[type]) -> Any:
        if target_type is None or value is None or isinstance(value, target_type):
            return value
        source_name = type(value).__name__
        try:
            if target_type is str:
                return str(value)
            if target_type is int:
                return int(value)
            if target_type is float:
                return float(value)
            if target_type is Decimal:
                return Decimal(str(value))
            if target_type is bool:
                return cls._truthy(value)
        except (ValueError, TypeError, InvalidOperation):
            # `value` is the recovered plaintext, and int()/float()/Decimal()
            # quote their input in the exception message, for example
            # int("abc") -> "...: 'abc'". Interpolating `{e}` or chaining the
            # exception would both leak it, and `from None` only hides
            # `__context__` from display without removing it. So the error
            # names only the types and is raised outside the handler, the same
            # way `serialize_value`, `deserialize_value`, and `_utf8_bytes` do.
            pass
        # Reached after a caught conversion failure, or when target_type
        # matched no branch above.
        raise TypeConversionError(
            f"Cannot convert {source_name} to {target_type}",
            data_type=str(target_type),
        )

    @classmethod
    def _read_marker(cls, payload: bytes) -> int:
        try:
            return TypeMarker.from_value(payload[0])
        except ValueError as e:
            raise DecryptionError(str(e)) from e

    @classmethod
    def _validate_key(cls, data_key: BytesLike, algorithm: str) -> None:
        expected = EncryptionAlgorithm.key_length(algorithm)
        if not isinstance(data_key, (bytes, bytearray, memoryview)) or len(data_key) != expected:
            got = len(data_key) if isinstance(data_key, (bytes, bytearray, memoryview)) else None
            raise InvalidKeyError(
                f"Data key must be {expected} bytes for {algorithm}, got {got!r}",
                algorithm=algorithm,
            )

    @classmethod
    def _validate_hmac_key(cls, hmac_key: BytesLike) -> None:
        if not isinstance(hmac_key, (bytes, bytearray, memoryview)) or len(hmac_key) == 0:
            raise InvalidKeyError("An HMAC key is required to protect encrypted values")

    @classmethod
    def _validate_key_id(cls, key_id: int) -> None:
        # Packed as an unsigned 32-bit integer. bool is excluded (it is an int subclass).
        if isinstance(key_id, bool) or not isinstance(key_id, int) or not (0 <= key_id <= 0xFFFFFFFF):
            raise EncryptionFailedError(
                f"A valid key id is required to tag an encrypted value, got {key_id!r}"
            )

    @classmethod
    def _expect_length(cls, data: bytes, length: int, marker: int) -> bytes:
        if len(data) != length:
            raise TypeConversionError(
                f"Expected {length} bytes for {TypeMarker.name_for(marker)}, got {len(data)}",
                data_type=TypeMarker.name_for(marker),
            )
        return data

    @staticmethod
    def _to_bytes(value: BytesLike) -> bytes:
        return value if isinstance(value, bytes) else bytes(value)

    @staticmethod
    def _utf8(data: bytes) -> str:
        return bytes(data).decode("utf-8")

    @classmethod
    def _utf8_bytes(cls, string: str) -> bytes:
        """Encode the string as UTF-8. A str with no UTF-8 form (lone
        surrogates) is refused, and the error does not quote the offending
        character, since that character is plaintext."""
        try:
            return string.encode("utf-8")
        except UnicodeEncodeError:
            # Raise outside the except block. UnicodeEncodeError holds the full
            # plaintext in `.object`, and raising inside the handler would
            # attach it as the new error's __context__, which `from None` hides
            # from display but does not remove.
            pass
        raise EncryptionFailedError(
            "Cannot encrypt a string with no UTF-8 form", data_type="str"
        )

    @classmethod
    def _to_millis(cls, value: Any) -> int:
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
            return round(dt.timestamp() * cls._MILLIS_PER_SECOND)
        if isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            return round(dt.timestamp() * cls._MILLIS_PER_SECOND)
        raise EncryptionFailedError(
            f"Cannot convert {type(value).__name__} to a timestamp"
        )

    @classmethod
    def _from_millis(cls, millis: int) -> datetime:
        return datetime.fromtimestamp(millis / cls._MILLIS_PER_SECOND, tz=timezone.utc)

    @staticmethod
    def _parse_local_datetime(text: str) -> datetime:
        """Parse an ISO-8601 local date-time in any form the wrappers write.

        This wrapper always writes ``YYYY-MM-DDTHH:MM:SS`` with no fractional
        part. Other wrappers drop the seconds when they are zero
        (``...THH:MM``) and append 3, 6, or 9 fractional digits when present.
        ``datetime.fromisoformat`` accepts the no-seconds, millisecond, and
        microsecond forms, but on Python 3.10 (the support floor) it rejects
        9-digit nanoseconds, so the fraction is truncated to microseconds
        first. ``datetime`` can't hold anything finer, and this marker only
        promises millisecond/second precision anyway.
        """
        if "." in text:
            head, _, frac = text.partition(".")
            # Stop at the first non-digit (a timezone designator, which a
            # local date-time never has), then keep at most 6 digits.
            digits = ""
            for ch in frac:
                if ch.isdigit():
                    digits += ch
                else:
                    break
            if len(digits) > 6:
                text = f"{head}.{digits[:6]}"
        return datetime.fromisoformat(text)

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("t", "true", "y", "yes", "1")


def _decimal_to_plain_string(value: Decimal) -> str:
    """Render a Decimal in plain (non-scientific) notation, so the stored bytes
    are identical regardless of how the value was constructed."""
    sign, digits, exponent = value.as_tuple()
    if not isinstance(exponent, int):  # 'n'/'N'/'F' for NaN/Infinity
        return str(value)
    return format(value, "f")
