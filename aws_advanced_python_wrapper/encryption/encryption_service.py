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
import operator
import os
import struct
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any, Optional, Union

try:
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ModuleNotFoundError as e:  # pragma: no cover - exercised by the import-guard test
    # Only a missing package gets the install hint. An installed but broken
    # cryptography raises its own ImportError, which is clearer as it is.
    if (e.name or "").partition(".")[0] != "cryptography":
        raise
    raise ImportError(
        "The KMS encryption plugin needs the 'cryptography' package. "
        "Install it with: pip install 'aws-advanced-python-wrapper[encryption]'") from e

from aws_advanced_python_wrapper.encryption import buffers
from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (DecryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidKeyError,
                                                TypeConversionError)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.encryption.buffers import BytesLike

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MICROSECOND = timedelta(microseconds=1)
_MILLISECOND = timedelta(milliseconds=1)
# The epoch-millis range a datetime can be read back into.
_MIN_MILLIS = -((_EPOCH - datetime.min.replace(tzinfo=timezone.utc)) // _MILLISECOND)
_MAX_MILLIS = (datetime.max.replace(tzinfo=timezone.utc) - _EPOCH) // _MILLISECOND


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

    Errors never quote plaintext. Several standard-library failures do quote
    their input (``UnicodeEncodeError.object``, ``int("...")``,
    ``datetime.fromisoformat``), and an exception raised inside an ``except``
    block keeps the caught one as ``__context__`` even with ``from None``. So
    where such a failure is caught, the wrapper error is raised after the
    ``except`` block, with nothing chained.
    """

    HMAC_TAG_LENGTH = 32
    KEY_ID_LENGTH = 4
    TYPE_MARKER_LENGTH = 1
    GCM_IV_LENGTH = 12
    GCM_TAG_LENGTH = 16
    MIN_ENCRYPTED_LENGTH = (
        HMAC_TAG_LENGTH + KEY_ID_LENGTH + TYPE_MARKER_LENGTH + GCM_IV_LENGTH + GCM_TAG_LENGTH
    )

    #: Zero a ``bytearray`` in place; see :func:`buffers.wipe`.
    wipe = staticmethod(buffers.wipe)

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
        :return: the binary payload to store, or ``None`` when value is ``None``.
        :raises EncryptionError: if the key, algorithm, or key id is unusable,
            or the value cannot be serialized.
        """
        if value is None:
            return None

        cls._validate_key(data_key, algorithm)
        cls._validate_hmac_key(hmac_key)
        cls._validate_key_id(key_id)

        marker = TypeMarker.from_object(value)
        # The buffer handed to AES-GCM is the one wiped afterwards.
        plaintext = bytearray(cls.serialize_value(value, marker))
        try:
            # body = [ key id : 4 ][ type marker : 1 ][ IV : 12 ][ ciphertext ][ GCM tag : 16 ]
            body = struct.pack(">I", key_id) + cls._seal(plaintext, marker, data_key, algorithm)
            tag = hmac.new(cls._hmac_key(hmac_key), body, hashlib.sha256).digest()
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
        :raises IntegrityCheckError: if the payload is too short or its HMAC
            does not verify: it was modified, was never encrypted, or was
            signed with a different HMAC key.
        :raises EncryptionError: if the payload cannot be decrypted with the
            given key or its value cannot be restored.
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
        actual = hmac.new(cls._hmac_key(hmac_key), body, hashlib.sha256).digest()
        if not hmac.compare_digest(expected, actual):
            raise IntegrityCheckError(
                "Integrity check failed: the value was modified, was never encrypted, "
                "or was signed with a different HMAC key",
                algorithm=algorithm,
            )

        payload = body[cls.KEY_ID_LENGTH:]
        marker = cls._read_marker(payload)
        # The recovered value is returned to the caller as a Python object, so
        # there is no buffer here worth wiping.
        plaintext = cls._unseal(payload, data_key, algorithm, marker)
        return cls._convert_to_target_type(cls.deserialize_value(plaintext, marker), target_type)

    # ---- serialization ----

    @classmethod
    def serialize_value(cls, value: Any, marker: Optional[int] = None) -> bytes:
        """Serialize a value to the bytes that get encrypted.

        :param marker: the marker to write; defaults to the one
            :meth:`TypeMarker.from_object` picks for ``value``.
        :raises EncryptionFailedError: if ``value`` cannot be written with
            that marker.
        """
        if marker is None:
            marker = TypeMarker.from_object(value)
        if not TypeMarker.is_known(marker):
            raise EncryptionFailedError(f"Unsupported type marker: {marker!r}")
        try:
            return cls._serialize(value, marker)
        except (TypeError, ValueError, AttributeError, ArithmeticError, struct.error):
            # Mostly a value that does not fit an explicitly requested marker
            # (or a GENERIC value whose __str__ fails). The caught message may
            # quote the value, so it is not chained, and the error is raised
            # below, outside the handler. ArithmeticError covers OverflowError
            # and decimal.InvalidOperation.
            pass
        raise EncryptionFailedError(
            f"Cannot write a value of type {type(value).__name__} as {TypeMarker.name_for(marker)}",
            data_type=TypeMarker.name_for(marker))

    @classmethod
    def _serialize(cls, value: Any, marker: int) -> bytes:
        if marker in (TypeMarker.STRING, TypeMarker.GENERIC):
            return cls._utf8_bytes(str(value))
        if marker == TypeMarker.LOCAL_TIME:
            # LOCAL_TIME is ISO text on the wire; TIME is the epoch-millis form.
            if isinstance(value, time):
                cls._require_naive(value, "LOCAL_TIME")
                return cls._utf8_bytes(value.isoformat())
            return cls._utf8_bytes(str(value))
        if marker == TypeMarker.BYTE_ARRAY:
            # bytes(7) would be seven zero bytes, so only bytes-like values.
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise TypeError("BYTE_ARRAY needs a bytes-like value")
            return cls._to_bytes(value)
        if marker == TypeMarker.INTEGER:
            return struct.pack(">i", value)
        if marker == TypeMarker.LONG:
            # Every Python int but bool defaults to LONG, so an out-of-range
            # one gets its own message rather than the generic one.
            # operator.index accepts what INTEGER's struct packing accepts
            # (int, bool, other integer types) and raises TypeError for a
            # float or Decimal.
            value = operator.index(value)
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
            # Not truthiness: "false" must not be written as True.
            if not isinstance(value, bool):
                raise TypeError("BOOLEAN needs a bool")
            return b"\x01" if value else b"\x00"
        if marker == TypeMarker.BIG_DECIMAL:
            # Plain (non-scientific) notation, so Decimal("1E+2") and
            # Decimal("100") give the same bytes. The scale is kept:
            # Decimal("1.0") is written as "1.0".
            if not value.is_finite():
                raise EncryptionFailedError(
                    "Cannot encrypt a non-finite Decimal", data_type="BIG_DECIMAL")
            return cls._utf8_bytes(format(value, "f"))
        if marker in (TypeMarker.DATE, TypeMarker.TIME, TypeMarker.TIMESTAMP):
            return struct.pack(">q", cls._to_millis(value, TypeMarker.name_for(marker) or ""))
        if marker == TypeMarker.LOCAL_DATE:
            # isoformat always writes a 4-digit year; strftime("%Y") does not
            # on every platform. A datetime is cut to its own (wall-clock)
            # date, as a local date has no offset to keep.
            return cls._utf8_bytes(date(value.year, value.month, value.day).isoformat())
        if marker == TypeMarker.LOCAL_DATE_TIME:
            if not isinstance(value, datetime):
                raise EncryptionFailedError(
                    f"Cannot write a value of type {type(value).__name__} as LOCAL_DATE_TIME",
                    data_type="LOCAL_DATE_TIME")
            cls._require_naive(value, "LOCAL_DATE_TIME")
            return cls._utf8_bytes(value.replace(microsecond=0).isoformat())
        raise EncryptionFailedError(  # pragma: no cover - serialize_value checks the marker first
            f"Unsupported type marker: {marker!r}")

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
                return date.fromisoformat(cls._utf8(data))
            if marker == TypeMarker.LOCAL_DATE_TIME:
                return cls._parse_local_datetime(cls._utf8(data))
        except (ValueError, struct.error, InvalidOperation, OverflowError):
            # The text markers' parse errors quote the decrypted plaintext, so
            # the error reports only the marker and is raised below, outside
            # the handler (see the class docstring).
            failed_marker = marker
        else:
            raise DecryptionError(
                f"Unsupported type marker: {marker!r}", data_type=str(marker)
            )
        raise TypeConversionError(
            f"Failed to deserialize {TypeMarker.name_for(failed_marker) or failed_marker}",
            data_type=TypeMarker.name_for(failed_marker),
        )

    # ---- AES-GCM ----

    @classmethod
    def _seal(cls, plaintext: BytesLike, marker: int, data_key: BytesLike, algorithm: str) -> bytes:
        try:
            iv = os.urandom(cls.GCM_IV_LENGTH)
            # AESGCM appends the 16-byte tag to the ciphertext, giving the
            # [ciphertext][GCM tag] tail the wire format expects. It accepts a
            # bytearray key, so no immutable copy of the key is made here.
            sealed = AESGCM(data_key).encrypt(iv, plaintext, None)
            return bytes([marker]) + iv + sealed
        except Exception as e:  # pragma: no cover - cryptography rarely fails here
            raise EncryptionFailedError(
                f"Failed to encrypt value: {type(e).__name__}",
                algorithm=algorithm, data_type=TypeMarker.name_for(marker),
            ) from e

    @classmethod
    def _unseal(cls, payload: bytes, data_key: BytesLike, algorithm: str, marker: int) -> bytes:
        iv = payload[cls.TYPE_MARKER_LENGTH:cls.TYPE_MARKER_LENGTH + cls.GCM_IV_LENGTH]
        sealed = payload[cls.TYPE_MARKER_LENGTH + cls.GCM_IV_LENGTH:]
        try:
            return AESGCM(data_key).decrypt(iv, sealed, None)
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
        except (ValueError, TypeError, ArithmeticError):
            # `value` is the recovered plaintext and int()/float()/Decimal()
            # quote their input, so nothing is interpolated or chained, and the
            # error is raised below, outside the handler (see the class
            # docstring). ArithmeticError covers OverflowError (int of an
            # infinite float or Decimal) and decimal.InvalidOperation.
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

    @staticmethod
    def _require_naive(value: Any, marker_name: str) -> None:
        # A local date-time or time of day has no UTC offset on the wire, so an
        # aware value would silently lose it. Aware datetimes are written as
        # TIMESTAMP instead (see TypeMarker.from_object).
        if isinstance(value, (datetime, time)) and value.utcoffset() is not None:
            raise EncryptionFailedError(
                f"Cannot write a timezone-aware value as {marker_name}", data_type=marker_name)

    @classmethod
    def _expect_length(cls, data: bytes, length: int, marker: int) -> bytes:
        if len(data) != length:
            raise TypeConversionError(
                f"Expected {length} bytes for {TypeMarker.name_for(marker)}, got {len(data)}",
                data_type=TypeMarker.name_for(marker),
            )
        return data

    @staticmethod
    def _hmac_key(key: BytesLike) -> Union[bytes, bytearray]:
        # hmac takes bytes or bytearray but not memoryview, so a memoryview key
        # is copied to bytes (which cannot be wiped). A bytearray key is passed
        # as is, so no immutable copy of it is made.
        return key if isinstance(key, (bytes, bytearray)) else bytes(key)

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
            # UnicodeEncodeError holds the full plaintext in `.object`; raised
            # below, outside the handler (see the class docstring).
            pass
        raise EncryptionFailedError(
            "Cannot encrypt a string with no UTF-8 form", data_type="str"
        )

    @classmethod
    def _to_millis(cls, value: Any, marker_name: str) -> int:
        """Milliseconds since the Unix epoch, rounded half away from zero,
        except at the very end of the datetime range, which keeps its last
        millisecond rather than rounding past it.

        Integer arithmetic, so the result is exact for any datetime; a float
        ``timestamp()`` loses microseconds far from the epoch.
        """
        if isinstance(value, datetime):
            dt = value if value.utcoffset() is not None else value.replace(tzinfo=timezone.utc)
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
        else:
            raise TypeError(f"{marker_name} needs a date or datetime")
        micros = (dt - _EPOCH) // _MICROSECOND
        millis, remainder = divmod(abs(micros), 1000)
        if remainder >= 500:
            millis += 1
        millis = millis if micros >= 0 else -millis
        if micros // 1000 == _MAX_MILLIS:
            # Rounding up past the last representable millisecond (for example
            # an aware datetime.max, a common "never" value) keeps that
            # millisecond.
            millis = _MAX_MILLIS
        if not _MIN_MILLIS <= millis <= _MAX_MILLIS:
            # A UTC offset put the instant outside what a datetime can hold,
            # so it could not be read back.
            raise EncryptionFailedError(
                "In UTC, the value is outside the range a datetime can represent", data_type=marker_name)
        return millis

    @classmethod
    def _from_millis(cls, millis: int) -> datetime:
        return _EPOCH + millis * _MILLISECOND

    @staticmethod
    def _parse_local_datetime(text: str) -> datetime:
        """Parse an ISO-8601 local date-time in any form the wrappers write.

        This wrapper writes ``YYYY-MM-DDTHH:MM:SS`` with no fractional part.
        Other wrappers drop the seconds when they are zero (``...THH:MM``)
        and append 3, 6, or 9 fractional digits when present.
        ``datetime.fromisoformat`` accepts the no-seconds, 3-digit, and
        6-digit forms, but on Python 3.10 (the support floor) it rejects 9
        digits, so the fraction is cut to microseconds, the finest unit
        ``datetime`` can hold. Anything after the digits is kept as is.
        """
        head, dot, rest = text.partition(".")
        if dot:
            digits = len(rest) - len(rest.lstrip("0123456789"))
            if digits > 6:
                text = f"{head}.{rest[:6]}{rest[digits:]}"
        return datetime.fromisoformat(text)

    @staticmethod
    def _truthy(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in ("t", "true", "y", "yes", "1")
