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

import hashlib
import hmac
import struct
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum

import pytest

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService
from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker
from aws_advanced_python_wrapper.errors import (DecryptionError,
                                                EncryptionError,
                                                EncryptionFailedError,
                                                IntegrityCheckError,
                                                InvalidAlgorithmError,
                                                InvalidKeyError,
                                                TypeConversionError)

DATA_KEY = bytes(range(32))         # 32 bytes
HMAC_KEY = bytes(range(32, 64))     # 32 bytes
KEY_ID = 7


class Color(str, Enum):
    RED = "red"


class Masked(str):
    def __str__(self):
        return "***"


def enc(value, key_id=KEY_ID, dk=DATA_KEY, hk=HMAC_KEY, algorithm=EncryptionAlgorithm.DEFAULT):
    return EncryptionService.encrypt(value, dk, hk, algorithm, key_id=key_id)


def dec(payload, dk=DATA_KEY, hk=HMAC_KEY, algorithm=EncryptionAlgorithm.DEFAULT, target_type=None):
    return EncryptionService.decrypt(payload, dk, hk, algorithm, target_type=target_type)


# ---- payload layout ----

class TestPayloadLayout:
    def test_min_length_is_65(self):
        assert EncryptionService.MIN_ENCRYPTED_LENGTH == 65

    def test_layout_sizes(self):
        payload = enc("hello")
        assert len(payload) == 32 + 4 + 1 + 12 + len(b"hello") + 16

    def test_hmac_covers_everything_after_itself(self):
        import hashlib
        import hmac
        payload = enc("hello")
        expected = hmac.new(HMAC_KEY, payload[32:], hashlib.sha256).digest()
        assert payload[:32] == expected

    def test_key_id_recorded_before_marker(self):
        payload = enc("hello", key_id=12345)
        assert struct.unpack(">I", payload[32:36])[0] == 12345

    def test_marker_recorded_after_key_id(self):
        payload = enc("hello")
        assert payload[36] == TypeMarker.STRING

    def test_key_id_from_payload(self):
        assert EncryptionService.key_id_from_payload(enc("x", key_id=999)) == 999
        assert EncryptionService.key_id_from_payload(None) is None
        assert EncryptionService.key_id_from_payload(b"\x00" * 10) is None  # too short


# ---- round trips for every supported type ----

class TestRoundTrip:
    @pytest.mark.parametrize("value", [
        "hello", "", "café-ümlaut-漢字", "a" * 10000,
    ])
    def test_string(self, value):
        assert dec(enc(value)) == value

    def test_bytes(self):
        payload = enc(b"\x00\x01\x02\xff")
        out = dec(payload)
        assert out == b"\x00\x01\x02\xff"
        assert isinstance(out, bytes)

    @pytest.mark.parametrize("value", [0, 1, -1, 1234567890123, -1234567890123, 2 ** 62])
    def test_int(self, value):
        assert dec(enc(value)) == value

    @pytest.mark.parametrize("value", [0.0, 3.141592653589793, -2.5, 1e300])
    def test_float(self, value):
        assert dec(enc(value)) == value

    @pytest.mark.parametrize("value", [True, False])
    def test_bool(self, value):
        assert dec(enc(value)) is value

    def test_decimal(self):
        value = Decimal("12345.6789")
        assert dec(enc(value)) == value

    def test_local_date(self):
        value = date(2023, 6, 15)
        assert dec(enc(value)) == value

    def test_local_datetime(self):
        value = datetime(2023, 6, 15, 13, 45, 30)
        assert dec(enc(value)) == value

    def test_none_passthrough(self):
        assert enc(None) is None
        assert dec(None) is None


# ---- target type coercion ----

class TestTargetType:
    def test_coerce_to_str(self):
        assert dec(enc(123), target_type=str) == "123"

    def test_coerce_to_int(self):
        assert dec(enc("42"), target_type=int) == 42

    def test_coerce_to_bool(self):
        assert dec(enc("yes"), target_type=bool) is True
        assert dec(enc("no"), target_type=bool) is False


# ---- fail-closed behavior ----

class TestFailClosed:
    def test_too_short_raises_integrity(self):
        with pytest.raises(IntegrityCheckError):
            dec(b"\x00" * 10)

    def test_tampered_body_fails_hmac(self):
        payload = bytearray(enc("hello"))
        payload[-1] ^= 0xFF  # flip a ciphertext/tag byte
        with pytest.raises(IntegrityCheckError):
            dec(bytes(payload))

    def test_tampered_key_id_fails_hmac(self):
        payload = bytearray(enc("hello"))
        payload[34] ^= 0xFF  # key id is inside the HMAC-covered region
        with pytest.raises(IntegrityCheckError):
            dec(bytes(payload))

    @pytest.mark.parametrize("body", [
        b"",                                                 # tag only
        struct.pack(">I", 7),                                # tag + key id
        struct.pack(">I", 7) + bytes([TypeMarker.STRING]),   # + marker, no IV
    ])
    def test_signed_but_too_short_payload_is_an_integrity_error(self, body):
        # Signed with the right HMAC key, so only the length check refuses it;
        # without it the framing would fail with a bare IndexError/ValueError.
        payload = hmac.new(HMAC_KEY, body, hashlib.sha256).digest() + body
        with pytest.raises(IntegrityCheckError, match="too short"):
            dec(payload)

    def test_wrong_data_key_fails_gcm_not_hmac(self):
        # Decrypt with a different data key. The HMAC still matches (same HMAC
        # key and body), so this has to fail at GCM with DecryptionError, never
        # with IntegrityCheckError.
        import hashlib
        import hmac
        payload = enc("hello")
        wrong_dk = bytes([(b + 1) % 256 for b in DATA_KEY])
        with pytest.raises(DecryptionError):
            dec(payload, dk=wrong_dk)
        _ = (hashlib, hmac)

    def test_bad_data_key_length(self):
        with pytest.raises(InvalidKeyError):
            EncryptionService.encrypt("x", b"short", HMAC_KEY, key_id=1)

    def test_empty_hmac_key(self):
        with pytest.raises(InvalidKeyError):
            EncryptionService.encrypt("x", DATA_KEY, b"", key_id=1)

    @pytest.mark.parametrize("bad", [-1, 2 ** 32, True, "7", 1.5])
    def test_bad_key_id(self, bad):
        with pytest.raises(EncryptionError):
            EncryptionService.encrypt("x", DATA_KEY, HMAC_KEY, key_id=bad)


# ---- key rotation: value carries the id it was written with ----

class TestKeyId:
    def test_payload_carries_write_key_id(self):
        for kid in (0, 1, 7, 2 ** 32 - 1):
            assert EncryptionService.key_id_from_payload(enc("v", key_id=kid)) == kid


# ---- randomized IV ----

def test_iv_is_random_per_call():
    a = enc("same-value")
    b = enc("same-value")
    assert a != b  # fresh IV each time
    assert dec(a) == dec(b) == "same-value"


# ---- AES-128 ----

def test_aes_128_round_trip():
    dk16 = bytes(range(16))
    payload = EncryptionService.encrypt("x", dk16, HMAC_KEY, EncryptionAlgorithm.AES_128_GCM, key_id=1)
    assert EncryptionService.decrypt(payload, dk16, HMAC_KEY, EncryptionAlgorithm.AES_128_GCM) == "x"


# ---- wipe ----

def test_wipe_zeroes_bytearray():
    buf = bytearray(b"secret-key-material")
    EncryptionService.wipe(buf)
    assert buf == bytearray(len(buf))


def test_datetime_tz_normalization_matches_millis_contract():
    # Under the TIMESTAMP marker, a timezone-naive datetime is taken as UTC.
    naive = datetime(2023, 1, 1, 0, 0, 0)
    millis = EncryptionService._to_millis(naive, "TIMESTAMP")
    assert millis == int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)


# ---- marker coverage: generic, time, and reading an externally-written INTEGER ----

class TestMarkerCoverage:
    def test_generic_falls_back_to_str(self):
        class Thing:
            def __str__(self):
                return "thing-repr"
        assert dec(enc(Thing())) == "thing-repr"

    def test_time_round_trips_as_iso_text(self):
        from datetime import time
        value = time(13, 45, 30)
        # LOCAL_TIME is written as ISO text and read back as that text.
        assert dec(enc(value)) == value.isoformat()

    def test_reads_externally_written_integer_marker(self):
        # This wrapper writes integers as LONG; the 4-byte INTEGER marker (2) is still understood on read.
        import hashlib
        import hmac
        import struct

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        iv = b"\x09" * 12
        sealed = AESGCM(DATA_KEY).encrypt(iv, struct.pack(">i", 42), None)
        body = struct.pack(">I", KEY_ID) + bytes([TypeMarker.INTEGER]) + iv + sealed
        payload = hmac.new(HMAC_KEY, body, hashlib.sha256).digest() + body
        assert dec(payload) == 42

    def test_decimal_plain_not_scientific(self):
        # The wire contract requires plain notation for a Decimal that would
        # otherwise print in scientific notation. A round trip can't catch a
        # scientific-notation serializer, because "1E+2" decodes back equal to
        # 100 (str(Decimal("1E+2")) == "1E+2"), so assert the bytes.
        value = Decimal("1E+2")
        assert EncryptionService.serialize_value(value, TypeMarker.BIG_DECIMAL) == b"100"
        out = dec(enc(value))
        assert out == value


class TestAlgorithm:
    def test_unsupported_algorithm_raises(self):
        with pytest.raises(InvalidAlgorithmError):
            EncryptionService.encrypt("x", DATA_KEY, HMAC_KEY, "AES-999-XYZ", key_id=1)


# =====================================================================
# Edge cases, plus tests that pin behavior specific to Python.
# =====================================================================


def _resign(payload: bytes, hmac_key: bytes) -> bytes:
    """Re-sign a payload whose body was edited, so a test can get past the
    integrity check and exercise the decryption path itself."""
    body = payload[ES_TAG:]
    return hmac.new(hmac_key, body, hashlib.sha256).digest() + body


ES_TAG = 32          # HMAC_TAG_LENGTH
ES_KEYID = 4         # KEY_ID_LENGTH
MARKER_OFFSET = ES_TAG + ES_KEYID


# ---- serialize_value: markers it can/can't write ----

class TestSerializeValue:
    def test_raises_for_unknown_marker(self):
        # 12345 is no marker at all, so any value is refused.
        with pytest.raises(EncryptionFailedError, match="Unsupported type marker: 12345"):
            EncryptionService.serialize_value("x", 12345)

    def test_long_overflow_raises_without_leaking_the_value(self):
        # Every Python int but bool uses the LONG marker, so one outside the
        # signed 64-bit range must be refused. The error must not expose the
        # integer through its message, __cause__, or __context__.
        too_big = 2 ** 63  # one past the signed 64-bit max
        with pytest.raises(EncryptionFailedError) as ei:
            enc(too_big)
        err = ei.value
        assert err.data_type == "LONG"
        assert str(too_big) not in str(err)
        assert err.__cause__ is None
        assert err.__context__ is None

    def test_long_underflow_raises_without_leaking_the_value(self):
        # The lower half of the range check, held to the same no-leak rule as
        # the upper half. (test_long_out_of_range_keeps_its_own_message pins
        # the message.)
        too_small = -(2 ** 63) - 1
        with pytest.raises(EncryptionFailedError) as ei:
            enc(too_small)
        err = ei.value
        assert err.data_type == "LONG"
        assert str(too_small) not in str(err)
        assert err.__cause__ is None
        assert err.__context__ is None

    @pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), Decimal("sNaN")])
    def test_non_finite_decimal_is_refused(self, value):
        with pytest.raises(EncryptionFailedError):
            enc(value)

    def test_aware_time_is_refused(self):
        # A local time of day has no offset on the wire; writing one would drop it.
        with pytest.raises(EncryptionFailedError):
            enc(time(10, 30, tzinfo=timezone.utc))

    def test_aware_datetime_cannot_be_forced_into_local_date_time(self):
        with pytest.raises(EncryptionFailedError):
            EncryptionService.serialize_value(datetime(2024, 1, 1, tzinfo=timezone.utc), TypeMarker.LOCAL_DATE_TIME)

    @pytest.mark.parametrize("value,expected", [
        (date(999, 1, 2), b"0999-01-02"),
        (datetime(999, 1, 2, 3, 4, 5, 678), b"0999-01-02T03:04:05.000678"),
    ])
    def test_years_before_1000_keep_four_digits(self, value, expected):
        # strftime("%Y") does not zero-pad on every platform; the reader needs
        # four digits.
        assert EncryptionService.serialize_value(value) == expected
        assert dec(enc(value)) == value

    @pytest.mark.parametrize("micros,expected_millis", [
        (1500, 2), (2500, 3), (2499, 2), (-1500, -2), (-2500, -3),
    ])
    def test_millis_round_half_away_from_zero(self, micros, expected_millis):
        value = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)
        assert EncryptionService._to_millis(value, "TIMESTAMP") == expected_millis

    def test_millis_are_exact_far_from_the_epoch(self):
        value = datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc)
        expected = (value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)
        assert EncryptionService._to_millis(value, "TIMESTAMP") == expected

    @pytest.mark.parametrize("value,marker", [
        (date(2024, 1, 2), TypeMarker.LOCAL_DATE_TIME),   # a date is not a date-time
        ("not a date", TypeMarker.LOCAL_DATE),
        ("SECRET-123", TypeMarker.LONG),
        ("SECRET-123", TypeMarker.INTEGER),
        (2 ** 40, TypeMarker.INTEGER),                    # out of the 32-bit range
        ("SECRET-123", TypeMarker.DOUBLE),
        ("SECRET-123", TypeMarker.TIMESTAMP),
        ("SECRET-123", TypeMarker.DATE),
        (Decimal("NaN"), TypeMarker.LONG),                 # TypeError: a Decimal is not an integer
        (7, TypeMarker.BYTE_ARRAY),                       # bytes(7) would be 7 zero bytes
        ("false", TypeMarker.BOOLEAN),                    # not truthiness
        (1e300, TypeMarker.FLOAT),                        # OverflowError: too large for a 32-bit float
        (time(3, 4, 5), TypeMarker.LOCAL_DATE_TIME),      # a time of day is not a date-time
    ])
    def test_value_that_does_not_fit_an_explicit_marker(self, value, marker):
        # Mostly reachable only with an explicit marker. The error is a wrapper
        # error, names the marker, and quotes nothing of the value.
        with pytest.raises(EncryptionFailedError) as ei:
            EncryptionService.serialize_value(value, marker)
        err = ei.value
        assert "SECRET" not in str(err)
        assert err.__cause__ is None and err.__context__ is None
        assert err.data_type == TypeMarker.name_for(marker)

    @pytest.mark.parametrize("marker", [TypeMarker.DATE, TypeMarker.TIME, TypeMarker.TIMESTAMP])
    def test_out_of_range_instant_is_refused_naming_its_marker(self, marker):
        value = datetime(9999, 12, 31, 23, 0, tzinfo=timezone(timedelta(hours=-2)))  # after year 9999 in UTC
        with pytest.raises(EncryptionFailedError, match="outside the range") as ei:
            EncryptionService.serialize_value(value, marker)
        assert ei.value.data_type == TypeMarker.name_for(marker)

    @pytest.mark.parametrize("value", [bytearray(b"\x00\x01\xff"), memoryview(b"\x00\x01\xff")],
                             ids=["bytearray", "memoryview"])
    def test_bytes_like_values_round_trip_as_byte_array(self, value):
        # bytearray and memoryview (bytes is covered above) are accepted as
        # BYTE_ARRAY, and the caller's buffer is left alone.
        assert EncryptionService.serialize_value(value) == b"\x00\x01\xff"
        assert dec(enc(value)) == b"\x00\x01\xff"
        assert bytes(value) == b"\x00\x01\xff"

    @pytest.mark.parametrize("value", [1, 0])
    def test_int_is_not_written_as_boolean(self, value):
        with pytest.raises(EncryptionFailedError) as ei:
            EncryptionService.serialize_value(value, TypeMarker.BOOLEAN)
        assert ei.value.data_type == "BOOLEAN"

    @pytest.mark.parametrize("value", [1e300, Decimal("1E+30"), 1.5])
    def test_non_int_as_long_gets_the_generic_message(self, value):
        with pytest.raises(EncryptionFailedError, match="Cannot write a value of type") as ei:
            EncryptionService.serialize_value(value, TypeMarker.LONG)
        assert ei.value.data_type == "LONG"

    def test_instant_just_past_the_last_millisecond_is_refused(self):
        # The clamp only undoes rounding: the first millisecond after year
        # 9999 in UTC (no sub-millisecond part to round) is still refused.
        value = datetime(9999, 12, 31, 23, 0, tzinfo=timezone(timedelta(hours=-1)))  # 10000-01-01T00:00Z
        with pytest.raises(EncryptionFailedError, match="outside the range"):
            EncryptionService.serialize_value(value)

    def test_int_subclass_is_written_as_long(self):
        # TypeMarker.from_object sends any int but bool (an IntEnum, say)
        # to LONG.
        from enum import IntEnum

        class Level(IntEnum):
            HIGH = 7

        assert EncryptionService.serialize_value(Level.HIGH) == EncryptionService.serialize_value(7)
        assert dec(enc(Level.HIGH)) == 7

    def test_integer_like_value_is_accepted_as_long(self):
        # Anything with __index__ (numpy integer types, for example) is
        # accepted as LONG, as it is as INTEGER.
        class Index:
            def __index__(self):
                return 5

        assert EncryptionService.serialize_value(Index(), TypeMarker.LONG) == struct.pack(">q", 5)

    def test_value_whose_str_fails_is_refused(self):
        # str() of a huge int raises ValueError (the int-digit limit).
        with pytest.raises(EncryptionFailedError) as ei:
            EncryptionService.serialize_value(10 ** 5000, TypeMarker.STRING)
        assert ei.value.__cause__ is None and ei.value.__context__ is None

    @pytest.mark.parametrize("value", [2 ** 63, -(2 ** 63) - 1])
    def test_long_out_of_range_keeps_its_own_message(self, value):
        with pytest.raises(EncryptionFailedError, match="signed 64-bit range"):
            EncryptionService.serialize_value(value)

    def test_datetime_written_as_local_date_keeps_only_the_date(self):
        data = EncryptionService.serialize_value(datetime(2024, 1, 2, 3, 4, 5), TypeMarker.LOCAL_DATE)
        assert data == b"2024-01-02"
        assert EncryptionService.deserialize_value(data, TypeMarker.LOCAL_DATE) == date(2024, 1, 2)

    @pytest.mark.parametrize("value", [
        datetime(1, 1, 1, 0, 30, tzinfo=timezone(timedelta(hours=1))),           # before year 1 in UTC
        datetime(9999, 12, 31, 23, 0, tzinfo=timezone(timedelta(hours=-2))),     # after year 9999 in UTC
    ])
    def test_timestamp_that_could_not_be_read_back_is_refused(self, value):
        with pytest.raises(EncryptionFailedError):
            enc(value)

    def test_timestamp_at_the_edges_of_the_range_round_trips(self):
        for value in (datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc),
                      datetime(1, 1, 1, tzinfo=timezone.utc)):
            assert dec(enc(value)) == value

    def test_aware_datetime_max_keeps_its_last_millisecond(self):
        # A common "never" value: rounding .999999 up would pass the end of
        # the range, so it keeps the last representable millisecond instead.
        value = datetime.max.replace(tzinfo=timezone.utc)
        assert dec(enc(value)) == value.replace(microsecond=999000)

    def test_long_boundary_values_round_trip(self):
        # The signed 64-bit edges serialize and decrypt back exactly.
        for value in (2 ** 63 - 1, -(2 ** 63)):
            assert dec(enc(value)) == value

    @pytest.mark.parametrize("value", [Color.RED, Masked("red")], ids=["str_enum", "overridden_str"])
    def test_str_subclass_is_stored_as_its_own_text(self, value):
        # str() gives 'Color.RED' for the member and '***' for Masked.
        assert EncryptionService.serialize_value(value) == b"red"

    def test_str_subclass_reads_back_as_plain_str(self):
        out = dec(enc(Color.RED))
        assert out == "red" and type(out) is str

    @pytest.mark.parametrize("value,marker,expected", [
        (Color.RED, TypeMarker.GENERIC, b"Color.RED"),
        (42, TypeMarker.STRING, b"42"),
        (Decimal("1.5"), TypeMarker.STRING, b"1.5"),
    ])
    def test_generic_and_non_str_under_string_keep_str_of_value(self, value, marker, expected):
        assert EncryptionService.serialize_value(value, marker) == expected

    def test_object_that_only_claims_to_be_a_str_keeps_str_of_value(self, mocker):
        # isinstance() trusts a faked __class__ (proxies, mocks) and picks
        # STRING, but str.__str__ needs a real str, so the check is on type().
        proxy = mocker.MagicMock(spec=str)
        proxy.__str__.return_value = "red"
        assert EncryptionService.serialize_value(proxy) == b"red"


# ---- deserialize_value: reading every marker ----

class TestDeserializeExternalMarkers:
    """This wrapper never writes some of these markers or forms, but it must
    read every one."""

    def test_reads_32bit_integer(self):
        assert EncryptionService.deserialize_value(struct.pack(">i", 42), TypeMarker.INTEGER) == 42

    def test_reads_32bit_float(self):
        assert EncryptionService.deserialize_value(struct.pack(">f", 1.5), TypeMarker.FLOAT) == 1.5

    def test_reads_date_marker_as_date(self):
        millis = int(datetime(2026, 8, 11, tzinfo=timezone.utc).timestamp() * 1000)
        out = EncryptionService.deserialize_value(struct.pack(">q", millis), TypeMarker.DATE)
        assert out == date(2026, 8, 11)

    def test_reads_time_marker_as_datetime(self):
        millis = int(datetime(2026, 8, 11, 12, 30, 45, tzinfo=timezone.utc).timestamp() * 1000)
        out = EncryptionService.deserialize_value(struct.pack(">q", millis), TypeMarker.TIME)
        assert abs(out.timestamp() - millis / 1000.0) < 0.001

    def test_reads_local_time_as_string(self):
        data = EncryptionService.serialize_value("12:30:45", TypeMarker.LOCAL_TIME)
        assert EncryptionService.deserialize_value(data, TypeMarker.LOCAL_TIME) == "12:30:45"

    @pytest.mark.parametrize("text,expected", [
        # The local date-time forms the reader accepts: with or without seconds,
        # and with 3, 6, or 9 fractional digits. A naive fromisoformat fails on
        # the 9-digit (nanosecond) form on the Python 3.10 floor, so that form
        # needs coverage.
        ("2007-12-03T10:15", datetime(2007, 12, 3, 10, 15)),
        ("2007-12-03T10:15:30", datetime(2007, 12, 3, 10, 15, 30)),
        ("2007-12-03T10:15:30.123", datetime(2007, 12, 3, 10, 15, 30, 123000)),
        ("2007-12-03T10:15:30.123456", datetime(2007, 12, 3, 10, 15, 30, 123456)),
        # nanoseconds truncated to microseconds (datetime has no finer precision)
        ("2007-12-03T10:15:30.123456789", datetime(2007, 12, 3, 10, 15, 30, 123456)),
        # a suffix after the fraction is kept, whatever the fraction's length
        ("2007-12-03T10:15:30.123456789+00:00", datetime(2007, 12, 3, 10, 15, 30, 123456, tzinfo=timezone.utc)),
        ("2007-12-03T10:15:30.123+00:00", datetime(2007, 12, 3, 10, 15, 30, 123000, tzinfo=timezone.utc)),
    ])
    def test_reads_every_accepted_local_date_time_form(self, text, expected):
        data = text.encode("utf-8")
        assert EncryptionService.deserialize_value(data, TypeMarker.LOCAL_DATE_TIME) == expected

    def test_reads_own_local_date_time_write_form(self):
        # Round-trip the form this wrapper writes for a whole-second value,
        # which has no fraction.
        value = datetime(2023, 6, 15, 13, 45, 30)
        data = EncryptionService.serialize_value(value, TypeMarker.LOCAL_DATE_TIME)
        assert EncryptionService.deserialize_value(data, TypeMarker.LOCAL_DATE_TIME) == value

    def test_unparseable_local_date_time(self):
        with pytest.raises(TypeConversionError):
            EncryptionService.deserialize_value(b"not a datetime", TypeMarker.LOCAL_DATE_TIME)

    def test_wrong_width_for_marker(self):
        with pytest.raises(EncryptionError) as ei:
            EncryptionService.deserialize_value(b"ab", TypeMarker.INTEGER)
        assert "Expected 4 bytes for INTEGER, got 2" in str(ei.value)

    def test_unparseable_big_decimal(self):
        with pytest.raises(TypeConversionError):
            EncryptionService.deserialize_value(b"not a number", TypeMarker.BIG_DECIMAL)

    def test_unparseable_local_date(self):
        with pytest.raises(TypeConversionError):
            EncryptionService.deserialize_value(b"not a date", TypeMarker.LOCAL_DATE)

    @pytest.mark.parametrize("marker,data", [
        (TypeMarker.LOCAL_DATE, b"SECRET-ssn-123-45-6789"),
        (TypeMarker.LOCAL_DATE_TIME, b"SECRET-ssn-123-45-6789"),
        (TypeMarker.BIG_DECIMAL, b"SECRET-ssn-123-45-6789"),
        # UnicodeDecodeError.object holds the raw plaintext bytes.
        (TypeMarker.STRING, b"SECRET-ssn-\xff"),
    ])
    def test_deserialize_error_does_not_leak_plaintext(self, marker, data):
        # A value that passes HMAC and GCM but doesn't parse as its declared
        # type must not leak its plaintext through the error message,
        # __cause__, or __context__. This is the read-side counterpart of the
        # encrypt-side leak tests.
        with pytest.raises(TypeConversionError) as ei:
            EncryptionService.deserialize_value(data, marker)
        err = ei.value
        assert "SECRET" not in str(err)
        assert err.__cause__ is None
        assert err.__context__ is None

    @pytest.mark.parametrize("marker", [TypeMarker.DATE, TypeMarker.TIME, TypeMarker.TIMESTAMP])
    @pytest.mark.parametrize("millis", [2 ** 63 - 1, -(2 ** 63), 10 ** 15])
    def test_out_of_range_epoch_millis_is_a_type_conversion_error(self, marker, millis):
        # Another writer can store epoch millis no datetime can hold; the
        # resulting OverflowError must surface as the wrapper's error.
        with pytest.raises(TypeConversionError) as ei:
            EncryptionService.deserialize_value(struct.pack(">q", millis), marker)
        assert ei.value.__cause__ is None and ei.value.__context__ is None

    def test_unknown_marker_on_read(self):
        with pytest.raises(EncryptionError) as ei:
            EncryptionService.deserialize_value(b"x", 12345)
        assert "Unsupported type marker: 12345" in str(ei.value)


# ---- decrypt path: unknown marker inside a (re-signed) payload ----

def test_decrypt_rejects_unknown_marker_in_payload():
    payload = bytearray(enc("123-45-6789"))
    payload[MARKER_OFFSET] = 77  # overwrite the marker byte
    resigned = _resign(bytes(payload), HMAC_KEY)
    with pytest.raises(DecryptionError) as ei:
        dec(resigned)
    assert "Unknown type marker: 77" in str(ei.value)


# ---- decrypt-path data-key validation (past length check, before GCM) ----

def test_decrypt_rejects_wrong_length_data_key():
    payload = enc("123-45-6789")
    with pytest.raises(InvalidKeyError):
        EncryptionService.decrypt(payload, bytes(range(16)), HMAC_KEY)


# ---- missing data key / hmac key messages ----

def test_missing_data_key_message():
    with pytest.raises(EncryptionError) as ei:
        EncryptionService.encrypt("x", None, HMAC_KEY, key_id=1)
    assert "got None" in str(ei.value)


# ---- target-type coercion edge cases ----

class TestTargetTypeEdges:
    def test_boolean_db_spellings_true(self):
        for v in ("t", "true", "TRUE", "y", "yes", "1"):
            assert dec(enc(v), target_type=bool) is True

    def test_boolean_db_spellings_false(self):
        for v in ("f", "false", "n", "no", "0", ""):
            assert dec(enc(v), target_type=bool) is False

    def test_already_target_type_passes_through(self):
        assert dec(enc("42"), target_type=str) == "42"

    def test_unknown_target_type_raises(self):
        with pytest.raises(TypeConversionError):
            dec(enc("abc"), target_type=list)

    def test_uncoercible_value_raises(self):
        with pytest.raises(TypeConversionError):
            dec(enc("abc"), target_type=int)

    @pytest.mark.parametrize("value", [float("inf"), float("nan"), Decimal("Infinity")])
    def test_non_finite_to_int_is_a_type_conversion_error(self, value):
        # int() raises OverflowError / ValueError here, outside the PEP 249
        # hierarchy unless it is converted.
        with pytest.raises(TypeConversionError) as ei:
            EncryptionService._convert_to_target_type(value, int)
        assert ei.value.__cause__ is None
        assert ei.value.__context__ is None

    @pytest.mark.parametrize("target_type", [int, float, Decimal])
    def test_target_type_coercion_failure_does_not_leak_plaintext(self, target_type):
        # A value that decrypts but can't be coerced to the requested
        # target_type must not leak its plaintext through the error message,
        # __cause__, or __context__. int() and float() quote their input
        # ("invalid literal for int() ... 'SECRET...'"), so this path needs its
        # own test alongside the deserialize and encrypt leak tests.
        secret = "SECRET-ssn-123-45-6789"
        with pytest.raises(TypeConversionError) as ei:
            dec(enc(secret), target_type=target_type)
        err = ei.value
        assert "SECRET" not in str(err)
        assert err.__cause__ is None
        assert err.__context__ is None

    def test_float_to_int_target_truncates_python_difference(self):
        # int(1.5) truncates to 1 rather than raising. target_type is an
        # explicit opt-in and default reads return strings, so Python keeps its
        # native semantics. The test pins this so any change gets noticed.
        assert dec(enc(1.5), target_type=int) == 1


# ---- key and plaintext buffers ----

def test_encrypt_wipes_the_buffer_aes_gcm_saw_and_copies_no_key(monkeypatch):
    # The keys reach AES-GCM and HMAC as given (no immutable copy of a
    # bytearray key), and the plaintext buffer AES-GCM read is the one wiped.
    from types import SimpleNamespace

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    from aws_advanced_python_wrapper.encryption import encryption_service
    seen = {}

    class SpyAESGCM:
        def __init__(self, key):
            seen["data_key"] = key
            self._real = AESGCM(key)

        def encrypt(self, iv, plaintext, aad):
            seen["plaintext"] = plaintext
            return self._real.encrypt(iv, plaintext, aad)

    def spy_hmac_new(key, msg, digestmod):
        seen["hmac_key"] = key
        return hmac.new(key, msg, digestmod)

    monkeypatch.setattr(encryption_service, "AESGCM", SpyAESGCM)
    monkeypatch.setattr(encryption_service, "hmac", SimpleNamespace(new=spy_hmac_new, compare_digest=hmac.compare_digest))
    data_key, hmac_key = bytearray(DATA_KEY), bytearray(HMAC_KEY)
    EncryptionService.encrypt("secret", data_key, hmac_key, key_id=1)
    assert seen["data_key"] is data_key
    assert seen["hmac_key"] is hmac_key
    assert isinstance(seen["plaintext"], bytearray)
    assert seen["plaintext"] == bytearray(len(seen["plaintext"]))


@pytest.mark.parametrize("dk_type,hk_type", [
    (memoryview, memoryview), (bytearray, memoryview), (memoryview, bytes)])
def test_memoryview_keys_are_accepted(dk_type, hk_type):
    # BytesLike includes memoryview; hmac does not accept one directly.
    dk, hk = dk_type(DATA_KEY), hk_type(HMAC_KEY)
    payload = EncryptionService.encrypt("v", dk, hk, key_id=1)
    assert EncryptionService.decrypt(payload, dk, hk) == "v"
    assert EncryptionService.decrypt(payload, DATA_KEY, HMAC_KEY) == "v"


# ---- immutability: the caller's value is not mutated ----

def test_encrypt_does_not_mutate_caller_bytes():
    value = bytearray(b"123-45-6789")
    snapshot = bytes(value)
    enc(bytes(value))
    assert bytes(value) == snapshot


# ---- precision contract ----

class TestPrecision:
    def test_timestamp_marker_keeps_millisecond(self):
        # A value read via the TIMESTAMP marker keeps millisecond precision
        # (epoch-millis storage).
        millis = int(datetime(2026, 8, 11, 12, 30, 45, 123000, tzinfo=timezone.utc).timestamp() * 1000)
        out = EncryptionService.deserialize_value(struct.pack(">q", millis), TypeMarker.TIMESTAMP)
        assert abs(out.timestamp() * 1000 - millis) < 1.0

    def test_aware_datetime_keeps_its_instant(self):
        # An aware datetime is written as TIMESTAMP (epoch millis), so the
        # instant survives and it reads back as aware UTC, to the millisecond.
        v = datetime(2024, 1, 1, 12, 0, 0, 123456, tzinfo=timezone(timedelta(hours=5)))
        payload = enc(v)
        assert payload[MARKER_OFFSET] == TypeMarker.TIMESTAMP
        out = dec(payload)
        assert out == v.replace(microsecond=123000)
        assert out.utcoffset() == timedelta(0)

    def test_naive_datetime_keeps_microseconds(self):
        # A naive datetime is written as LOCAL_DATE_TIME (ISO text) with its
        # fraction, so it reads back equal, to the microsecond.
        v = datetime(2026, 8, 11, 12, 30, 45, 123456)
        payload = enc(v)
        assert payload[MARKER_OFFSET] == TypeMarker.LOCAL_DATE_TIME
        out = dec(payload)
        assert out == v
        assert out.microsecond == 123456

    def test_whole_second_naive_datetime_text_is_unchanged(self):
        # isoformat() leaves out a zero fraction, so a whole-second value is
        # still written as YYYY-MM-DDTHH:MM:SS and its stored text stays the
        # same.
        v = datetime(2026, 8, 11, 12, 30, 45)
        assert EncryptionService.serialize_value(v) == b"2026-08-11T12:30:45"


# ---- no-UTF-8-form string is refused without quoting the plaintext ----

def test_lone_surrogate_string_refused_without_quoting_plaintext():
    secret = "secret-\ud800-value"  # lone surrogate: no valid UTF-8 form
    with pytest.raises(EncryptionError) as ei:
        enc(secret)
    # UnicodeEncodeError holds the plaintext in .object. The production code
    # raises outside the except block so that error isn't attached as
    # __cause__ or __context__. Check the plaintext isn't reachable at all.
    assert "secret" not in str(ei.value)
    assert ei.value.__cause__ is None
    assert ei.value.__context__ is None


# ---- GCM auth independence from HMAC ----

def test_gcm_auth_tag_bound_to_data_key():
    # A payload encrypted+signed under a different data key (same HMAC key) must
    # pass the HMAC check but fail GCM when decrypted with the original data key.
    other_dk = bytes([(b + 7) % 256 for b in DATA_KEY])
    other = EncryptionService.encrypt("v", other_dk, HMAC_KEY, key_id=KEY_ID)
    with pytest.raises(DecryptionError):
        dec(other)  # HMAC matches (HMAC_KEY), data key wrong for DATA_KEY decrypt


# ---- exception hierarchy: lenient read mode relies on this ----

class TestExceptionHierarchy:
    """The read path's lenient mode (encryption_return_unverified_data) will
    catch IntegrityCheckError and let every other failure through. These tests
    pin the class hierarchy that makes `except IntegrityCheckError` work.
    """

    def test_integrity_check_error_classifies_as_integrity_error(self):
        # SQLAlchemy takes the first PEP 249 class in the MRO, so
        # IntegrityError has to come before EncryptionError's OperationalError.
        from aws_advanced_python_wrapper.pep249 import (IntegrityError,
                                                        OperationalError)
        mro = IntegrityCheckError.__mro__
        assert mro.index(IntegrityError) < mro.index(OperationalError)
        sqlalchemy_exc = pytest.importorskip("sqlalchemy.exc")
        wrapped = sqlalchemy_exc.DBAPIError.instance("SELECT 1", {}, IntegrityCheckError("x"), Exception)
        assert isinstance(wrapped, sqlalchemy_exc.IntegrityError)

    def test_integrity_and_decryption_are_distinct_subclasses(self):
        assert issubclass(IntegrityCheckError, EncryptionError)
        assert issubclass(DecryptionError, EncryptionError)
        assert not issubclass(DecryptionError, IntegrityCheckError)

    def test_tampered_value_is_catchable_as_integrity_only(self):
        payload = bytearray(enc("hello"))
        payload[-1] ^= 0xFF
        # Lenient mode would swallow this...
        with pytest.raises(IntegrityCheckError):
            dec(bytes(payload))

    def test_wrong_key_is_not_an_integrity_error(self):
        # ...but it must not swallow a wrong-key failure. That raises
        # DecryptionError, which is not an IntegrityCheckError.
        wrong_dk = bytes([(b + 1) % 256 for b in DATA_KEY])
        with pytest.raises(DecryptionError):
            dec(enc("hello"), dk=wrong_dk)
        try:
            dec(enc("hello"), dk=wrong_dk)
        except IntegrityCheckError:  # pragma: no cover - must not happen
            pytest.fail("wrong-key failure must not be an IntegrityCheckError")
        except DecryptionError:
            pass
