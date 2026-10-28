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

"""Reference encoder for ``conformance_vectors.json``.

Builds each payload straight from the wire-format spec with raw
``cryptography`` and the standard library. It imports nothing from
``aws_advanced_python_wrapper``, so decrypting its output tests
the wrapper against the spec rather than against itself.

Each vector uses a fixed IV made of one repeated byte, unique per vector, so
the output is deterministic and can never come from the wrapper's own
encrypt (which draws a random IV). Add a vector with the next unused IV byte,
then regenerate::

    python tests/unit/resources/encryption/generate_conformance_vectors.py > \\
        tests/unit/resources/encryption/conformance_vectors.json

``test_encryption_conformance.py`` checks that the committed file matches
this script's output.
"""

import base64
import hashlib
import hmac
import json
import struct
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, NamedTuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

DATA_KEY = bytes(range(32))
HMAC_KEY = bytes(range(32, 64))
KEY_ID = 7

STRING, INTEGER, LONG, DOUBLE, FLOAT, BOOLEAN, BIG_DECIMAL = 1, 2, 3, 4, 5, 6, 7
DATE, TIME, TIMESTAMP, LOCAL_DATE, LOCAL_TIME, LOCAL_DATE_TIME, BYTE_ARRAY = 8, 9, 10, 11, 12, 13, 14
GENERIC = 99


def _millis(iso: str) -> bytes:
    instant = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
    return struct.pack(">q", (instant - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1))


class Vector(NamedTuple):
    name: str
    marker: int
    plaintext: bytes
    iv_byte: int
    expected: Dict[str, Any]
    key_id: int = KEY_ID


VECTORS: List[Vector] = [
    Vector("string", STRING, b"hello", 0x00, {"expect": "hello"}),
    Vector("unicode", STRING, "café-漢字".encode(), 0x01, {"expect": "café-漢字"}),
    Vector("long", LONG, struct.pack(">q", 1234567890123), 0x02, {"expect": 1234567890123}),
    Vector("double", DOUBLE, struct.pack(">d", 3.14159), 0x03, {"expect": 3.14159}),
    Vector("bool_t", BOOLEAN, b"\x01", 0x04, {"expect": True}),
    Vector("bool_f", BOOLEAN, b"\x00", 0x09, {"expect": False}),
    Vector("bytes", BYTE_ARRAY, b"\x00\x01\x02\xff", 0x05, {"expect_b64": "AAEC/w=="}),
    Vector("decimal", BIG_DECIMAL, b"12345.6789", 0x06, {"expect": "12345.6789"}),
    Vector("local_date_time_no_seconds", LOCAL_DATE_TIME, b"2007-12-03T10:15", 0x07,
           {"expect_datetime_iso": "2007-12-03T10:15:00"}),
    Vector("local_date_time_nanos", LOCAL_DATE_TIME, b"2007-12-03T10:15:30.123456789", 0x08,
           {"expect_datetime_iso": "2007-12-03T10:15:30.123456"}),
    # Markers this wrapper reads but never writes.
    Vector("integer", INTEGER, struct.pack(">i", 42), 0x0A, {"expect": 42}),
    Vector("float", FLOAT, struct.pack(">f", 1.5), 0x0B, {"expect": 1.5}),
    Vector("date", DATE, _millis("2026-08-11T00:00:00"), 0x0C, {"expect_date_iso": "2026-08-11"}),
    Vector("time", TIME, _millis("1970-01-01T12:30:45.123"), 0x0D,
           {"expect_datetime_iso": "1970-01-01T12:30:45.123000+00:00"}),
    Vector("timestamp", TIMESTAMP, _millis("2026-08-11T12:30:45.123"), 0x0E,
           {"expect_datetime_iso": "2026-08-11T12:30:45.123000+00:00"}),
    Vector("local_date", LOCAL_DATE, b"2007-12-03", 0x0F, {"expect_date_iso": "2007-12-03"}),
    Vector("local_time_no_seconds", LOCAL_TIME, b"10:15", 0x10, {"expect": "10:15"}),
    Vector("generic", GENERIC, b"thing", 0x11, {"expect": "thing"}),
    # The key id is an unsigned 32-bit integer.
    Vector("key_id_high_bit", STRING, b"hi", 0x12, {"expect": "hi"}, 0x80000001),
]


def build() -> Dict[str, Any]:
    vectors = []
    for v in VECTORS:
        iv = bytes([v.iv_byte]) * 12
        body = struct.pack(">I", v.key_id) + bytes([v.marker]) + iv + AESGCM(DATA_KEY).encrypt(iv, v.plaintext, None)
        payload = hmac.new(HMAC_KEY, body, hashlib.sha256).digest() + body
        vector: Dict[str, Any] = {"name": v.name, "marker": v.marker}
        if v.key_id != KEY_ID:
            vector["key_id"] = v.key_id
        vector["ciphertext_b64"] = base64.b64encode(payload).decode("ascii")
        vector.update(v.expected)
        vectors.append(vector)
    return {
        "data_key_b64": base64.b64encode(DATA_KEY).decode("ascii"),
        "hmac_key_b64": base64.b64encode(HMAC_KEY).decode("ascii"),
        "key_id": KEY_ID,
        "vectors": vectors,
    }


def render() -> str:
    return json.dumps(build(), indent=2, ensure_ascii=False) + "\n"


if __name__ == "__main__":
    # Written as UTF-8 bytes, so the non-ASCII vector works on any console.
    sys.stdout.buffer.write(render().encode("utf-8"))
