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

"""Byte-format conformance tests for the KMS encryption wire format.

These check that Python matches the payload layout shown below. The
database-side trigger checks its minimum length and its HMAC tag.

The vectors in ``resources/encryption/conformance_vectors.json`` come from
the reference encoder next to them, ``generate_conformance_vectors.py``, which
builds each payload from the spec with raw ``cryptography`` and the stdlib,
without importing the wrapper, so decrypting them tests the code against the
spec. The encrypt->decrypt round trips in test_encryption_service.py can't do
that by themselves, because they would still pass if the whole format were
wrong.

The format under test::

    [ HMAC-SHA256 : 32 ][ key id : 4 ][ marker : 1 ][ IV : 12 ][ ciphertext ][ GCM tag : 16 ]
"""

import base64
import hashlib
import hmac
import importlib.util
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from aws_advanced_python_wrapper.encryption.encryption_service import \
    EncryptionService

_RESOURCES = Path(__file__).parent / "resources" / "encryption"
_VECTORS_PATH = _RESOURCES / "conformance_vectors.json"
_IV_OFFSET = EncryptionService.HMAC_TAG_LENGTH + EncryptionService.KEY_ID_LENGTH + EncryptionService.TYPE_MARKER_LENGTH


def _load():
    data = json.loads(_VECTORS_PATH.read_text(encoding="utf-8"))
    dk = base64.b64decode(data["data_key_b64"])
    hk = base64.b64decode(data["hmac_key_b64"])
    return dk, hk, data["key_id"], data["vectors"]


DK, HK, KEY_ID, VECTORS = _load()
_IDS = [v["name"] for v in VECTORS]


@pytest.mark.parametrize("vec", VECTORS, ids=_IDS)
def test_decrypts_independent_reference_payload(vec):
    """Python decrypts a payload it did not produce to the expected value."""
    ciphertext = base64.b64decode(vec["ciphertext_b64"])
    result = EncryptionService.decrypt(ciphertext, DK, HK)

    if "expect_b64" in vec:
        assert result == base64.b64decode(vec["expect_b64"])
    elif "expect_datetime_iso" in vec:
        # LOCAL_DATE_TIME vectors include the no-seconds and nanosecond forms
        # the reader accepts. Python must read them to the microsecond.
        assert result == datetime.fromisoformat(vec["expect_datetime_iso"])
    elif "expect_date_iso" in vec:
        assert result == date.fromisoformat(vec["expect_date_iso"])
    elif vec["name"] == "decimal":
        assert Decimal(result) == Decimal(vec["expect"])
    else:
        # The type matters too: INTEGER must read as int, not 42.0.
        assert result == vec["expect"]
        assert type(result) is type(vec["expect"])


@pytest.mark.parametrize("vec", VECTORS, ids=_IDS)
def test_reference_payload_carries_expected_key_id(vec):
    ciphertext = base64.b64decode(vec["ciphertext_b64"])
    assert EncryptionService.key_id_from_payload(ciphertext) == vec.get("key_id", KEY_ID)


@pytest.mark.parametrize("vec", VECTORS, ids=_IDS)
def test_our_encrypt_output_passes_the_db_trigger_hmac(vec):
    """Check our output the way the DB-side trigger does: the HMAC over
    everything after the 32-byte tag, computed with the stored HMAC key, must
    match the tag. The trigger would reject any write that fails this.
    """
    # Re-encrypt the decrypted value, then recompute the HMAC.
    value = EncryptionService.decrypt(base64.b64decode(vec["ciphertext_b64"]), DK, HK)
    payload = EncryptionService.encrypt(value, DK, HK, key_id=vec.get("key_id", KEY_ID))
    assert len(payload) >= EncryptionService.MIN_ENCRYPTED_LENGTH
    stored_tag = payload[:EncryptionService.HMAC_TAG_LENGTH]
    body = payload[EncryptionService.HMAC_TAG_LENGTH:]
    recomputed = hmac.new(HK, body, hashlib.sha256).digest()
    assert hmac.compare_digest(stored_tag, recomputed)


def test_vectors_use_distinct_fixed_ivs():
    """Each vector's IV is one repeated byte, unique per vector. The wrapper's
    encrypt draws a random IV, so it could not have produced these payloads."""
    ivs = []
    for vec in VECTORS:
        iv = base64.b64decode(vec["ciphertext_b64"])[_IV_OFFSET:_IV_OFFSET + EncryptionService.GCM_IV_LENGTH]
        assert len(set(iv)) == 1, vec["name"]
        ivs.append(iv)
    assert len(set(ivs)) == len(ivs)


def test_committed_vectors_match_the_reference_encoder():
    """The JSON file is exactly what the reference encoder produces, so the
    vectors can be regenerated and reviewed from source."""
    spec = importlib.util.spec_from_file_location("generate_conformance_vectors",
                                                  _RESOURCES / "generate_conformance_vectors.py")
    assert spec is not None and spec.loader is not None
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)
    assert _VECTORS_PATH.read_text(encoding="utf-8") == generator.render()
