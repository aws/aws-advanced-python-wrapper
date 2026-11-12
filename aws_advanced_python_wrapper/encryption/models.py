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

"""Value objects for rows of the ``key_storage`` and ``encryption_metadata``
tables. Their ``repr`` never shows key material, so they are safe to log."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Optional, cast

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm

if TYPE_CHECKING:
    from datetime import datetime

REDACTED = "[REDACTED]"
DEFAULT_KEY_SPEC = "AES_256"

# An ARN as AWS quotes it in an error message, in any partition. It must name
# an account, so text that is already masked doesn't match. Punctuation that
# ends a sentence or a list is left out of the match; inside the ARN (a role
# session name can hold '.' and ',') it is kept.
_ARN_IN_TEXT = re.compile(
    r"arn:(?P<partition>aws(?:-[a-z]+)*):(?P<service>[a-z0-9-]+):[a-z0-9-]*:\d{12}:"
    r"(?P<resource>[^\s'\"]*[^\s'\".,;:!?)\]])")


def mask_key_id(value: Optional[str]) -> Optional[str]:
    """Keep the first and last four characters of an identifier; one of 8
    characters or fewer becomes ``***``."""
    if value is None:
        return None
    return f"{value[:4]}***{value[-4:]}" if len(value) > 8 else "***"


def mask_arn(value: Optional[str]) -> Optional[str]:
    """Mask the region and account of a KMS key ARN, and the middle of its key
    id, for the wrapper's own log and error messages. ARNs are identifiers,
    not secrets: the messages that quote a KMS error mask the ARNs in its text
    with :func:`mask_arns_in_text`, but the KMS error itself, kept as the
    ``__cause__``, still shows them."""
    if value is None:
        return None
    _, sep, key = value.rpartition("/")
    return f"arn:aws:kms:***:***:key/{mask_key_id(key) if sep and key else '***'}"


def mask_arns_in_text(text: str) -> str:
    """Mask the ARNs in ``text`` that name an account, such as those in a
    failed KMS call's error message: the master key and, for an access denial,
    the caller's role session. A KMS key ARN is masked as :func:`mask_arn`
    does; any other ARN keeps only its partition and service. Text without
    such an ARN comes back unchanged."""
    return _ARN_IN_TEXT.sub(_mask_matched_arn, text)


def _mask_matched_arn(match: re.Match[str]) -> str:
    if match["service"] == "kms" and match["resource"].startswith("key/"):
        return cast("str", mask_arn(match[0]))
    return f"arn:{match['partition']}:{match['service']}:***:***:***"


def _blank(value: Optional[str]) -> bool:
    return value is None or not value.strip()


@dataclass(frozen=True)
class KeyMetadata:
    """One ``key_storage`` row.

    ``id`` is the integer surrogate key, which encrypted payloads record and
    ``encryption_metadata.key_id`` references. ``key_id`` is a separate UUID
    string. ``encrypted_data_key`` is the base64 text of the KMS ciphertext
    blob; ``hmac_key`` is raw bytes. The plaintext data key is never held here.
    """

    id: Optional[int] = None
    key_id: Optional[str] = None
    key_name: Optional[str] = None
    master_key_arn: Optional[str] = None
    encrypted_data_key: Optional[str] = field(default=None, repr=False)
    hmac_key: Optional[bytes] = field(default=None, repr=False)
    key_spec: str = DEFAULT_KEY_SPEC
    created_at: Optional[datetime] = None
    last_used_at: Optional[datetime] = None

    @property
    def is_usable(self) -> bool:
        """``True`` once the row names a master key and carries an encrypted
        data key, an HMAC key, and a key spec."""
        return (not _blank(self.master_key_arn) and not _blank(self.encrypted_data_key)
                and bool(self.hmac_key) and not _blank(self.key_spec))

    @classmethod
    def from_row(cls, row: Mapping[str, Any], prefix: str = "") -> KeyMetadata:
        """Build from a ``key_storage`` row whose columns may carry ``prefix``."""
        hmac_key = row.get(f"{prefix}hmac_key")
        return cls(
            id=None if row.get(f"{prefix}id") is None else int(row[f"{prefix}id"]),
            key_id=row.get(f"{prefix}key_id"),
            key_name=row.get(f"{prefix}name"),
            master_key_arn=row.get(f"{prefix}master_key_arn"),
            encrypted_data_key=row.get(f"{prefix}encrypted_data_key"),
            hmac_key=None if hmac_key is None else bytes(hmac_key),
            key_spec=row.get(f"{prefix}key_spec") or DEFAULT_KEY_SPEC,
            created_at=row.get(f"{prefix}created_at"),
            last_used_at=row.get(f"{prefix}last_used_at"),
        )

    def __repr__(self) -> str:
        return (f"KeyMetadata(id={self.id!r}, key_id={self.key_id!r}, key_name={self.key_name!r}, "
                f"master_key_arn={mask_arn(self.master_key_arn)!r}, encrypted_data_key={REDACTED!r}, "
                f"hmac_key={None if self.hmac_key is None else REDACTED!r}, key_spec={self.key_spec!r}, "
                f"created_at={self.created_at!r}, last_used_at={self.last_used_at!r})")

    __str__ = __repr__


@dataclass(frozen=True)
class ColumnEncryptionConfig:
    """One ``encryption_metadata`` row joined to the key it references.

    ``key_metadata`` is ``None`` when the referenced ``key_storage`` row is
    missing. The column still counts as encrypted, so a write fails closed
    instead of storing plaintext.
    """

    table_name: str
    column_name: str
    algorithm: str = EncryptionAlgorithm.DEFAULT
    key_id: Optional[int] = None
    key_metadata: Optional[KeyMetadata] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def column_identifier(self) -> str:
        return f"{self.table_name}.{self.column_name}"

    @property
    def is_usable(self) -> bool:
        """``True`` when the algorithm is supported and the key row is usable."""
        return (EncryptionAlgorithm.is_supported(self.algorithm)
                and self.key_metadata is not None and self.key_metadata.is_usable)
