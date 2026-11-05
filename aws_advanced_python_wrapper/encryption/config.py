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

import math
import os
import re
from dataclasses import dataclass, field
from typing import (TYPE_CHECKING, Any, FrozenSet, Iterable, Optional, Type,
                    TypeVar)

from aws_advanced_python_wrapper.errors import EncryptionConfigError
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (WrapperProperties,
                                                          WrapperProperty)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.utils.properties import Properties

# Schema names cannot be bound as statement parameters, so they are
# interpolated into the plugin's own SQL. Only plain identifiers are accepted,
# which rules out quoting, whitespace, comments, and statement separators.
_SCHEMA_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

T = TypeVar("T", int, float)


def validate_schema_name(name: Any) -> str:
    """Return ``name`` if it is a plain SQL identifier, else raise
    :class:`EncryptionConfigError`."""
    if not isinstance(name, str) or not _SCHEMA_NAME_PATTERN.fullmatch(name):
        raise EncryptionConfigError(
            Messages.get_formatted("EncryptionConfig.InvalidSchemaName", repr(name)))
    return name


def _normalize_arns(value: Any) -> FrozenSet[str]:
    """Accept ``None``, a comma-separated string, or an iterable of strings."""
    if value is None:
        return frozenset()
    entries: Iterable[Any] = value.split(",") if isinstance(value, str) else value
    return frozenset(s for s in (str(e).strip() for e in entries) if s)


@dataclass(frozen=True)
class EncryptionConfig:
    """The plugin's configuration, resolved once from the connection
    properties. Build it with :meth:`from_props`; the constructor validates."""

    kms_region: str
    allowed_master_key_arns: FrozenSet[str] = field(default_factory=frozenset)
    kms_endpoint: Optional[str] = None
    metadata_schema: str = "encrypt"
    metadata_cache_enabled: bool = True
    metadata_cache_expiration_sec: float = 3600.0
    metadata_cache_refresh_interval_sec: float = 300.0
    data_key_cache_enabled: bool = True
    data_key_cache_max_size: int = 1000
    data_key_cache_expiration_sec: float = 300.0
    key_management_max_retries: int = 3
    key_management_retry_backoff_base_sec: float = 0.1
    audit_logging_enabled: bool = False
    return_unverified_data: bool = False

    def __post_init__(self) -> None:
        if not self.kms_region or not self.kms_region.strip():
            raise EncryptionConfigError(Messages.get("EncryptionConfig.MissingRegion"))
        # Normalize here too, so a directly constructed config gets the same
        # exact-match semantics as one built from properties.
        object.__setattr__(self, "allowed_master_key_arns", _normalize_arns(self.allowed_master_key_arns))
        validate_schema_name(self.metadata_schema)

        wp = WrapperProperties
        # Durations must be finite: an infinite interval or TTL reaches
        # Event.wait(), which rejects it and kills the background thread, and
        # an infinite expiry or backoff is never what was meant.
        self._require(_finite(self.metadata_cache_expiration_sec) and self.metadata_cache_expiration_sec > 0,
                      wp.ENCRYPTION_METADATA_CACHE_EXPIRATION_SEC, "a finite number > 0")
        self._require(_finite(self.metadata_cache_refresh_interval_sec) and self.metadata_cache_refresh_interval_sec >= 0,
                      wp.ENCRYPTION_METADATA_CACHE_REFRESH_INTERVAL_SEC, "a finite number >= 0")
        self._require(self.data_key_cache_max_size > 0, wp.ENCRYPTION_DATA_KEY_CACHE_MAX_SIZE, "> 0")
        self._require(_finite(self.data_key_cache_expiration_sec) and self.data_key_cache_expiration_sec > 0,
                      wp.ENCRYPTION_DATA_KEY_CACHE_EXPIRATION_SEC, "a finite number > 0")
        self._require(self.key_management_max_retries >= 0, wp.ENCRYPTION_KEY_MANAGEMENT_MAX_RETRIES, ">= 0")
        self._require(_finite(self.key_management_retry_backoff_base_sec)
                      and self.key_management_retry_backoff_base_sec > 0,
                      wp.ENCRYPTION_KEY_MANAGEMENT_RETRY_BACKOFF_BASE_SEC, "a finite number > 0")

    @classmethod
    def from_props(cls, props: Properties, *, require_allowed_master_keys: bool = True) -> EncryptionConfig:
        """Build the configuration from connection properties.

        :param require_allowed_master_keys: the plugin refuses to run without
            ``encryption_allowed_master_key_arns``. Only administrative setup
            code that builds a config for :class:`KeyManagementUtility` should
            pass ``False``, so a new master key can be created and used in the
            same setup script.
        :raises EncryptionConfigError: if a value is missing or out of range.
        """
        wp = WrapperProperties
        # A blank value at one level falls through to the next.
        region = (_stripped(wp.ENCRYPTION_KMS_REGION.get(props))
                  or _stripped(os.environ.get("AWS_REGION"))
                  or _stripped(os.environ.get("AWS_DEFAULT_REGION")))
        config = cls(
            kms_region=region,
            kms_endpoint=wp.ENCRYPTION_KMS_ENDPOINT.get(props),
            # Read directly: the value may be a list, which WrapperProperty.get
            # (typed Optional[str]) does not model.
            allowed_master_key_arns=_normalize_arns(props.get(wp.ENCRYPTION_ALLOWED_MASTER_KEY_ARNS.name)),
            metadata_schema=wp.ENCRYPTION_METADATA_SCHEMA.get(props) or "",
            metadata_cache_enabled=wp.ENCRYPTION_METADATA_CACHE_ENABLED.get_bool(props),
            metadata_cache_expiration_sec=_number(wp.ENCRYPTION_METADATA_CACHE_EXPIRATION_SEC, props, float),
            metadata_cache_refresh_interval_sec=_number(wp.ENCRYPTION_METADATA_CACHE_REFRESH_INTERVAL_SEC, props, float),
            data_key_cache_enabled=wp.ENCRYPTION_DATA_KEY_CACHE_ENABLED.get_bool(props),
            data_key_cache_max_size=_number(wp.ENCRYPTION_DATA_KEY_CACHE_MAX_SIZE, props, int),
            data_key_cache_expiration_sec=_number(wp.ENCRYPTION_DATA_KEY_CACHE_EXPIRATION_SEC, props, float),
            key_management_max_retries=_number(wp.ENCRYPTION_KEY_MANAGEMENT_MAX_RETRIES, props, int),
            key_management_retry_backoff_base_sec=_number(
                wp.ENCRYPTION_KEY_MANAGEMENT_RETRY_BACKOFF_BASE_SEC, props, float),
            audit_logging_enabled=wp.ENCRYPTION_AUDIT_LOGGING_ENABLED.get_bool(props),
            return_unverified_data=wp.ENCRYPTION_RETURN_UNVERIFIED_DATA.get_bool(props),
        )

        if require_allowed_master_keys and not config.restricts_master_keys:
            raise EncryptionConfigError(
                Messages.get_formatted("EncryptionConfig.MissingAllowedMasterKeys",
                                       wp.ENCRYPTION_ALLOWED_MASTER_KEY_ARNS.name))
        return config

    @property
    def restricts_master_keys(self) -> bool:
        """``True`` when only the listed master keys may be used."""
        return bool(self.allowed_master_key_arns)

    @property
    def background_refresh_enabled(self) -> bool:
        return self.metadata_cache_enabled and self.metadata_cache_refresh_interval_sec > 0

    def is_master_key_allowed(self, master_key_arn: Optional[str]) -> bool:
        """Exact-match check against the allow-list. With no allow-list
        configured (administrative use only) every key is allowed."""
        if not self.restricts_master_keys:
            return True
        return master_key_arn is not None and master_key_arn in self.allowed_master_key_arns

    @staticmethod
    def _require(condition: bool, prop: WrapperProperty, requirement: str) -> None:
        if not condition:
            raise EncryptionConfigError(Messages.get_formatted("EncryptionConfig.OutOfRange", prop.name, requirement))


def _stripped(value: Optional[str]) -> str:
    return (value or "").strip()


def _finite(value: float) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def _number(prop: WrapperProperty, props: Properties, kind: Type[T]) -> T:
    """Read a numeric property, naming it if the value is not a number.
    Property values are configuration, not secrets, so chaining is fine."""
    try:
        return prop.get_type(props, kind)
    except (TypeError, ValueError) as e:
        raise EncryptionConfigError(Messages.get_formatted("EncryptionConfig.InvalidValue", prop.name, e)) from e
