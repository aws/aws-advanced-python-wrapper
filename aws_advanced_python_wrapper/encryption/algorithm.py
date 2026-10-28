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

from typing import Dict, Optional, Tuple

from aws_advanced_python_wrapper.errors import InvalidAlgorithmError


class EncryptionAlgorithm:
    """The symmetric algorithms the plugin can use to encrypt column values.

    These names are stored in the ``encryption_metadata.encryption_algorithm``
    column, which is part of the metadata schema the shared specification
    defines.
    """

    AES_256_GCM: str = "AES-256-GCM"
    AES_128_GCM: str = "AES-128-GCM"
    DEFAULT: str = AES_256_GCM

    # Expected data key length in bytes for each algorithm.
    _KEY_LENGTHS: Dict[str, int] = {
        AES_256_GCM: 32,
        AES_128_GCM: 16,
    }

    ALL: Tuple[str, ...] = tuple(_KEY_LENGTHS.keys())

    @classmethod
    def data_key_spec(cls, name: str) -> str:
        """Return the KMS ``GenerateDataKey`` key spec (``AES_256`` /
        ``AES_128``) that yields a data key of the right length for ``name``.
        This is also the value stored in ``key_storage.key_spec``."""
        return f"AES_{cls.key_length(name) * 8}"

    @classmethod
    def key_length(cls, name: str) -> int:
        """Return the required data key length in bytes for ``name``."""
        try:
            return cls._KEY_LENGTHS[name]
        except KeyError:
            # ``from None`` drops the KeyError from the dict lookup;
            # unsupported() already explains the failure.
            raise cls.unsupported(name) from None

    @classmethod
    def is_supported(cls, name: Optional[str]) -> bool:
        return name in cls._KEY_LENGTHS

    @classmethod
    def unsupported(cls, name: Optional[str]) -> InvalidAlgorithmError:
        return InvalidAlgorithmError(
            f"Unsupported kms_encryption algorithm: {name!r}. "
            f"Supported algorithms: {', '.join(cls.ALL)}",
            algorithm=name,
        )
