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

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, Optional


class TypeMarker:
    """The single byte written ahead of the ciphertext recording how the
    plaintext was serialized, so a decrypted payload can be turned back into a
    Python value.

    The marker numbers are part of the shared on-the-wire format, so payloads
    are portable across the AWS Advanced Wrappers. Python has a single unbounded
    ``int`` type, so integers are always written as :data:`LONG` (8 bytes,
    big-endian); :data:`INTEGER` (4 bytes) is still understood on read for
    payloads other wrappers produced.
    """

    STRING = 1
    INTEGER = 2
    LONG = 3
    DOUBLE = 4
    FLOAT = 5
    BOOLEAN = 6
    BIG_DECIMAL = 7
    DATE = 8
    TIME = 9
    TIMESTAMP = 10
    LOCAL_DATE = 11
    LOCAL_TIME = 12
    LOCAL_DATE_TIME = 13
    BYTE_ARRAY = 14
    GENERIC = 99

    _NAMES: Dict[int, str] = {
        STRING: "STRING",
        INTEGER: "INTEGER",
        LONG: "LONG",
        DOUBLE: "DOUBLE",
        FLOAT: "FLOAT",
        BOOLEAN: "BOOLEAN",
        BIG_DECIMAL: "BIG_DECIMAL",
        DATE: "DATE",
        TIME: "TIME",
        TIMESTAMP: "TIMESTAMP",
        LOCAL_DATE: "LOCAL_DATE",
        LOCAL_TIME: "LOCAL_TIME",
        LOCAL_DATE_TIME: "LOCAL_DATE_TIME",
        BYTE_ARRAY: "BYTE_ARRAY",
        GENERIC: "GENERIC",
    }

    @classmethod
    def from_value(cls, value: int) -> int:
        """Validate a marker byte read from a payload and return it unchanged."""
        if value not in cls._NAMES:
            raise ValueError(f"Unknown type marker: {value!r}")
        return value

    @classmethod
    def from_object(cls, value: Any) -> int:
        """Pick the marker used to serialize a Python value.

        Unknown types fall back to :data:`GENERIC`, serialized through ``str``.
        Order matters: ``bool`` is a subclass of ``int`` and ``datetime`` a
        subclass of ``date``, so the narrower types are checked first.
        """
        if isinstance(value, (bytes, bytearray, memoryview)):
            return cls.BYTE_ARRAY
        if isinstance(value, str):
            return cls.STRING
        if isinstance(value, bool):
            return cls.BOOLEAN
        if isinstance(value, int):
            return cls.LONG
        if isinstance(value, float):
            return cls.DOUBLE
        if isinstance(value, Decimal):
            return cls.BIG_DECIMAL
        if isinstance(value, datetime):
            return cls.LOCAL_DATE_TIME
        if isinstance(value, date):
            return cls.LOCAL_DATE
        if isinstance(value, time):
            return cls.LOCAL_TIME
        return cls.GENERIC

    @classmethod
    def name_for(cls, marker: int) -> Optional[str]:
        return cls._NAMES.get(marker)

    @classmethod
    def is_known(cls, marker: int) -> bool:
        return marker in cls._NAMES
