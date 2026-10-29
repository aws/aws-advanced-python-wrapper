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

from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from aws_advanced_python_wrapper.encryption.type_marker import TypeMarker


class TestFromObject:
    @pytest.mark.parametrize("value,expected", [
        ("s", TypeMarker.STRING),
        (b"x", TypeMarker.BYTE_ARRAY),
        (bytearray(b"x"), TypeMarker.BYTE_ARRAY),
        (memoryview(b"x"), TypeMarker.BYTE_ARRAY),
        (True, TypeMarker.BOOLEAN),      # must win over int
        (False, TypeMarker.BOOLEAN),
        (5, TypeMarker.LONG),            # every Python int but bool -> LONG
        (5.0, TypeMarker.DOUBLE),
        (Decimal("1.5"), TypeMarker.BIG_DECIMAL),
        (datetime(2020, 1, 1), TypeMarker.LOCAL_DATE_TIME),  # must win over date
        (datetime(2020, 1, 1, tzinfo=timezone.utc), TypeMarker.TIMESTAMP),  # aware: an instant
        (date(2020, 1, 1), TypeMarker.LOCAL_DATE),
        (time(1, 2, 3), TypeMarker.LOCAL_TIME),
        (object(), TypeMarker.GENERIC),
    ])
    def test_from_object(self, value, expected):
        assert TypeMarker.from_object(value) == expected


class TestFromValue:
    def test_known(self):
        assert TypeMarker.from_value(TypeMarker.STRING) == TypeMarker.STRING

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            TypeMarker.from_value(200)


class TestNames:
    def test_name_for_known(self):
        assert TypeMarker.name_for(TypeMarker.LONG) == "LONG"

    def test_name_for_unknown(self):
        assert TypeMarker.name_for(200) is None

    def test_is_known(self):
        assert TypeMarker.is_known(TypeMarker.GENERIC)
        assert not TypeMarker.is_known(200)


def test_marker_numbers_never_change():
    # The marker numbers are stored in every payload, so they can't change
    # without breaking values already written.
    assert (TypeMarker.STRING, TypeMarker.INTEGER, TypeMarker.LONG, TypeMarker.DOUBLE,
            TypeMarker.FLOAT, TypeMarker.BOOLEAN, TypeMarker.BIG_DECIMAL, TypeMarker.DATE,
            TypeMarker.TIME, TypeMarker.TIMESTAMP, TypeMarker.LOCAL_DATE, TypeMarker.LOCAL_TIME,
            TypeMarker.LOCAL_DATE_TIME, TypeMarker.BYTE_ARRAY, TypeMarker.GENERIC) == \
           (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 99)
