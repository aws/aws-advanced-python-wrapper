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

from typing import Generic, TypeVar, Union, cast

V = TypeVar('V')


class Empty(object):
    """An empty sentinel object used to differentiate between None vs an empty value."""
    pass


class ValueContainer(Generic[V]):
    """A container object which may or may not contain a non-None value."""

    # Sentinel object to represent an empty ValueContainer
    _EMPTY = Empty()

    def __init__(self, value: Union[Empty, V] = _EMPTY):
        self._value = value

    @classmethod
    def of(cls, value: V) -> 'ValueContainer[V]':
        """Returns a ValueContainer with the specified non-None value."""
        if value is None:
            raise ValueError("Value cannot be None")
        return cls(value)

    @classmethod
    def empty(cls) -> 'ValueContainer[V]':
        """Returns an empty ValueContainer instance."""
        return cls()

    def is_present(self) -> bool:
        """Returns true if a value is present."""
        return self._value is not self._EMPTY

    def is_empty(self) -> bool:
        """Returns true if no value is present."""
        return self._value is self._EMPTY

    def get(self) -> V:
        """Returns the value if present, otherwise raises ValueError."""
        if self._value is self._EMPTY:
            raise ValueError("No value present")
        return cast('V', self._value)

    def or_else(self, other: V) -> V:
        """Returns the value if present, otherwise returns other."""
        return cast('V', self._value) if self.is_present() else other

    def __eq__(self, other: object) -> bool:
        """Checks if this ValueContainer is equal to another object."""
        if not isinstance(other, ValueContainer):
            return False
        if self.is_empty() and other.is_empty():
            return True
        if self.is_empty() or other.is_empty():
            return False
        return self._value == other._value

    def __str__(self) -> str:
        """Returns a string representation of this ValueContainer."""
        return "ValueContainer.empty" if self.is_empty() else f"ValueContainer[{self._value}]"
