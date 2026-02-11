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

from types import MappingProxyType
from typing import (TYPE_CHECKING, Any, ClassVar, Optional, Tuple, Type,
                    TypeAlias, TypeVar)

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.hostinfo import HostInfo

from aws_advanced_python_wrapper.utils.cache_map import CacheMap

V = TypeVar('V')
Topology: TypeAlias = Tuple["HostInfo", ...]


class StorageService:
    _storage_map: ClassVar[MappingProxyType] = MappingProxyType({
        Topology: CacheMap()
    })

    @staticmethod
    def get(item_class: Type[V], key: Any) -> Optional[V]:
        cache = StorageService._storage_map.get(item_class)
        if cache is None:
            return None

        value = cache.get(key)
        # TODO: publish data access event
        return value

    @staticmethod
    def get_all(item_class: Type[V]) -> Optional[CacheMap[Any, V]]:
        cache = StorageService._storage_map.get(item_class)
        return cache

    @staticmethod
    def set(key: Any, item: V, item_class: Type[V]) -> None:
        cache = StorageService._storage_map.get(item_class)
        if cache is not None:
            cache.put(key, item)

    @staticmethod
    def remove(item_class: Type, key: Any) -> None:
        cache = StorageService._storage_map.get(item_class)
        if cache is not None:
            cache.remove(key)

    @staticmethod
    def clear(item_class: Type) -> None:
        cache = StorageService._storage_map.get(item_class)
        if cache is not None:
            cache.clear()

    @staticmethod
    def clear_all() -> None:
        for cache in StorageService._storage_map.values():
            cache.clear()
