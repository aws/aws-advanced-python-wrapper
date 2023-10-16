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

from enum import Enum, auto


class HostEvent(Enum):
    URL_CHANGED = auto()
    CONVERTED_TO_WRITER = auto()
    CONVERTED_TO_READER = auto()
    WENT_UP = auto()
    WENT_DOWN = auto()
    HOST_ADDED = auto()
    HOST_DELETED = auto()
    HOST_CHANGED = auto()


class ConnectionEvent(Enum):
    CONNECTION_OBJECT_CHANGED = auto()
    INITIAL_CONNECTION = auto()


class OldConnectionSuggestedAction(Enum):
    NO_OPINION = auto()
    DISPOSE = auto()
    PRESERVE = auto()
