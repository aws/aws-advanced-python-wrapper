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

import csv
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List


@dataclass
class PerfStatBase(ABC):

    @abstractmethod
    def write_data(self, writer):
        pass


class PerformanceUtil:
    @staticmethod
    def write_perf_data_to_file(file_name: str, header: List, data: List[PerfStatBase]):
        with open(f"{file_name}", "w", encoding="UTF8") as f:
            writer = csv.writer(f)
            writer.writerow(header)

            for row in data:
                row.write_data(writer)

    @staticmethod
    def to_millis(nanos: int):
        return nanos / 1_000_000
