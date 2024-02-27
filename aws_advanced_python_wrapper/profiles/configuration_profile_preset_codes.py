#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http:#www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

class ConfigurationProfilePresetCodes:
    # Presets family A, B, C - no connection pool
    # Presets family D, E ,F - internal connection pool
    # Presets family G, H, I - external connection pool

    A0 = "A0"  # Normal
    A1 = "A1"  # Easy
    A2 = "A2"  # Aggressive
    B = "B"  # Normal
    PG_C0 = "PG_C0"  # Normal
    PG_C1 = "PG_C1"  # Aggressive
    D0 = "D0"  # Normal
    D1 = "D1"  # Easy
    E = "E"  # Normal
    PG_F0 = "PG_F0"  # Normal
    PG_F1 = "PG_F1"  # Aggressive
    G0 = "G0"  # Normal
    G1 = "G1"  # Easy
    H = "H"  # Normal
    PG_I0 = "PG_I0"  # Normal
    PG_I1 = "PG_I1"  # Aggressive
