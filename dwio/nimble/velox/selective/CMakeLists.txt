# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
target_sources(
  nimble
  PRIVATE
    ChunkedDecoder.cpp
    ColumnReader.cpp
    FlatMapColumnReader.cpp
    IntegerColumnReader.cpp
    NimbleData.cpp
    ReaderBase.cpp
    RowSizeTracker.cpp
    SelectiveNimbleReader.cpp
    SelectiveNimbleReaderInitHookOSS.cpp
    StringColumnReader.cpp
    StructColumnReader.cpp
    StructColumnReader.cpp
    VariableLengthColumnReader.cpp
)

add_subdirectory(tests)
