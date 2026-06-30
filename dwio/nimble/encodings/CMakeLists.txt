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
add_subdirectory(benchmarks)
add_subdirectory(tests)
add_subdirectory(legacy)

add_library(
  nimble_encodings
  ConstantEncoding.cpp
  FsstEncoding.cpp
  MainlyConstantEncoding.cpp
  PrefixEncoding.cpp
  RLEEncoding.cpp
  SparseBoolEncoding.cpp
  TrivialEncoding.cpp
  common/Encoding.cpp
  common/EncodingFactory.cpp
  common/EncodingLayout.cpp
  selection/EncodingSelectionPolicy.cpp
  selection/Statistics.cpp
)

set(FSST_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../third-party/fsst")
if(NOT TARGET fsst AND EXISTS "${FSST_SOURCE_DIR}/CMakeLists.txt")
  add_subdirectory("${FSST_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/fsst")
endif()

target_include_directories(nimble_encodings PUBLIC "${FSST_SOURCE_DIR}")

target_link_libraries(
  nimble_encodings
  nimble_compression
  nimble_common
  velox_common_base
  velox_fastpforlib
  fsst
  Folly::folly
  absl::flat_hash_map
  protobuf::libprotobuf
)
