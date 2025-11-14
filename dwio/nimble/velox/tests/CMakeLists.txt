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
target_sources(nimble_test PRIVATE SchemaUtils.cpp)

add_executable(
  nimble_velox_tests
  BufferGrowthPolicyTest.cpp
  EncodingLayoutTreeTests.cpp
  LayoutPlannerTests.cpp
  OrderedRangesTests.cpp
  SchemaTests.cpp
  TypeTests.cpp
  VeloxReaderTests.cpp
  VeloxWriterTests.cpp
  FieldWriterStatsTests.cpp
)
add_test(nimble_velox_tests nimble_velox_tests)

target_link_libraries(
  nimble_velox_tests
  PRIVATE nimble_test velox_vector velox_vector_fuzzer velox_vector_test_lib
)

add_executable(raw_size_tests RawSizeTests.cpp)
add_test(raw_size_tests raw_size_tests)

target_link_libraries(
  raw_size_tests
  PRIVATE nimble_test velox_vector velox_vector_test_lib
)

add_executable(raw_size_benchmark RawSizeBenchmark.cpp)

target_link_libraries(raw_size_benchmark PRIVATE nimble Folly::follybenchmark)
