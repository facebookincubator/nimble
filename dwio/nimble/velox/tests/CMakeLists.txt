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
add_library(nimble_velox_schema_utils SchemaUtils.cpp)
target_link_libraries(
  nimble_velox_schema_utils
  nimble_velox_schema_fb
  nimble_velox_schema_builder
  nimble_velox_schema_reader
  gtest
  gtest_main
)

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
  nimble_velox_common
  nimble_velox_schema_utils
  nimble_velox_reader
  nimble_velox_writer
  nimble_velox_layout_planner
  nimble_velox_stats_fb
  nimble_common_file_writer
  nimble_common
  nimble_encodings
  nimble_encodings_tests_utils
  velox_vector
  velox_vector_fuzzer
  velox_vector_test_lib
  gmock
  gtest
  gtest_main
  Folly::folly
  raw_size_utils
  nimble_column_stats_utils
)

add_executable(raw_size_tests RawSizeTests.cpp)
add_test(raw_size_tests raw_size_tests)

target_link_libraries(
  raw_size_tests
  raw_size_utils
  velox_vector
  velox_vector_test_lib
  gtest
  gtest_main
  Folly::folly
)

add_executable(nimble_index_writer_test IndexWriterTest.cpp)
add_test(nimble_index_writer_test nimble_index_writer_test)

target_link_libraries(
  nimble_index_writer_test
  nimble_velox_index_writer
  nimble_common
  nimble_common_tests_utils
  nimble_index
  velox_vector
  velox_vector_fuzzer
  velox_vector_test_lib
  gmock
  gtest
  gtest_main
  Folly::folly
)

add_executable(nimble_stream_data_tests StreamDataTest.cpp)
add_test(nimble_stream_data_tests nimble_stream_data_tests)

target_link_libraries(
  nimble_stream_data_tests
  nimble_velox_stream_data
  nimble_velox_schema_builder
  nimble_common
  velox_memory
  gmock
  gtest
  gtest_main
)

add_executable(nimble_stream_chunker_tests StreamChunkerTests.cpp)
add_test(nimble_stream_chunker_tests nimble_stream_chunker_tests)

target_link_libraries(
  nimble_stream_chunker_tests
  nimble_velox_stream_chunker
  nimble_velox_stream_data
  nimble_velox_schema_builder
  nimble_velox_chunked_stream_writer
  nimble_common
  velox_memory
  gmock
  gtest
  gtest_main
)

add_executable(raw_size_benchmark RawSizeBenchmark.cpp)

target_link_libraries(
  raw_size_benchmark
  raw_size_utils
  Folly::folly
  Folly::follybenchmark
)
