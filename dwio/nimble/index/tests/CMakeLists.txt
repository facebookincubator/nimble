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
add_library(nimble_index_test_utils TabletIndexTestUtils.cpp)
target_link_libraries(
  nimble_index_test_utils
  nimble_index
  nimble_common
  velox_memory
  Folly::folly
)

add_executable(
  nimble_index_tests
  IndexKeyReaderTest.cpp
  StripeIndexGroupTest.cpp
  TabletIndexTest.cpp
  TabletIndexTestBase.cpp
)

add_test(nimble_index_tests nimble_index_tests)

target_link_libraries(
  nimble_index_tests
  nimble_index
  nimble_velox_writer
  velox_core
  velox_vector
  velox_vector_test_lib
  velox_memory
  velox_dwio_common
  gmock
  gtest
  gtest_main
  Folly::folly
)
