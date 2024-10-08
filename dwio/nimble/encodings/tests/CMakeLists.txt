# Copyright (c) Meta Platforms, Inc. and its affiliates.
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
add_library(nimble_encodings_tests_utils TestUtils.cpp)
target_link_libraries(nimble_encodings_tests_utils nimble_encodings)

add_executable(
  nimble_encodings_tests
  ConstantEncodingTests.cpp
  EncodingLayoutTests.cpp
  EncodingSelectionTests.cpp
  EncodingTestsNew.cpp
  MainlyConstantEncodingTests.cpp
  NullableEncodingTests.cpp
  RleEncodingTests.cpp
  SentinelEncodingTests.cpp
  StatisticsTests.cpp)

add_test(nimble_encodings_tests nimble_encodings_tests)

target_link_libraries(
  nimble_encodings_tests
  nimble_encodings_tests_utils
  nimble_encodings
  nimble_common
  nimble_tools_common
  gtest
  gtest_main
  Folly::folly)
