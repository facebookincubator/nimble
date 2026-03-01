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
add_executable(
  nimble_serializer_tests
  OptionsTest.cpp
  ProjectorTest.cpp
  SerializationTest.cpp
  SerializerImplTest.cpp
)
add_test(nimble_serializer_tests nimble_serializer_tests)

target_link_libraries(
  nimble_serializer_tests
  nimble_serializer
  nimble_deserializer
  nimble_serializer_impl
  nimble_serializer_options
  nimble_projector
  nimble_common
  nimble_velox_common
  nimble_velox_schema_builder
  nimble_velox_writer
  velox_type
  velox_vector
  velox_vector_fuzzer
  gtest
  gtest_main
  Folly::folly
)
