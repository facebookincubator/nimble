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
add_executable(nimble_index_projector_test NimbleIndexProjectorTest.cpp)
add_test(nimble_index_projector_test nimble_index_projector_test)

target_link_libraries(
  nimble_index_projector_test
  nimble_index_projector
  nimble_deserializer
  nimble_velox_common
  nimble_velox_writer
  nimble_velox_selective
  velox_dwio_common
  velox_key_encoder
  velox_memory
  velox_vector_test_lib
  gtest
  gtest_main
)
