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

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Footer.fbs"
  ""
  nimble_footer_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  ""
)
add_library(nimble_footer_fb INTERFACE)
# build_flatbuffers() assumes that we include generated .h files by
# #include "FooterGenerated.h" but we want to use #include
# "dwio/nimble/tablet/FooterGenerated.h". So we need
# ${PROJECT_BINARY_DIR} here.
target_include_directories(
  nimble_footer_fb
  INTERFACE ${PROJECT_BINARY_DIR} ${FLATBUFFERS_INCLUDE_DIR}
)
add_dependencies(nimble_footer_fb nimble_footer_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Index.fbs"
  ""
  nimble_index_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  ""
)
add_library(nimble_index_fb INTERFACE)
target_include_directories(
  nimble_index_fb
  INTERFACE ${PROJECT_BINARY_DIR} ${FLATBUFFERS_INCLUDE_DIR}
)
add_dependencies(nimble_index_fb nimble_index_schema_fb)

add_library(nimble_tablet_common Compression.cpp MetadataBuffer.cpp)
target_link_libraries(
  nimble_tablet_common
  nimble_footer_fb
  nimble_common
  Folly::folly
)

add_library(nimble_tablet_reader FileLayout.cpp TabletReader.cpp)
target_link_libraries(nimble_tablet_reader nimble_tablet_common)

add_library(nimble_tablet_writer TabletIndexWriter.cpp TabletWriter.cpp)
target_link_libraries(nimble_tablet_writer nimble_tablet_common nimble_index_fb)

add_subdirectory(tests)
