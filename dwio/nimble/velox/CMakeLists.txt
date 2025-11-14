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
    SchemaUtils.cpp
    SchemaTypes.cpp
    SchemaReader.cpp
    SchemaBuilder.cpp
    StreamData.cpp
    FieldReader.cpp
    LayoutPlanner.cpp
    BufferGrowthPolicy.cpp
    DeduplicationUtils.cpp
    FieldWriter.cpp
)

target_link_libraries(
  nimble
  PRIVATE velox_type Folly::folly absl::flat_hash_map protobuf::libprotobuf
)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Schema.fbs"
  ""
  nimble_velox_schema_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  ""
)
add_library(nimble_velox_schema_fb INTERFACE)
target_include_directories(
  nimble_velox_schema_fb
  INTERFACE ${FLATBUFFERS_INCLUDE_DIR}
)
add_dependencies(nimble_velox_schema_fb nimble_velox_schema_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Metadata.fbs"
  ""
  nimble_velox_metadata_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  ""
)
add_library(nimble_velox_metadata_fb INTERFACE)
target_include_directories(
  nimble_velox_metadata_fb
  INTERFACE ${FLATBUFFERS_INCLUDE_DIR}
)
add_dependencies(nimble_velox_metadata_fb nimble_velox_metadata_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Stats.fbs"
  ""
  nimble_velox_stats_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  ""
)
add_library(nimble_velox_stats_fb INTERFACE)
target_include_directories(
  nimble_velox_stats_fb
  INTERFACE ${FLATBUFFERS_INCLUDE_DIR}
)
add_dependencies(nimble_velox_stats_fb nimble_velox_stats_schema_fb)

target_link_libraries(
  nimble
  PRIVATE nimble_velox_schema_fb nimble_velox_metadata_fb nimble_velox_stats_fb
)

target_sources(
  nimble
  PRIVATE
    ChunkedStream.cpp
    ChunkedStreamDecoder.cpp
    SchemaSerialization.cpp
    StreamLabels.cpp
    VeloxReader.cpp
    RawSizeUtils.cpp
    DecodedVectorManager.cpp
    EncodingLayoutTree.cpp
    FlushPolicy.cpp
    VeloxWriter.cpp
    ChunkedStreamWriter.cpp
    VeloxWriterDefaultMetadataOSS.cpp
    StreamChunker.cpp
)

add_subdirectory(selective)
add_subdirectory(tests)
