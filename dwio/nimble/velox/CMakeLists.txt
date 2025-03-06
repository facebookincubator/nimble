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
add_subdirectory(tests)

add_library(nimble_velox_common SchemaUtils.cpp)
target_link_libraries(nimble_velox_common nimble_common velox_type)

add_library(nimble_velox_schema SchemaTypes.cpp)
target_link_libraries(nimble_velox_schema nimble_common Folly::folly)

add_library(nimble_velox_schema_reader SchemaReader.cpp)
target_link_libraries(nimble_velox_schema_reader nimble_velox_schema
                      nimble_common Folly::folly)

add_library(nimble_velox_schema_builder SchemaBuilder.cpp)
target_link_libraries(nimble_velox_schema_builder nimble_velox_schema_reader
                      nimble_velox_schema nimble_common Folly::folly)

add_library(nimble_velox_stream_data StreamData.cpp)
target_link_libraries(nimble_velox_stream_data nimble_velox_schema_builder
                      nimble_common)

add_library(nimble_velox_field_reader FieldReader.cpp)
target_link_libraries(
  nimble_velox_field_reader nimble_velox_schema_reader nimble_common
  Folly::folly absl::flat_hash_map protobuf::libprotobuf)

add_library(nimble_velox_layout_planner LayoutPlanner.cpp)
target_link_libraries(nimble_velox_layout_planner nimble_velox_schema_reader
                      velox_file)

add_library(nimble_velox_field_writer BufferGrowthPolicy.cpp
                                      DeduplicationUtils.cpp FieldWriter.cpp)
target_link_libraries(
  nimble_velox_field_writer nimble_velox_schema nimble_velox_stream_data
  nimble_velox_schema_builder Folly::folly)

# Nimble code expects an upper case suffix to the generated file.
list(PREPEND FLATBUFFERS_FLATC_SCHEMA_EXTRA_ARGS "--filename-suffix"
     "Generated")

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Schema.fbs"
  ""
  nimble_velox_schema_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(nimble_velox_schema_fb INTERFACE)
target_include_directories(nimble_velox_schema_fb
                           INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(nimble_velox_schema_fb nimble_velox_schema_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Metadata.fbs"
  ""
  nimble_velox_metadata_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(nimble_velox_metadata_fb INTERFACE)
target_include_directories(nimble_velox_metadata_fb
                           INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(nimble_velox_metadata_fb nimble_velox_metadata_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Stats.fbs"
  ""
  nimble_velox_stats_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(nimble_velox_stats_fb INTERFACE)
target_include_directories(nimble_velox_stats_fb
                           INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(nimble_velox_stats_fb nimble_velox_stats_schema_fb)

add_library(nimble_velox_schema_serialization SchemaSerialization.cpp)
target_link_libraries(
  nimble_velox_schema_serialization nimble_velox_schema_reader
  nimble_velox_schema_builder nimble_velox_schema_fb)

add_library(nimble_velox_reader ChunkedStream.cpp ChunkedStreamDecoder.cpp
                                StreamLabels.cpp VeloxReader.cpp)
target_link_libraries(
  nimble_velox_reader
  nimble_velox_schema
  nimble_velox_schema_serialization
  nimble_velox_schema_fb
  nimble_velox_metadata_fb
  nimble_velox_field_reader
  nimble_tablet_reader
  nimble_common
  Folly::folly)

add_library(
  nimble_velox_writer EncodingLayoutTree.cpp FlushPolicy.cpp VeloxWriter.cpp
                      ChunkedStreamWriter.cpp VeloxWriterDefaultMetadataOSS.cpp)
target_link_libraries(
  nimble_velox_writer
  nimble_encodings
  nimble_common
  nimble_tablet_writer
  nimble_velox_metadata_fb
  nimble_velox_stats_fb
  velox_dwio_common
  Folly::folly)

add_library(raw_size_utils RawSizeUtils.cpp)
target_link_libraries(raw_size_utils velox_vector)
