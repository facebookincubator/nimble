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

add_library(nimble_velox_config Config.cpp)
target_link_libraries(nimble_velox_config nimble_encodings nimble_common
                      Folly::folly)

add_library(nimble_velox_field_reader FieldReader.cpp)
target_link_libraries(nimble_velox_field_reader nimble_velox_schema_reader
                      nimble_common Folly::folly)

add_library(nimble_velox_flatmap_layout_planner FlatMapLayoutPlanner.cpp)
target_link_libraries(nimble_velox_flatmap_layout_planner
                      nimble_velox_schema_reader nimble_tablet velox_file)

add_library(nimble_velox_field_writer BufferGrowthPolicy.cpp FieldWriter.cpp)
target_link_libraries(nimble_velox_field_writer nimble_velox_schema
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

add_library(nimble_velox_schema_serialization SchemaSerialization.cpp)
target_link_libraries(
  nimble_velox_schema_serialization nimble_velox_schema_reader
  nimble_velox_schema_builder nimble_velox_schema_fb)

add_library(nimble_velox_deserializer Deserializer.cpp)
target_link_libraries(
  nimble_velox_deserializer
  nimble_common
  nimble_velox_common
  nimble_velox_field_reader
  velox_vector
  velox_memory
  velox_dwio_common)

add_library(nimble_velox_serializer Serializer.cpp)
target_link_libraries(nimble_velox_serializer nimble_common
                      nimble_velox_schema_builder velox_vector)

add_library(nimble_velox_reader StreamInputDecoder.cpp StreamLabels.cpp
                               VeloxReader.cpp)
target_link_libraries(
  nimble_velox_reader
  nimble_velox_schema
  nimble_velox_schema_serialization
  nimble_velox_schema_fb
  nimble_velox_metadata_fb
  nimble_common
  Folly::folly)

add_library(
  nimble_velox_writer EncodingLayoutTree.cpp FlushPolicy.cpp VeloxWriter.cpp
                     VeloxWriterDefaultMetadataOSS.cpp)
target_link_libraries(nimble_velox_writer nimble_encodings nimble_common
                      nimble_velox_metadata_fb Folly::folly)
