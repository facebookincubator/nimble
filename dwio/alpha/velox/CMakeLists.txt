add_subdirectory(tests)

add_library(alpha_velox_common SchemaUtils.cpp)
target_link_libraries(alpha_velox_common alpha_common velox_type)

add_library(alpha_velox_schema SchemaTypes.cpp)
target_link_libraries(alpha_velox_schema alpha_common Folly::folly)

add_library(alpha_velox_schema_reader SchemaReader.cpp)
target_link_libraries(alpha_velox_schema_reader alpha_velox_schema alpha_common
                      Folly::folly)

add_library(alpha_velox_schema_builder SchemaBuilder.cpp)
target_link_libraries(alpha_velox_schema_builder alpha_velox_schema_reader
                      alpha_velox_schema alpha_common Folly::folly)

add_library(alpha_velox_config Config.cpp)
target_link_libraries(alpha_velox_config alpha_encodings alpha_common
                      Folly::folly)

add_library(alpha_velox_field_reader FieldReader.cpp)
target_link_libraries(alpha_velox_field_reader alpha_velox_schema_reader
                      alpha_common Folly::folly)

add_library(alpha_velox_flatmap_layout_planner FlatMapLayoutPlanner.cpp)
target_link_libraries(alpha_velox_flatmap_layout_planner
                      alpha_velox_schema_reader alpha_tablet velox_file)

add_library(alpha_velox_field_writer BufferGrowthPolicy.cpp FieldWriter.cpp)
target_link_libraries(alpha_velox_field_writer alpha_velox_schema
                      alpha_velox_schema_builder Folly::folly)

# Alpha code expects an upper case suffix to the generated file.
list(PREPEND FLATBUFFERS_FLATC_SCHEMA_EXTRA_ARGS "--filename-suffix"
     "Generated")

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Schema.fbs"
  ""
  alpha_velox_schema_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(alpha_velox_schema_fb INTERFACE)
target_include_directories(alpha_velox_schema_fb
                           INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(alpha_velox_schema_fb alpha_velox_schema_schema_fb)

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Metadata.fbs"
  ""
  alpha_velox_metadata_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(alpha_velox_metadata_fb INTERFACE)
target_include_directories(alpha_velox_metadata_fb
                           INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(alpha_velox_metadata_fb alpha_velox_metadata_schema_fb)

add_library(alpha_velox_schema_serialization SchemaSerialization.cpp)
target_link_libraries(
  alpha_velox_schema_serialization alpha_velox_schema_reader
  alpha_velox_schema_builder alpha_velox_schema_fb)

add_library(alpha_velox_deserializer Deserializer.cpp)
target_link_libraries(
  alpha_velox_deserializer
  alpha_common
  alpha_velox_common
  alpha_velox_field_reader
  velox_vector
  velox_memory
  velox_dwio_common)

add_library(alpha_velox_serializer Serializer.cpp)
target_link_libraries(alpha_velox_serializer alpha_common
                      alpha_velox_schema_builder velox_vector)

add_library(alpha_velox_reader StreamInputDecoder.cpp StreamLabels.cpp
                               VeloxReader.cpp)
target_link_libraries(
  alpha_velox_reader
  alpha_velox_schema
  alpha_velox_schema_serialization
  alpha_velox_schema_fb
  alpha_velox_metadata_fb
  alpha_common
  Folly::folly)

add_library(
  alpha_velox_writer EncodingLayoutTree.cpp FlushPolicy.cpp VeloxWriter.cpp
                     VeloxWriterDefaultMetadataOSS.cpp)
target_link_libraries(alpha_velox_writer alpha_encodings alpha_common
                      alpha_velox_metadata_fb Folly::folly)
