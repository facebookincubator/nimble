add_library(alpha_velox_schema_utils SchemaUtils.cpp)
target_link_libraries(
  alpha_velox_schema_utils alpha_velox_schema_fb alpha_velox_schema_builder
  alpha_velox_schema_reader gtest gtest_main)

add_executable(
  alpha_velox_tests
  BufferGrowthPolicyTest.cpp
  EncodingLayoutTreeTests.cpp
  FlatMapLayoutPlannerTests.cpp
  OrderedRangesTests.cpp
  SchemaTests.cpp
  SerializationTests.cpp
  TypeTests.cpp
  VeloxReaderTests.cpp
  VeloxWriterTests.cpp)
add_test(alpha_velox_tests alpha_velox_tests)

target_link_libraries(
  alpha_velox_tests
  alpha_velox_common
  alpha_velox_schema_utils
  alpha_velox_reader
  alpha_velox_writer
  alpha_velox_serializer
  alpha_velox_deserializer
  alpha_velox_field_writer
  alpha_velox_flatmap_layout_planner
  alpha_common_file_writer
  alpha_common
  alpha_encodings
  velox_vector
  velox_vector_fuzzer
  velox_vector_test_lib
  gmock
  gtest
  gtest_main
  Folly::folly)
