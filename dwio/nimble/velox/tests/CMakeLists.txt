add_library(nimble_velox_schema_utils SchemaUtils.cpp)
target_link_libraries(
  nimble_velox_schema_utils nimble_velox_schema_fb nimble_velox_schema_builder
  nimble_velox_schema_reader gtest gtest_main)

add_executable(
  nimble_velox_tests
  BufferGrowthPolicyTest.cpp
  EncodingLayoutTreeTests.cpp
  FlatMapLayoutPlannerTests.cpp
  OrderedRangesTests.cpp
  SchemaTests.cpp
  SerializationTests.cpp
  TypeTests.cpp
  VeloxReaderTests.cpp
  VeloxWriterTests.cpp)
add_test(nimble_velox_tests nimble_velox_tests)

target_link_libraries(
  nimble_velox_tests
  nimble_velox_common
  nimble_velox_schema_utils
  nimble_velox_reader
  nimble_velox_writer
  nimble_velox_serializer
  nimble_velox_deserializer
  nimble_velox_field_writer
  nimble_velox_flatmap_layout_planner
  nimble_common_file_writer
  nimble_common
  nimble_encodings
  velox_vector
  velox_vector_fuzzer
  velox_vector_test_lib
  gmock
  gtest
  gtest_main
  Folly::folly)
