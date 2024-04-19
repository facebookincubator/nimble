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
  nimble_encodings
  nimble_common
  nimble_tools_common
  gtest
  gtest_main
  Folly::folly)
