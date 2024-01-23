add_executable(
  alpha_encodings_tests
  ConstantEncodingTests.cpp
  EncodingLayoutTests.cpp
  EncodingSelectionTests.cpp
  EncodingTestsNew.cpp
  MainlyConstantEncodingTests.cpp
  NullableEncodingTests.cpp
  RleEncodingTests.cpp
  SentinelEncodingTests.cpp
  StatisticsTests.cpp)

add_test(alpha_encodings_tests alpha_encodings_tests)

target_link_libraries(
  alpha_encodings_tests
  alpha_encodings
  alpha_common
  alpha_tools_common
  gtest
  gtest_main
  Folly::folly)
