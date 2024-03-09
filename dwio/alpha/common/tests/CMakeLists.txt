add_library(alpha_common_file_writer AlphaFileWriter.cpp)

target_link_libraries(alpha_common_file_writer alpha_common alpha_velox_writer
                      velox_vector)

add_executable(
  alpha_common_tests
  BitEncoderTests.cpp
  BitsTests.cpp
  ExceptionTests.cpp
  FixedBitArrayTests.cpp
  HuffmanTests.cpp
  IndexMapTests.cpp
  VarintTests.cpp
  VectorTests.cpp)

add_test(alpha_common_tests alpha_common_tests)

target_link_libraries(alpha_common_tests alpha_common gtest gtest_main
                      glog::glog Folly::folly)
