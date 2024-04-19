add_library(nimble_common_file_writer NimbleFileWriter.cpp)

target_link_libraries(nimble_common_file_writer nimble_common
                      nimble_velox_writer velox_vector)

add_executable(
  nimble_common_tests
  BitEncoderTests.cpp
  BitsTests.cpp
  ExceptionTests.cpp
  FixedBitArrayTests.cpp
  HuffmanTests.cpp
  IndexMapTests.cpp
  VarintTests.cpp
  VectorTests.cpp)

add_test(nimble_common_tests nimble_common_tests)

target_link_libraries(nimble_common_tests nimble_common gtest gtest_main
                      glog::glog Folly::folly)
