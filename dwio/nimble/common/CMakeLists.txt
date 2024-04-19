add_subdirectory(tests)

add_library(
  nimble_common
  Bits.cpp
  Checksum.cpp
  FixedBitArray.cpp
  Huffman.cpp
  MetricsLogger.cpp
  Types.cpp
  Varint.cpp)

target_link_libraries(nimble_common velox_memory Folly::folly)
