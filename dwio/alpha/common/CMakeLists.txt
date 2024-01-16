add_subdirectory(tests)

add_library(
  alpha_common
  Bits.cpp
  Checksum.cpp
  FixedBitArray.cpp
  Huffman.cpp
  MetricsLogger.cpp
  Types.cpp
  Varint.cpp)

target_link_libraries(alpha_common velox_memory Folly::folly)
