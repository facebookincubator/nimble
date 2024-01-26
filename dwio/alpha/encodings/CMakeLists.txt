add_subdirectory(tests)

add_library(
  alpha_encodings
  Compression.cpp
  Encoding.cpp
  EncodingFactoryNew.cpp
  EncodingLayout.cpp
  EncodingLayoutCapture.cpp
  RleEncoding.cpp
  SparseBoolEncoding.cpp
  Statistics.cpp
  TrivialEncoding.cpp)

target_link_libraries(alpha_encodings Folly::folly absl::flat_hash_map)
