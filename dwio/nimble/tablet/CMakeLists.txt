add_subdirectory(tests)

# Nimble code expects an upper case suffix to the generated file.
list(PREPEND FLATBUFFERS_FLATC_SCHEMA_EXTRA_ARGS "--filename-suffix"
     "Generated")

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Footer.fbs"
  ""
  nimble_footer_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(nimble_footer_fb INTERFACE)
target_include_directories(
  nimble_footer_fb
  INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(nimble_footer_fb nimble_footer_schema_fb)

add_library(nimble_tablet Compression.cpp StreamInput.cpp Tablet.cpp)

target_link_libraries(nimble_tablet nimble_footer_fb Folly::folly)
