add_subdirectory(tests)

# Alpha code expects an upper case suffix to the generated file. 
list(PREPEND FLATBUFFERS_FLATC_SCHEMA_EXTRA_ARGS "--filename-suffix" "Generated")

build_flatbuffers(
  "${CMAKE_CURRENT_SOURCE_DIR}/Footer.fbs"
  ""
  alpha_footer_schema_fb
  ""
  "${CMAKE_CURRENT_BINARY_DIR}"
  ""
  "")
add_library(alpha_footer_fb INTERFACE)
target_include_directories(alpha_footer_fb INTERFACE ${FLATBUFFERS_INCLUDE_DIR})
add_dependencies(alpha_footer_fb alpha_footer_schema_fb)

add_library(
  alpha_tablet
  Compression.cpp
  StreamInput.cpp
  Tablet.cpp)

target_link_libraries(alpha_tablet alpha_footer_fb Folly::folly)
