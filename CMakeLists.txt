cmake_minimum_required(VERSION 3.14)

# Set the project name.
project(Alpha)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Use ThirdPartyToolchain dependencies macros from Velox.
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake"
     "${PROJECT_SOURCE_DIR}/velox/CMake")
include(ResolveDependency)

set(VELOX_BUILD_MINIMAL ON CACHE BOOL "Velox minimal build.")
set(VELOX_DEPENDENCY_SOURCE
  AUTO
  CACHE
  STRING
  "Default dependency source: AUTO SYSTEM or BUNDLED."
)

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")

# Ignore known compiler warnings.
check_cxx_compiler_flag("-Wstringop-overread" COMPILER_HAS_W_STRINGOP_OVERREAD)
if(COMPILER_HAS_W_STRINGOP_OVERREAD)
  string(APPEND CMAKE_CXX_FLAGS " -Wno-stringop-overread")
endif()

check_cxx_compiler_flag("-Wdeprecated-declarations"
                        COMPILER_HAS_W_DEPRECATED_DECLARATIONS)
if(COMPILER_HAS_W_DEPRECATED_DECLARATIONS)
  string(APPEND CMAKE_CXX_FLAGS " -Wno-deprecated-declarations")
endif()

check_cxx_compiler_flag("-Wmaybe-uninitialized"
                        COMPILER_HAS_W_MAYBE_UNINITIALIZED)
if(COMPILER_HAS_W_MAYBE_UNINITIALIZED)
  string(APPEND CMAKE_CXX_FLAGS " -Wno-maybe-uninitialized")
endif()


message("FINAL CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

include(CTest) # include after project() but before add_subdirectory()

# This doesn't necessarily need to be a dependency (we can check in the
# generated .cpp/.h files), but adding this for convenience for now.
find_package(FlatBuffers REQUIRED)

set_source(gtest)
resolve_dependency(gtest)

set_source(glog)
resolve_dependency(glog)

set_source(gflags)
resolve_dependency(gflags COMPONENTS shared)

set_source(folly)
resolve_dependency(folly)

# Use xxhash and xsimd from Velox for now.
include_directories(.)
include_directories(SYSTEM velox)
include_directories(SYSTEM velox/velox/external/xxhash)
include_directories(SYSTEM ${CMAKE_BINARY_DIR}/_deps/xsimd-src/include/)

add_subdirectory(velox)
add_subdirectory(dwio/alpha/common)
add_subdirectory(dwio/alpha/tablet)
add_subdirectory(dwio/alpha/tools)
add_subdirectory(dwio/alpha/encodings)
