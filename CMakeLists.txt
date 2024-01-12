cmake_minimum_required(VERSION 3.14)

# Set the project name.
project(Alpha)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Use ThirdPartyToolchain dependencies macros from Velox.
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/velox/CMake")
include(ResolveDependency)

set(VELOX_BUILD_MINIMAL ON CACHE BOOL "Velox minimal build.")

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message("FINAL CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

include(CTest) # include after project() but before add_subdirectory()

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

# TODO - compile only Velox submodule and alpha/common
add_subdirectory(velox)
add_subdirectory(dwio/alpha/common)
