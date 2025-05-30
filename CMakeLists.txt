# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.14)

# Set the project name.
project(Nimble)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_compile_definitions(DISABLE_META_INTERNAL_COMPRESSOR=1)

# The policy allows us to change options without caching.
cmake_policy(SET CMP0077 NEW)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

# Sets new behavior for CMP0135, which controls how timestamps are extracted
# when using ExternalProject_Add():
# https://cmake.org/cmake/help/latest/policy/CMP0135.html
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
  set(CMAKE_POLICY_DEFAULT_CMP0135 NEW)
endif()

# Use ThirdPartyToolchain dependencies macros from Velox.
list(PREPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/CMake"
     "${PROJECT_SOURCE_DIR}/velox/CMake")
include(ResolveDependency)

set(VELOX_BUILD_MINIMAL_WITH_DWIO
    ON
    CACHE BOOL "Velox minimal build with dwio.")
set(VELOX_BUILD_VECTOR_TEST_UTILS
    ON
    CACHE BOOL "Velox vector test utilities (VectorMaker).")
set(VELOX_DEPENDENCY_SOURCE
    AUTO
    CACHE STRING "Default dependency source: AUTO SYSTEM or BUNDLED.")

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

check_cxx_compiler_flag("-Wunknown-warning-option"
                        COMPILER_HAS_W_UNKNOWN_WARNING_OPTION)
if(COMPILER_HAS_W_UNKNOWN_WARNING_OPTION)
  string(APPEND CMAKE_CXX_FLAGS " -Wno-unknown-warning-option")
endif()

check_cxx_compiler_flag("-Wnullability-completeness"
                        COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
if(COMPILER_HAS_W_NULLABILITY_COMPLETENESS)
  string(APPEND CMAKE_CXX_FLAGS " -Wno-nullability-completeness")
endif()

# Nimble, Velox and folly need to be compiled with the same compiler flags.
execute_process(
  COMMAND
    bash -c
    "( source ${CMAKE_CURRENT_SOURCE_DIR}/velox/scripts/setup-helper-functions.sh && echo -n $(get_cxx_flags $ENV{CPU_TARGET}))"
  OUTPUT_VARIABLE SCRIPT_CXX_FLAGS
  RESULT_VARIABLE COMMAND_STATUS)

if(COMMAND_STATUS EQUAL "1")
  message(FATAL_ERROR "Unable to determine compiler flags!")
endif()
message("Setting CMAKE_CXX_FLAGS=${SCRIPT_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${SCRIPT_CXX_FLAGS}")

message("FINAL CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

include(CTest) # include after project() but before add_subdirectory()

# This doesn't necessarily need to be a dependency (we can check in the
# generated .cpp/.h files), but adding this for convenience for now.
find_package(FlatBuffers REQUIRED)

velox_set_source(gtest)
velox_resolve_dependency(gtest)

velox_set_source(glog)
velox_resolve_dependency(glog)

velox_set_source(gflags)
velox_resolve_dependency(gflags COMPONENTS shared)

set(BOOST_INCLUDE_LIBRARIES algorithm context filesystem program_options)

velox_set_source(Boost)
velox_resolve_dependency(Boost 1.77.0 COMPONENTS ${BOOST_INCLUDE_LIBRARIES})

velox_set_source(folly)
velox_resolve_dependency(folly)

velox_set_source(abseil)
velox_resolve_dependency(abseil)

# Use xxhash and xsimd from Velox for now.
include_directories(.)
include_directories(SYSTEM velox)
include_directories(SYSTEM velox/velox/external/xxhash)
include_directories(SYSTEM ${CMAKE_BINARY_DIR}/_deps/xsimd-src/include/)

add_subdirectory(velox)
add_subdirectory(dwio/nimble/common)
add_subdirectory(dwio/nimble/tablet)
add_subdirectory(dwio/nimble/tools)
add_subdirectory(dwio/nimble/encodings)
add_subdirectory(dwio/nimble/velox)
