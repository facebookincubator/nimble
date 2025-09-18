include_guard(GLOBAL)

# TODO: these variables are named VELOX_* because we are piggy-backing on
# Velox's resolve dependency module for now. We should change and have our own
# in the future.
set(VELOX_ABSL_VERSION 20240116.0)
set(
  VELOX_ABSL_BUILD_SHA256_CHECKSUM
  "338420448b140f0dfd1a1ea3c3ce71b3bc172071f24f4d9a57d59b45037da440"
)
set(
  VELOX_ABSL_SOURCE_URL
  "https://github.com/abseil/abseil-cpp/archive/refs/tags/${VELOX_ABSL_VERSION}.tar.gz"
)

velox_resolve_dependency_url(ABSL)

message(STATUS "Building Abseil from source")

FetchContent_Declare(absl URL ${VELOX_ABSL_SOURCE_URL} URL_HASH ${VELOX_ABSL_BUILD_SHA256_CHECKSUM})

set(ABSL_BUILD_TESTING OFF)
set(ABSL_PROPAGATE_CXX_STD ON)
set(ABSL_ENABLE_INSTALL ON)
FetchContent_MakeAvailable(absl)
