include_guard(GLOBAL)

# TODO: these variables are named VELOX_* because we are piggy-backing on
# Velox's resolve dependency module for now. We should change and have our own
# in the future.
set(VELOX_ABSEIL_VERSION 20240116.0)
set(VELOX_ABSEIL_BUILD_SHA256_CHECKSUM
    "338420448b140f0dfd1a1ea3c3ce71b3bc172071f24f4d9a57d59b45037da440")
set(VELOX_ABSEIL_SOURCE_URL
    "https://github.com/abseil/abseil-cpp/archive/refs/tags/${VELOX_ABSEIL_VERSION}.tar.gz"
)

velox_resolve_dependency_url(ABSEIL)

message(STATUS "Building abseil from source")

FetchContent_Declare(
  abseil
  URL ${VELOX_ABSEIL_SOURCE_URL}
  URL_HASH ${VELOX_ABSEIL_BUILD_SHA256_CHECKSUM})

FetchContent_MakeAvailable(abseil)
