/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dwio/nimble/serializer/SerializerImpl.h"

#include <zstd.h>
#include <zstd_errors.h>

namespace facebook::nimble::serde::detail {

uint32_t getStringsTotalSize(std::string_view input) {
  const auto strData = reinterpret_cast<const std::string_view*>(input.data());
  const auto strDataEnd =
      reinterpret_cast<const std::string_view*>(input.end());
  uint32_t size = 0;
  for (auto sv = strData; sv < strDataEnd; ++sv) {
    size += sizeof(uint32_t);
    size += sv->size();
  }
  return size;
}

void encodeStrings(std::string_view input, uint32_t size, char* output) {
  const auto strData = reinterpret_cast<const std::string_view*>(input.data());
  const auto strDataEnd =
      reinterpret_cast<const std::string_view*>(input.end());
  encoding::writeUint32(size, output);
  for (auto sv = strData; sv < strDataEnd; ++sv) {
    encoding::writeString(*sv, output);
  }
}

uint32_t
encode(const SerializerOptions& options, std::string_view input, char* output) {
  // Size prefix + compression type + actual content
  uint32_t size = input.size();
  const auto compression = options.compressionType;
  bool writeUncompressed{true};
  if (compression != CompressionType::Uncompressed &&
      size >= options.compressionThreshold) {
    auto* compPos = output + sizeof(uint32_t);
    encoding::writeChar(static_cast<int8_t>(compression), compPos);
    // TODO: share compression implementation
    switch (compression) {
      case CompressionType::Zstd: {
        const auto ret = ZSTD_compress(
            compPos, size, input.data(), size, options.compressionLevel);
        if (ZSTD_isError(ret)) {
          NIMBLE_CHECK_EQ(
              static_cast<int>(ZSTD_getErrorCode(ret)),
              static_cast<int>(ZSTD_error_dstSize_tooSmall),
              "zstd error");
          // Fall back to uncompressed
        } else {
          size = ret;
          writeUncompressed = false;
        }
        break;
      }
      default:
        NIMBLE_UNSUPPORTED("Unsupported compression {}", toString(compression));
    }
  }

  if (writeUncompressed) {
    auto* compPos = output + sizeof(uint32_t);
    encoding::writeChar(
        static_cast<int8_t>(CompressionType::Uncompressed), compPos);
    std::copy(input.data(), input.end(), compPos);
  }
  encoding::writeUint32(size + 1, output);
  return size + sizeof(uint32_t) + 1;
}

} // namespace facebook::nimble::serde::detail
