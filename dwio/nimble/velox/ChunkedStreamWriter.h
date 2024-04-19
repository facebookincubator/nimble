// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Types.h"

namespace facebook::nimble {

class ChunkedStreamWriter {
 public:
  explicit ChunkedStreamWriter(
      Buffer& buffer,
      CompressionParams compressionParams = {
          .type = CompressionType::Uncompressed});

  std::vector<std::string_view> encode(std::string_view chunk);

 private:
  Buffer& buffer_;
  CompressionParams compressionParams_;
};

} // namespace facebook::nimble
