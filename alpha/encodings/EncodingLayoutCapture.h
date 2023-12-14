// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <string_view>
#include "dwio/alpha/encodings/EncodingLayout.h"

namespace facebook::alpha {

class EncodingLayoutCapture {
 public:
  // Captures an encoding tree from an encoded stream.
  // It traverses the encoding headers in the stream and produces a serialized
  // encoding tree layout.
  // |encoding| - The serialized encoding
  static EncodingLayout capture(std::string_view encoding);
};

} // namespace facebook::alpha
