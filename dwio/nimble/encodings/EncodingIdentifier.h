// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include <cstdint>

namespace facebook::nimble {

// When encoding contains nested encodings, each nested encoding has its
// own identifier.
using NestedEncodingIdentifier = uint8_t;

struct EncodingIdentifiers {
  struct Dictionary {
    static constexpr NestedEncodingIdentifier Alphabet = 0;
    static constexpr NestedEncodingIdentifier Indices = 1;
  };

  struct MainlyConstant {
    static constexpr NestedEncodingIdentifier IsCommon = 0;
    static constexpr NestedEncodingIdentifier OtherValues = 1;
  };

  struct Nullable {
    static constexpr NestedEncodingIdentifier Data = 0;
    static constexpr NestedEncodingIdentifier Nulls = 1;
  };

  struct RunLength {
    static constexpr NestedEncodingIdentifier RunLengths = 0;
    static constexpr NestedEncodingIdentifier RunValues = 1;
  };

  struct SparseBool {
    static constexpr NestedEncodingIdentifier Indices = 0;
  };

  struct Trivial {
    static constexpr NestedEncodingIdentifier Lengths = 0;
  };
};

} // namespace facebook::nimble
