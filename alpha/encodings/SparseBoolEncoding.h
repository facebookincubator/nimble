// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <span>
#include "dwio/alpha/common/Buffer.h"
#include "dwio/alpha/common/FixedBitArray.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/encodings/EncodingIdentifier.h"
#include "dwio/alpha/encodings/EncodingSelection.h"
#include "velox/common/memory/Memory.h"

// Represents bools sparsely. Useful when only a small fraction  are set (or not
// set).
//
// In terms of space, this will be better than the trivial bitmap encoding when
// the fraction of set (or unset) bits is smaller than 1 / lg(n), where n is the
// number of rows in the stream. On normal size files thats around 5%.
//
// This encoding will likely be of most use in representing the nulls
// subencoding inside a NullableEncoding.
//
// TODO: Should we generalize the SparseEncoding idea to other data types?
// Maybe. Or maybe in our mainly-constant encoding implementation we simple use
// a SparseBoolEncoding on top of the dense encoded data.

namespace facebook::alpha {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: whether the sparse bits are set or unset
// XX bytes: indices encoding bytes
class SparseBoolEncoding final : public TypedEncoding<bool, bool> {
 public:
  using cppDataType = bool;

  static constexpr int kPrefixSize = 1;
  static constexpr int kSparseValueOffset = Encoding::kPrefixSize;
  static constexpr int kIndicesOffset = Encoding::kPrefixSize + 1;

  SparseBoolEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  static std::string_view encode(
      EncodingSelection<bool>& selection,
      std::span<const bool> values,
      Buffer& buffer);

 private:
  // If true, then indices give the position of the set bits; if false they give
  // the positions of the unset bits.
  const bool sparseValue_;
  Vector<char> indicesUncompressed_;
  detail::BufferedEncoding<uint32_t, 32> indices_;
  uint32_t nextIndex_; // The current index (FBA value).
  uint32_t row_;
};

} // namespace facebook::alpha
