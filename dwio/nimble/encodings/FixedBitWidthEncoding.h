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
#pragma once

#include <span>
#include <type_traits>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Compression.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "velox/dwio/common/DecoderUtil.h"

// The FixedBitWidthEncoding stores integer data in a fixed number of
// bits equal to the number of bits required to represent the largest value in
// the encoding. For now we only support encoding non-negative values, but
// we may later add an optional zigzag encoding that will let us handle
// negatives.

namespace facebook::nimble {

// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 1 byte: compression type
// sizeof(T) byte: baseline value
// 1 byte: bit width
// FixedBitArray::BufferSize(rowCount, bit_width) bytes: packed values.
template <typename T>
class FixedBitWidthEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kPrefixSize = 2 + sizeof(T);

  FixedBitWidthEncoding(
      velox::memory::MemoryPool& memoryPool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  // Bulk scan method for fast path decoding.
  // Reads multiple values at once and processes them through the visitor.
  // This is used by readWithVisitorFast for efficient batch processing.
  template <bool kScatter, typename Visitor>
  void bulkScan(
      Visitor& visitor,
      vector_size_t currentRow,
      const vector_size_t* selectedRows,
      vector_size_t numSelected,
      const vector_size_t* scatterRows);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  std::string debugString(int offset) const final;

 private:
  int bitWidth_;
  physicalType baseline_;
  FixedBitArray fixedBitArray_;
  uint32_t row_ = 0;
  Vector<char> uncompressedData_;
  Vector<physicalType> buffer_;
};

//
// End of public API. Implementations follow.
//

template <typename T>
FixedBitWidthEncoding<T>::FixedBitWidthEncoding(
    velox::memory::MemoryPool& memoryPool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{memoryPool, data, options},
      uncompressedData_{&memoryPool},
      buffer_{&memoryPool} {
  auto pos = data.data() + this->dataOffset();
  auto compressionType = static_cast<CompressionType>(encoding::readChar(pos));
  baseline_ = encoding::read<const physicalType>(pos);
  bitWidth_ = static_cast<uint32_t>(encoding::readChar(pos));
  if (compressionType != CompressionType::Uncompressed) {
    uncompressedData_ = Compression::uncompress(
        memoryPool,
        compressionType,
        DataType::Undefined,
        {pos, static_cast<size_t>(data.end() - pos)});
    fixedBitArray_ = FixedBitArray{
        {uncompressedData_.data(), uncompressedData_.size()}, bitWidth_};
  } else {
    fixedBitArray_ =
        FixedBitArray{{pos, static_cast<size_t>(data.end() - pos)}, bitWidth_};
  }
}

template <typename T>
void FixedBitWidthEncoding<T>::reset() {
  row_ = 0;
}

template <typename T>
void FixedBitWidthEncoding<T>::skip(uint32_t rowCount) {
  row_ += rowCount;
}

template <typename T>
void FixedBitWidthEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if constexpr (isFourByteIntegralType<physicalType>()) {
    fixedBitArray_.bulkGetWithBaseline32(
        row_, rowCount, static_cast<uint32_t*>(buffer), baseline_);
  } else if constexpr (isEightByteIntegralType<physicalType>()) {
    fixedBitArray_.bulkGet64WithBaseline(
        row_, rowCount, static_cast<uint64_t*>(buffer), baseline_);
  } else {
    const uint32_t start = row_;
    const uint32_t end = start + rowCount;
    physicalType* output = static_cast<physicalType*>(buffer);
    for (uint32_t i = start; i < end; ++i) {
      *output++ = fixedBitArray_.get(i) + baseline_;
    }
  }
  row_ += rowCount;
}

template <typename T>
template <typename V>
void FixedBitWidthEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  // Fast path: use bulk scan for 4-byte and 8-byte integral types.
  // Compile-time: type constraints (FBW physical type, ExtractToReader,
  // compatible integral output type at least as wide as the physical type).
  // Runtime (useFastPath): deterministic filter, AVX2, bulk path enabled,
  // null+filter/hook compatibility.
  using OutputType = detail::ValueType<typename V::DataType>;
  if constexpr (
      (isFourByteIntegralType<physicalType>() ||
       isEightByteIntegralType<physicalType>()) &&
      std::is_same_v<
          typename V::Extract,
          velox::dwio::common::ExtractToReader> &&
      std::is_integral_v<physicalType> && std::is_integral_v<OutputType> &&
      sizeof(OutputType) >= sizeof(physicalType)) {
    auto* nulls = visitor.reader().rawNullsInReadRange();
    if (velox::dwio::common::useFastPath(visitor, nulls)) {
      detail::readWithVisitorFast(*this, visitor, params, nulls);
      return;
    }
  }
  // Slow path: process one value at a time.
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value = fixedBitArray_.get(row_++) + baseline_;
        return value;
      });
}

template <typename T>
template <bool kScatter, typename V>
void FixedBitWidthEncoding<T>::bulkScan(
    V& visitor,
    vector_size_t currentRow,
    const vector_size_t* selectedRows,
    vector_size_t numSelected,
    const vector_size_t* scatterRows) {
  using OutputType = detail::ValueType<typename V::DataType>;
  static_assert(
      isFourByteIntegralType<physicalType>() ||
          isEightByteIntegralType<physicalType>(),
      "bulkScan only supports 4-byte or 8-byte integral types");

  if (numSelected == 0) {
    return;
  }

  const auto numRows = visitor.numRows() - visitor.rowIndex();

  // Calculate offset between our internal position and the external row number.
  // This handles cases where the encoding position (row_) differs from the
  // logical row number (currentRow).
  const auto offset =
      static_cast<int32_t>(row_) - static_cast<int32_t>(currentRow);

  // Get the output buffer.
  auto* values = detail::mutableValues<OutputType>(visitor, numRows);

  // Check type compatibility:
  // - Same type or same-size integral types: use fast memcpy
  //   (e.g., uint32_t vs int32_t have same bit representation)
  // - Widening (e.g., int32 → int64): use loop with implicit conversion
  constexpr bool kSameSize = sizeof(physicalType) == sizeof(OutputType);
  constexpr bool kIsUpcast = sizeof(OutputType) > sizeof(physicalType) &&
      std::is_integral_v<OutputType> && std::is_integral_v<physicalType>;

  if constexpr (V::dense) {
    if constexpr (isFourByteIntegralType<physicalType>()) {
      // 4-byte path: use the optimized template-unrolled bulk decode.
      buffer_.resize(numSelected);
      fixedBitArray_.bulkGetWithBaseline32(
          selectedRows[0] + offset,
          numSelected,
          reinterpret_cast<uint32_t*>(buffer_.data()),
          baseline_);

      if constexpr (kSameSize) {
        std::memcpy(values, buffer_.data(), numSelected * sizeof(physicalType));
      } else if constexpr (kIsUpcast) {
        for (vector_size_t i = 0; i < numSelected; ++i) {
          values[i] = static_cast<OutputType>(buffer_[i]);
        }
      }
    } else {
      // 8-byte path: use bulkGet64WithBaseline which handles all bit widths
      // including branchless byte-aligned loads for bitWidth <= 56.
      static_assert(isEightByteIntegralType<physicalType>());
      static_assert(kSameSize, "8-byte bulkScan requires same-size output");
      fixedBitArray_.bulkGet64WithBaseline(
          selectedRows[0] + offset,
          numSelected,
          reinterpret_cast<uint64_t*>(values),
          baseline_);
    }
  } else {
    // Sparse case: read individual values at specified positions.
    for (vector_size_t i = 0; i < numSelected; ++i) {
      values[i] = fixedBitArray_.get(selectedRows[i] + offset) + baseline_;
    }
  }

  // Phase 2: Post-decode processing.
  // processFixedWidthRun handles three concerns:
  //   - Scatter (kScatter=true): move values from packed non-null positions to
  //     their correct output positions using scatterRows (null gaps).
  //   - Filter: evaluate the filter on decoded values and record passing row
  //     numbers in filterHits. numValues is updated in-place by the call.
  //   - Hook: forward values to the hook callback instead of the reader buffer.
  //
  // For non-hook visitors, re-read the values pointer from rawValues() because
  // processFixedWidthRun expects the canonical buffer pointer.
  if constexpr (!V::kHasHook) {
    values = reinterpret_cast<OutputType*>(visitor.reader().rawValues());
  }

  // Snapshot numValues before the call — processFixedWidthRun updates it
  // in-place. The delta (post - pre) gives the count of filter-passing rows.
  int32_t numValues = visitor.reader().numValues();
  int32_t* filterHits;
  if constexpr (V::kHasFilter) {
    NIMBLE_DCHECK_EQ(visitor.reader().numRows(), numValues, "");
    filterHits = visitor.outputRows(numSelected) - numValues;
  } else {
    filterHits = nullptr;
  }

  velox::dwio::common::
      processFixedWidthRun<OutputType, V::kFilterOnly, kScatter, V::dense>(
          velox::RowSet(selectedRows, numSelected),
          0,
          numSelected,
          scatterRows,
          values,
          filterHits,
          numValues,
          visitor.filter(),
          visitor.hook());

  row_ += selectedRows[numSelected - 1] - currentRow + 1;

  if constexpr (!V::kHasHook) {
    visitor.addNumValues(
        V::kHasFilter ? numValues - visitor.reader().numValues() : numRows);
  }
  visitor.setRowIndex(visitor.numRows());
}

template <typename T>
std::string_view FixedBitWidthEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  static_assert(
      std::is_same_v<
          typename std::make_unsigned<physicalType>::type,
          physicalType>,
      "Physical type must be unsigned.");
  const uint32_t rowCount = values.size();
  // NOTE: If we end up with bitsRequired not being a multiple of 8, it degrades
  // compression efficiency a lot. To (temporarily) mitigate this, we revert to
  // using "FixedByteWidth", meaning that we round the required bitness to the
  // closest byte. This is a temporary solution. Going forward we should
  // evaluate other options. For example:
  // 1. Asymetric bit width, where we don't encode the data during write
  // (instead we use Trivial + Compression), but we encode it when we read (for
  // better in-memory representation).
  // 2. Apply compression only if bit width is a multiple of 8
  // 3. Try both bit width and byte width and pick one.
  // 4. etc...
  const int bitsRequired =
      (velox::bits::bitsRequired(
           selection.statistics().max() - selection.statistics().min()) +
       7) &
      ~7;

  const uint32_t fixedBitArraySize =
      FixedBitArray::bufferSize(values.size(), bitsRequired);

  Vector<char> vector{&buffer.getMemoryPool()};

  auto dataCompressionPolicy = selection.compressionPolicy();
  CompressionEncoder<T> compressionEncoder{
      buffer.getMemoryPool(),
      *dataCompressionPolicy,
      DataType::Undefined,
      bitsRequired,
      fixedBitArraySize,
      [&]() {
        vector.resize(fixedBitArraySize);
        return std::span<char>{vector};
      },
      [&, baseline = selection.statistics().min()](char*& pos) {
        memset(pos, 0, fixedBitArraySize);
        FixedBitArray fba(pos, bitsRequired);
        if constexpr (sizeof(physicalType) == 4) {
          fba.bulkSet32WithBaseline(
              0,
              rowCount,
              reinterpret_cast<const uint32_t*>(values.data()),
              baseline);
        } else {
          // TODO: We may want to support 32-bit mode with (u)int64 here as
          // well.
          for (uint32_t i = 0; i < values.size(); ++i) {
            fba.set(i, values[i] - baseline);
          }
        }
        pos += fixedBitArraySize;
        return pos;
      }};

  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) +
      FixedBitWidthEncoding<T>::kPrefixSize + compressionEncoder.getSize();
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::FixedBitWidth,
      TypeTraits<T>::dataType,
      rowCount,
      useVarint,
      pos);
  encoding::writeChar(
      static_cast<char>(compressionEncoder.compressionType()), pos);
  encoding::write(selection.statistics().min(), pos);
  encoding::writeChar(bitsRequired, pos);
  compressionEncoder.write(pos);

  NIMBLE_DCHECK_EQ(encodingSize, pos - reserved, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string FixedBitWidthEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bit_width={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      bitWidth_);
}

} // namespace facebook::nimble
