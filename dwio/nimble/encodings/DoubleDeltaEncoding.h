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

#include <algorithm>
#include <cstring>
#include <span>
#include <type_traits>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/FixedBitArray.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/Encoding.h"
#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "dwio/nimble/encodings/selection/EncodingSelection.h"
#include "velox/common/base/BitUtil.h"

// DoubleDeltaEncoding stores 64-bit integer streams using
// second-order differencing (delta-of-deltas). Remaining values beyond the
// first two are zigzag-encoded, shifted by a frame-of-reference minimum, and
// bitpacked at a uniform bit width. Periodic checkpoints allow O(K) seek
// instead of O(N) sequential decode.
//
// Targets monotone sequences with regular intervals (e.g. timestamps) where
// the double-deltas collapse near zero -- perfectly regular timestamps encode
// at bitWidth=0 and ~31 bytes regardless of row count.
//
// Ported from AusLongListDeltaOfDeltaEncoder (multifeed/datafm/encode/) with
// bitpacking in place of GroupVarint, plus forward-decode checkpoints for
// random access.

namespace facebook::nimble {

/// Encodes 64-bit integer streams using double-delta bitpacking.
///
/// Data layout (after the standard Encoding prefix):
///
///   8 bytes  firstValue           -- raw int64 stored as uint64 wire bytes
///   8 bytes  secondValue          -- raw int64 as uint64 (zero when N < 2)
///   8 bytes  minZigzagDoubleDelta -- frame-of-reference floor (zero when
///                                    N < 3)
///   1 byte   bitWidth             -- bits per bitpacked residual [0, 64]
///   variable bitpacked residuals  -- FixedBitArray::bufferSize(N - 2,
///                                    bitWidth) bytes; omitted when
///                                    bitWidth == 0 or N < 3
///   4 bytes  checkpointCount      -- uint32, native byte order
///   variable checkpoint entries   -- checkpointCount * 16 bytes, each is
///                                    {uint64 lastValue, uint64 lastDelta}
///                                    captured every kCheckpointStride
///                                    residuals
///
/// The checkpoint segment is always present (even when checkpointCount == 0).
template <typename T>
class DoubleDeltaEncoding final
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
  static_assert(
      std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>,
      "DoubleDelta only supports 64-bit integer types.");

 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  /// Fixed-size header following the standard Encoding prefix:
  /// firstValue(8) + secondValue(8) + minZigzagDoubleDelta(8) + bitWidth(1).
  static constexpr int kPrefixSize = 8 + 8 + 8 + 1;

  /// Forward-decode anchor stride. Every kCheckpointStride residuals we
  /// snapshot {lastValue, lastDelta} so seek-then-materialize costs
  /// O(kCheckpointStride) instead of O(targetRow). Storage overhead is
  /// 16 bytes per kCheckpointStride elements (~0.25% for 8-byte payloads).
  static constexpr uint32_t kCheckpointStride = 64;

  /// Construct a decoder from serialized wire bytes.
  DoubleDeltaEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      const std::function<void*(uint32_t)>& stringBufferFactory,
      const Encoding::Options& options = {});

  /// Reset the read cursor to the beginning of the stream.
  void reset() final;

  /// Advance the read cursor by `rowCount` rows without emitting values.
  void skip(uint32_t rowCount) final;

  /// Decode the next `rowCount` values into `buffer`. The caller must
  /// ensure `buffer` has room for at least rowCount * sizeof(physicalType).
  void materialize(uint32_t rowCount, void* buffer) final;

  /// Visitor-based read interface for selective/filtered reads.
  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  /// Encode `values` into the wire format, appending to `buffer`.
  /// Returns a view into the buffer covering the encoded bytes.
  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  /// Return a human-readable description of this encoding for diagnostics.
  std::string debugString(int offset) const final;

 private:
  // Decode the next bitpacked double-delta residual at the given index
  // (residualIndex == row_ - 2). Accumulate into lastDelta_ and lastValue_
  // and return the reconstructed value.
  uint64_t advanceFromBitpacked(uint32_t residualIndex);

  // Snapshot of decoder state captured every kCheckpointStride residuals
  // during encode. Enables O(1) seek to any checkpoint boundary.
  struct Checkpoint {
    // Reconstructed value at the checkpoint boundary.
    uint64_t lastValue{0};
    // Delta from the previous to the checkpoint boundary value.
    uint64_t lastDelta{0};
  };
  static_assert(sizeof(Checkpoint) == 16);

  // Load checkpoint[i] from wire bytes. Use memcpy for alignment safety
  // since the wire buffer may not be 8-byte aligned.
  Checkpoint loadCheckpoint(uint32_t index) const {
    Checkpoint checkpoint{};
    std::memcpy(
        &checkpoint,
        checkpointStart_ + index * sizeof(Checkpoint),
        sizeof(Checkpoint));
    return checkpoint;
  }

  // Fast-forward decoder state to the best checkpoint covering `targetRow`.
  // Skip when no checkpoint improves over the current row_ position. After
  // seeking, at most kCheckpointStride - 1 rows remain to be walked by the
  // per-row decode loop.
  void seekToCheckpointFor(uint32_t targetRow);

  // -- Wire-format fields, populated in the constructor --

  // All wire bytes are stored as uint64 and re-cast to the signed/unsigned
  // cpp type at emission time.
  uint64_t firstWire_{0};
  uint64_t secondWire_{0};
  // Frame-of-reference floor subtracted from each zigzag-encoded
  // double-delta before bitpacking.
  uint64_t minZigzagDoubleDelta_{0};
  // Uniform bit width of each bitpacked residual, range [0, 64].
  uint8_t bitWidth_{0};

  // Bitpacked double-delta residual region. Default-constructed (no backing
  // buffer) when bitWidth_ == 0 or rowCount_ < 3.
  FixedBitArray fixedBitArray_{};

  // Pointer into wire bytes at the first checkpoint entry (immediately after
  // the 4-byte count). nullptr when checkpointCount_ == 0.
  const char* checkpointStart_{nullptr};
  // Number of checkpoint entries in the wire data.
  uint32_t checkpointCount_{0};

  // -- Mutable decoder state --

  // Absolute row position of the next value to be decoded.
  uint32_t row_{0};
  // Last decoded value at position (row_ - 1). Valid when row_ >= 1.
  uint64_t lastValue_{0};
  // Delta from position (row_ - 2) to (row_ - 1). Valid when row_ >= 2.
  uint64_t lastDelta_{0};
};

namespace detail {
// TODO: Move to a common place.
// Zigzag-encode a 64-bit value using the standard interleave mapping:
//   0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, ...
// Accepts uint64 so callers can pass raw wire bytes; the arithmetic right
// shift is performed via a signed cast to extend the sign bit. Both the
// shift and the XOR are well-defined for every uint64 input, including
// the bit pattern of INT64_MIN.
inline uint64_t zigzagEncode64(uint64_t value) {
  const auto signedValue = static_cast<int64_t>(value);
  return (value << 1) ^ static_cast<uint64_t>(signedValue >> 63);
}

// Zigzag-decode a uint64 back to the original int64 bit pattern (returned
// as uint64 for uniform wire handling). Inverse of zigzagEncode64 for all
// 2^64 inputs.
inline uint64_t zigzagDecode64(uint64_t value) {
  return (value >> 1) ^ (~(value & 1) + 1);
}

} // namespace detail

//
// End of public API. Implementations follow.
//

template <typename T>
DoubleDeltaEncoding<T>::DoubleDeltaEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    const std::function<void*(uint32_t)>& /* stringBufferFactory */,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options} {
  const char* pos = data.data() + this->dataOffset();

  // Read the fixed-size header fields.
  firstWire_ = encoding::read<uint64_t>(pos);
  secondWire_ = encoding::read<uint64_t>(pos);
  minZigzagDoubleDelta_ = encoding::read<uint64_t>(pos);
  bitWidth_ = static_cast<uint8_t>(encoding::readChar(pos));
  NIMBLE_CHECK_LE(bitWidth_, 64, "DoubleDelta bit width must be in [0, 64].");

  // Parse the bitpacked residual region when present.
  size_t bitpackedSize = 0;
  if (bitWidth_ > 0 && this->rowCount_ > 2) {
    const uint32_t residualCount = this->rowCount_ - 2;
    bitpackedSize = FixedBitArray::bufferSize(residualCount, bitWidth_);
    // Guard against truncated wire data that would let the decoder read past
    // the buffer boundary.
    NIMBLE_CHECK_GE(
        static_cast<size_t>(data.end() - pos),
        bitpackedSize,
        "DoubleDelta bitpacked region underruns wire data.");
    fixedBitArray_ =
        FixedBitArray{{pos, static_cast<size_t>(data.end() - pos)}, bitWidth_};
  }
  pos += bitpackedSize;

  // Parse the trailing checkpoint segment: uint32 count followed by
  // count * sizeof(Checkpoint) bytes of checkpoint entries.
  NIMBLE_CHECK_GE(
      static_cast<size_t>(data.end() - pos),
      sizeof(uint32_t),
      "DoubleDelta checkpoint count underruns wire data.");
  checkpointCount_ = encoding::read<uint32_t>(pos);
  NIMBLE_CHECK_GE(
      static_cast<size_t>(data.end() - pos),
      static_cast<size_t>(checkpointCount_) * sizeof(Checkpoint),
      "DoubleDelta checkpoint array underruns wire data.");
  checkpointStart_ = checkpointCount_ > 0 ? pos : nullptr;

  reset();
}

template <typename T>
void DoubleDeltaEncoding<T>::reset() {
  row_ = 0;
  lastValue_ = 0;
  lastDelta_ = 0;
}

template <typename T>
uint64_t DoubleDeltaEncoding<T>::advanceFromBitpacked(uint32_t residualIndex) {
  // Recover the original double-delta:
  //   stored residual = zigzag(doubleDelta) - minZigzagDoubleDelta
  //   doubleDelta     = zigzagDecode(residual + minZigzagDoubleDelta)
  const uint64_t residual =
      bitWidth_ == 0 ? 0 : fixedBitArray_.get(residualIndex);
  const uint64_t doubleDelta =
      detail::zigzagDecode64(residual + minZigzagDoubleDelta_);

  // Accumulate via unsigned wrap-around arithmetic. This preserves bit-exact
  // recovery for int64 values whose true deltas span the signed range.
  lastDelta_ += doubleDelta;
  lastValue_ += lastDelta_;
  return lastValue_;
}

template <typename T>
void DoubleDeltaEncoding<T>::seekToCheckpointFor(uint32_t targetRow) {
  if (checkpointCount_ == 0 || targetRow < kCheckpointStride + 2) {
    return;
  }
  // checkpoint[i] captures state after residual (i+1)*K - 1 has been decoded,
  // which corresponds to row (i+1)*K + 1 having been emitted. The next row
  // to decode from that checkpoint is (i+1)*K + 2. Find the largest i such
  // that this next-row is both <= targetRow and > row_ (never step backward).
  const uint32_t checkpointIndex = (targetRow - 2) / kCheckpointStride - 1;
  const uint32_t boundedIndex = std::min(checkpointIndex, checkpointCount_ - 1);
  const uint32_t nextRow = (boundedIndex + 1) * kCheckpointStride + 2;
  if (nextRow <= row_) {
    return;
  }
  const auto checkpoint = loadCheckpoint(boundedIndex);
  lastValue_ = checkpoint.lastValue;
  lastDelta_ = checkpoint.lastDelta;
  row_ = nextRow;
}

template <typename T>
void DoubleDeltaEncoding<T>::skip(uint32_t rowCount) {
  if (rowCount == 0) {
    return;
  }

  const uint32_t end = row_ + rowCount;

  // Jump to the best checkpoint when it saves work over sequential decode.
  seekToCheckpointFor(end);

  // Handle the first two seed rows individually.
  while (row_ < end && row_ < 2) {
    if (row_ == 0) {
      lastValue_ = firstWire_;
    } else {
      lastDelta_ = secondWire_ - firstWire_;
      lastValue_ = secondWire_;
    }
    ++row_;
  }

  // Batch-decode remaining residuals. Bulk-extract into a temporary buffer
  // and prefix-sum, avoiding per-element get() overhead.
  const uint32_t residualCount = end - row_;
  if (residualCount == 0) {
    return;
  }
  if (bitWidth_ == 0) {
    // All double-deltas are identical (minZigzagDoubleDelta_ after zigzag
    // decode). Fast-forward with closed-form arithmetic.
    const uint64_t doubleDelta = detail::zigzagDecode64(minZigzagDoubleDelta_);
    const uint64_t n = residualCount;
    // After n steps: lastDelta advances by n * doubleDelta,
    // lastValue advances by sum_{k=1..n}(lastDelta + k * doubleDelta)
    //   = n * lastDelta + doubleDelta * n*(n+1)/2
    lastValue_ += lastDelta_ * n + doubleDelta * (n * (n + 1) / 2);
    lastDelta_ += doubleDelta * n;
    row_ = end;
    return;
  }
  // After checkpoint seek, at most kCheckpointStride - 1 residuals remain.
  NIMBLE_DCHECK_LE(
      residualCount,
      kCheckpointStride,
      "skip residualCount exceeds checkpoint stride.");
  uint64_t zigzagBuf[kCheckpointStride];
  const uint32_t residualStart = row_ - 2;
  fixedBitArray_.bulkGet64WithBaseline(
      /*start=*/residualStart,
      /*length=*/residualCount,
      zigzagBuf,
      /*baseline=*/minZigzagDoubleDelta_);
  for (uint32_t i = 0; i < residualCount; ++i) {
    const uint64_t doubleDelta = detail::zigzagDecode64(zigzagBuf[i]);
    lastDelta_ += doubleDelta;
    lastValue_ += lastDelta_;
  }
  row_ = end;
}

template <typename T>
void DoubleDeltaEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  if (rowCount == 0) {
    return;
  }

  auto* output = static_cast<physicalType*>(buffer);
  const uint32_t end = row_ + rowCount;
  uint32_t outputIndex = 0;

  // Handle the first two seed rows individually.
  while (row_ < end && row_ < 2) {
    if (row_ == 0) {
      lastValue_ = firstWire_;
    } else {
      lastDelta_ = secondWire_ - firstWire_;
      lastValue_ = secondWire_;
    }
    output[outputIndex++] = static_cast<physicalType>(lastValue_);
    ++row_;
  }

  // Batch-decode residuals. Bulk-extract via bulkGet64WithBaseline (uses
  // optimized template-unrolled paths for bitWidth <= 32 and branchless
  // loads for 33-57), then reconstruct values via prefix sum.
  if (row_ >= end) {
    return;
  }
  const uint32_t residualCount = end - row_;
  if (bitWidth_ == 0) {
    // All double-deltas are identical. Unroll the prefix sum without any
    // bitpacked extraction.
    const uint64_t doubleDelta = detail::zigzagDecode64(minZigzagDoubleDelta_);
    for (uint32_t i = 0; i < residualCount; ++i) {
      lastDelta_ += doubleDelta;
      lastValue_ += lastDelta_;
      output[outputIndex++] = static_cast<physicalType>(lastValue_);
    }
    row_ = end;
    return;
  }
  // Batch-extract residuals and prefix-sum reconstruct values.
  constexpr uint32_t kBatchSize{256};
  uint64_t zigzagBuf[kBatchSize];
  uint32_t remaining = residualCount;
  uint32_t residualStart = row_ - 2;
  while (remaining > 0) {
    const uint32_t batch = std::min(remaining, kBatchSize);
    fixedBitArray_.bulkGet64WithBaseline(
        /*start=*/residualStart,
        /*length=*/batch,
        zigzagBuf,
        /*baseline=*/minZigzagDoubleDelta_);
    for (uint32_t i = 0; i < batch; ++i) {
      const uint64_t doubleDelta = detail::zigzagDecode64(zigzagBuf[i]);
      lastDelta_ += doubleDelta;
      lastValue_ += lastDelta_;
      output[outputIndex++] = static_cast<physicalType>(lastValue_);
    }
    residualStart += batch;
    remaining -= batch;
  }
  row_ = end;
}

template <typename T>
template <typename V>
void DoubleDeltaEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&]() {
        if (row_ == 0) {
          lastValue_ = firstWire_;
        } else if (row_ == 1) {
          lastDelta_ = secondWire_ - firstWire_;
          lastValue_ = secondWire_;
        } else {
          advanceFromBitpacked(/*residualIndex=*/row_ - 2);
        }
        ++row_;
        return static_cast<physicalType>(lastValue_);
      });
}

template <typename T>
std::string_view DoubleDeltaEncoding<T>::encode(
    EncodingSelection<typename TypeTraits<T>::physicalType>& /*selectoin*/,
    std::span<const typename TypeTraits<T>::physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING(
        "DoubleDelta encoding cannot be used with 0 rows.");
  }
  const uint32_t rowCount = static_cast<uint32_t>(values.size());

  // Header fields that are always present. For N == 1 the second value is
  // zeroed; for N <= 2 the minZigzag and bitWidth are zeroed (no residuals).
  const uint64_t firstWire = static_cast<uint64_t>(values[0]);
  const uint64_t secondWire =
      rowCount >= 2 ? static_cast<uint64_t>(values[1]) : uint64_t{0};

  uint64_t minZigzag{0};
  uint8_t bitWidth{0};
  uint64_t bitpackedSize{0};

  // Pass 1: scan all double-deltas to determine the zigzag range and the
  // uniform bit width needed for frame-of-reference residuals.
  if (rowCount >= 3) {
    uint64_t minSeen{std::numeric_limits<uint64_t>::max()};
    uint64_t maxSeen{0};
    {
      uint64_t previousDelta = secondWire - firstWire;
      for (uint32_t i = 2; i < rowCount; ++i) {
        const uint64_t delta = static_cast<uint64_t>(values[i]) -
            static_cast<uint64_t>(values[i - 1]);
        const uint64_t doubleDelta = delta - previousDelta;
        const uint64_t zigzag = detail::zigzagEncode64(doubleDelta);
        minSeen = std::min(minSeen, zigzag);
        maxSeen = std::max(maxSeen, zigzag);
        previousDelta = delta;
      }
    }
    minZigzag = minSeen;
    const uint64_t maxResidual = maxSeen - minSeen;
    bitWidth = maxResidual == 0
        ? uint8_t{0}
        : static_cast<uint8_t>(velox::bits::bitsRequired(maxResidual));
    if (bitWidth > 0) {
      const uint32_t residualCount = rowCount - 2;
      bitpackedSize = FixedBitArray::bufferSize(residualCount, bitWidth);
    }
  }

  // Compute the number of forward-decode checkpoints. Skip checkpoints in
  // the bitWidth == 0 fast path to preserve the "constant-stride timestamps
  // encode to a few dozen bytes regardless of N" property -- the per-row
  // loop is trivially cheap when bitWidth == 0.
  const uint32_t checkpointCount = (rowCount >= 3 && bitWidth > 0)
      ? (rowCount - 2) / DoubleDeltaEncoding<T>::kCheckpointStride
      : uint32_t{0};
  const uint64_t checkpointSegmentSize = sizeof(uint32_t) +
      static_cast<uint64_t>(checkpointCount) * sizeof(Checkpoint);

  const uint32_t encodingSize =
      Encoding::serializePrefixSize(rowCount, useVarint) +
      DoubleDeltaEncoding<T>::kPrefixSize +
      static_cast<uint32_t>(bitpackedSize) +
      static_cast<uint32_t>(checkpointSegmentSize);

  // Serialize the encoding prefix and header fields.
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;
  Encoding::serializePrefix(
      EncodingType::DoubleDelta,
      TypeTraits<T>::dataType,
      rowCount,
      useVarint,
      pos);
  encoding::write<uint64_t>(firstWire, pos);
  encoding::write<uint64_t>(secondWire, pos);
  encoding::write<uint64_t>(minZigzag, pos);
  encoding::writeChar(static_cast<char>(bitWidth), pos);

  // Pass 2: bitpack residuals and capture checkpoints in a single O(N) walk
  // of the input values. Checkpoints are buffered locally and flushed after
  // the bitpacked region to maintain the wire layout:
  //   [bitpacked region][checkpointCount][checkpoint entries...]
  char* bitpackedStart = pos;
  const bool hasBitpacked = bitWidth > 0;
  if (hasBitpacked) {
    std::memset(bitpackedStart, 0, bitpackedSize);
  }
  FixedBitArray fixedBitArray =
      hasBitpacked ? FixedBitArray(bitpackedStart, bitWidth) : FixedBitArray{};

  std::vector<Checkpoint> capturedCheckpoints;
  capturedCheckpoints.reserve(checkpointCount);

  if (rowCount >= 3) {
    uint64_t previousDelta = secondWire - firstWire;
    uint32_t checkpointCounter = 0;
    for (auto i = 2; i < rowCount; ++i) {
      const auto currentValue = static_cast<uint64_t>(values[i]);
      const auto delta = currentValue - static_cast<uint64_t>(values[i - 1]);
      const auto doubleDelta = delta - previousDelta;
      if (hasBitpacked) {
        fixedBitArray.set(
            i - 2, detail::zigzagEncode64(doubleDelta) - minZigzag);
      }
      previousDelta = delta;

      // Capture a checkpoint every kCheckpointStride residuals. The
      // checkpoint stores the reconstructed value and delta at that point
      // so the decoder can resume from here without replaying earlier rows.
      if (hasBitpacked &&
          ++checkpointCounter == DoubleDeltaEncoding<T>::kCheckpointStride) {
        capturedCheckpoints.push_back({currentValue, delta});
        checkpointCounter = 0;
      }
    }
  }
  NIMBLE_DCHECK_EQ(
      capturedCheckpoints.size(),
      checkpointCount,
      "DoubleDelta checkpoint count mismatch.");

  // Flush the checkpoint segment after the bitpacked region.
  if (hasBitpacked) {
    pos += bitpackedSize;
  }
  encoding::write<uint32_t>(checkpointCount, pos);
  for (const auto& checkpoint : capturedCheckpoints) {
    encoding::write<uint64_t>(checkpoint.lastValue, pos);
    encoding::write<uint64_t>(checkpoint.lastDelta, pos);
  }

  NIMBLE_DCHECK_EQ(
      pos - reserved, encodingSize, "DoubleDelta encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string DoubleDeltaEncoding<T>::debugString(int offset) const {
  return fmt::format(
      "{}{}<{}> rowCount={} bitWidth={} checkpoints={}",
      std::string(offset, ' '),
      toString(Encoding::encodingType()),
      toString(Encoding::dataType()),
      Encoding::rowCount(),
      static_cast<int>(bitWidth_),
      checkpointCount_);
}

} // namespace facebook::nimble
