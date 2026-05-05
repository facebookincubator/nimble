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
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "dwio/nimble/encodings/SubIntSplitConfig.h"
#include "dwio/nimble/encodings/SubIntSplitSampler.h"
#include "dwio/nimble/encodings/SubIntSplitSelector.h"
#include "velox/common/memory/Memory.h"

// SubIntSplitEncoding: decomposes each value in a 32- or 64-bit integer stream
// into bit-range sub-streams, selects an optimal encoding for each sub-stream
// via a sample-driven DP algorithm, and stitches the encoded sub-streams back
// together for efficient decoding.
//
// Only supported for 32- and 64-bit types (int32_t, uint32_t, int64_t,
// uint64_t, float, double). The physical type for float is uint32_t and for
// double is uint64_t; bit patterns are preserved across encode/decode.
//
// Each section is encoded as the narrowest unsigned integer type that fits its
// bit width (uint8_t for 1-8 bits, uint16_t for 9-16, uint32_t for 17-32,
// uint64_t for 33-64). This avoids paying an 8-byte-per-value penalty for
// narrow sections when they land in e.g. Dictionary or Trivial encoding.
//
// Binary layout (after the standard Encoding prefix):
//   [1 byte]  splitCount (number of sections, 1..64)
//   [1 byte]  reserved (future: BitSplitOrder; currently 0)
//   [splitCount × 6 bytes]  {bitStart(1B), bitEnd(1B), encodedSize(4B)}
//   [section_0_bytes][section_1_bytes]...[section_{N-1}_bytes]
//
// Sections are stored in LSB-first order (section 0 covers the lowest bits).
// Section identifiers equal the section index (0, 1, …, splitCount-1).

namespace facebook::nimble {

template <typename T>
class SubIntSplitEncoding
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static_assert(
      sizeof(physicalType) == 4 || sizeof(physicalType) == 8,
      "SubIntSplitEncoding only supports 32- and 64-bit types");
  static_assert(
      isNumericType<physicalType>(),
      "SubIntSplitEncoding only supports numeric types");

  SubIntSplitEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory,
      const Encoding::Options& options = {});

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer,
      const Encoding::Options& options = {});

  std::string debugString(int offset) const final;

 private:
  struct SectionInfo {
    int bitStart{0};
    int bitEnd{0};
    uint64_t mask{0}; // (1 << width) - 1, or ~0 for full 64-bit section
    uint8_t storageBytes{8}; // 1, 2, 4, or 8 — matches the section's DataType
    std::unique_ptr<Encoding> encoding;
  };

  std::vector<SectionInfo> sections_;

  // Return the storage byte width for a section of the given bit width.
  static constexpr uint8_t sectionStorageBytes(int bitWidth) noexcept {
    if (bitWidth <= 8) return 1;
    if (bitWidth <= 16) return 2;
    if (bitWidth <= 32) return 4;
    return 8;
  }
};

//
// End of public API. Implementation follows.
//

namespace detail {
inline constexpr uint32_t kSubIntSplitSectionHeaderSize = 6; // bitStart+bitEnd+size

inline uint32_t subIntSplitSpecificHeaderSize(uint8_t splitCount) noexcept {
  return 2u + static_cast<uint32_t>(splitCount) * kSubIntSplitSectionHeaderSize;
}
} // namespace detail

template <typename T>
SubIntSplitEncoding<T>::SubIntSplitEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<T, physicalType>{pool, data, options},
      sections_{} {
  const EncodingFactory factory{options};
  const auto* pos = data.data() + this->dataOffset();

  const uint8_t splitCount = encoding::read<uint8_t>(pos);
  encoding::read<uint8_t>(pos); // reserved order byte

  struct SectionMeta {
    uint8_t bitStart;
    uint8_t bitEnd;
    uint32_t encodedSize;
  };
  std::vector<SectionMeta> meta(splitCount);
  for (uint8_t s = 0; s < splitCount; ++s) {
    meta[s].bitStart = encoding::read<uint8_t>(pos);
    meta[s].bitEnd = encoding::read<uint8_t>(pos);
    meta[s].encodedSize = encoding::readUint32(pos);
  }

  sections_.resize(splitCount);
  for (uint8_t s = 0; s < splitCount; ++s) {
    auto& sec = sections_[s];
    sec.bitStart = meta[s].bitStart;
    sec.bitEnd = meta[s].bitEnd;
    const int width = sec.bitEnd - sec.bitStart + 1;
    sec.mask = (width >= 64) ? ~uint64_t{0} : ((uint64_t{1} << width) - 1);
    sec.storageBytes = sectionStorageBytes(width);
    sec.encoding = factory.create(
        *this->pool_,
        {pos, meta[s].encodedSize},
        stringBufferFactory);
    pos += meta[s].encodedSize;
  }
}

template <typename T>
void SubIntSplitEncoding<T>::reset() {
  for (auto& sec : sections_) {
    sec.encoding->reset();
  }
}

template <typename T>
void SubIntSplitEncoding<T>::skip(uint32_t rowCount) {
  for (auto& sec : sections_) {
    sec.encoding->skip(rowCount);
  }
}

template <typename T>
void SubIntSplitEncoding<T>::materialize(uint32_t rowCount, void* buffer) {
  physicalType* output = static_cast<physicalType*>(buffer);
  std::fill(output, output + rowCount, physicalType{0});

  for (const auto& sec : sections_) {
    const int shift = sec.bitStart;
    const uint64_t mask = sec.mask;

    switch (sec.storageBytes) {
      case 1: {
        Vector<uint8_t> sectionValues{this->pool_};
        sectionValues.resize(rowCount);
        sec.encoding->materialize(rowCount, sectionValues.data());
        for (uint32_t i = 0; i < rowCount; ++i) {
          output[i] |= static_cast<physicalType>(sectionValues[i] & mask)
              << shift;
        }
        break;
      }
      case 2: {
        Vector<uint16_t> sectionValues{this->pool_};
        sectionValues.resize(rowCount);
        sec.encoding->materialize(rowCount, sectionValues.data());
        for (uint32_t i = 0; i < rowCount; ++i) {
          output[i] |= static_cast<physicalType>(sectionValues[i] & mask)
              << shift;
        }
        break;
      }
      case 4: {
        Vector<uint32_t> sectionValues{this->pool_};
        sectionValues.resize(rowCount);
        sec.encoding->materialize(rowCount, sectionValues.data());
        for (uint32_t i = 0; i < rowCount; ++i) {
          output[i] |= static_cast<physicalType>(sectionValues[i] & mask)
              << shift;
        }
        break;
      }
      case 8: {
        Vector<uint64_t> sectionValues{this->pool_};
        sectionValues.resize(rowCount);
        sec.encoding->materialize(rowCount, sectionValues.data());
        for (uint32_t i = 0; i < rowCount; ++i) {
          output[i] |= static_cast<physicalType>(sectionValues[i] & mask)
              << shift;
        }
        break;
      }
      default: {
        NIMBLE_UNREACHABLE("Invalid SubIntSplit section storage width.");
      }
    }
  }
}

template <typename T>
template <typename DecoderVisitor>
void SubIntSplitEncoding<T>::readWithVisitor(
    DecoderVisitor& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(
      visitor,
      params,
      [&](auto toSkip) { skip(toSkip); },
      [&] {
        physicalType value = 0;
        for (const auto& sec : sections_) {
          switch (sec.storageBytes) {
            case 1: {
              uint8_t sectionValue = 0;
              sec.encoding->materialize(1, &sectionValue);
              value |= static_cast<physicalType>(sectionValue & sec.mask)
                  << sec.bitStart;
              break;
            }
            case 2: {
              uint16_t sectionValue = 0;
              sec.encoding->materialize(1, &sectionValue);
              value |= static_cast<physicalType>(sectionValue & sec.mask)
                  << sec.bitStart;
              break;
            }
            case 4: {
              uint32_t sectionValue = 0;
              sec.encoding->materialize(1, &sectionValue);
              value |= static_cast<physicalType>(sectionValue & sec.mask)
                  << sec.bitStart;
              break;
            }
            case 8: {
              uint64_t sectionValue = 0;
              sec.encoding->materialize(1, &sectionValue);
              value |= static_cast<physicalType>(sectionValue & sec.mask)
                  << sec.bitStart;
              break;
            }
            default: {
              NIMBLE_UNREACHABLE("Invalid SubIntSplit section storage width.");
            }
          }
        }
        return value;
      });
}

template <typename T>
std::string_view SubIntSplitEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  const uint32_t valueCount = static_cast<uint32_t>(values.size());

  if (values.empty()) {
    NIMBLE_INCOMPATIBLE_ENCODING("SubIntSplitEncoding cannot be empty.");
  }

  constexpr int kBits = static_cast<int>(sizeof(physicalType) * 8);

    std::vector<detail::subintsplit::SegmentPlan> segments;
    const auto modeConfig = selection.getConfig(
      std::string(detail::subintsplit::kSplitModeConfigKey));
    if (modeConfig.has_value() &&
      *modeConfig == detail::subintsplit::kSplitModePreserve) {
    const auto boundaryConfig = selection.getConfig(
      std::string(detail::subintsplit::kSplitBoundariesConfigKey));
    NIMBLE_CHECK(
      boundaryConfig.has_value(),
      "SubIntSplit preserve mode requires boundaries config.");
    auto parsed = detail::subintsplit::parseSplitBoundaries(*boundaryConfig, kBits);
    NIMBLE_CHECK(
      parsed.has_value(),
      "Invalid SubIntSplit boundaries config.");
    segments = std::move(parsed.value());
    } else {
    // Default behavior: recompute the split boundaries from the sampled data.
    std::vector<uint64_t> sampleBuf;
    detail::subintsplit::sampleIntoU64<physicalType>(
      values,
      sampleBuf,
      detail::subintsplit::defaultSamplerConfig());

    auto selectorResult = detail::subintsplit::selectSplits(
      sampleBuf,
      kBits,
      valueCount,
      detail::subintsplit::defaultSelectorConfig());

    segments = std::move(selectorResult.segments);
    }

  NIMBLE_CHECK(
      !segments.empty(), "SubIntSplitEncoding: selector returned no segments");
  const uint8_t splitCount = static_cast<uint8_t>(segments.size());

  // --- Step 2: Encode each section into a temporary buffer ---
  // Each section is encoded as the narrowest unsigned integer type that fits
  // its bit width, so narrow sections don't pay an 8-byte-per-value penalty.
  Buffer tempBuffer{buffer.getMemoryPool()};
  std::vector<std::string_view> sectionData;
  sectionData.reserve(splitCount);

  for (uint8_t s = 0; s < splitCount; ++s) {
    const auto& seg = segments[s];
    const int bitStart = seg.bitStart;
    const int bitEnd = seg.bitEnd;
    const int width = bitEnd - bitStart + 1;
    const uint64_t mask =
        (width >= 64) ? ~uint64_t{0} : ((uint64_t{1} << width) - 1);
    const uint8_t sb = sectionStorageBytes(width);

    std::string_view encoded;
    switch (sb) {
      case 1: {
        Vector<uint8_t> sectionValues{&tempBuffer.getMemoryPool()};
        sectionValues.resize(valueCount);
        for (uint32_t i = 0; i < valueCount; ++i) {
          uint64_t v = 0;
          __builtin_memcpy(&v, &values[i], sizeof(physicalType));
          sectionValues[i] = static_cast<uint8_t>((v >> bitStart) & mask);
        }
        encoded = selection.template encodeNested<uint8_t>(
            static_cast<NestedEncodingIdentifier>(s),
            std::span<const uint8_t>(sectionValues.data(), sectionValues.size()),
            tempBuffer,
            options);
        break;
      }
      case 2: {
        Vector<uint16_t> sectionValues{&tempBuffer.getMemoryPool()};
        sectionValues.resize(valueCount);
        for (uint32_t i = 0; i < valueCount; ++i) {
          uint64_t v = 0;
          __builtin_memcpy(&v, &values[i], sizeof(physicalType));
          sectionValues[i] = static_cast<uint16_t>((v >> bitStart) & mask);
        }
        encoded = selection.template encodeNested<uint16_t>(
            static_cast<NestedEncodingIdentifier>(s),
            std::span<const uint16_t>(
                sectionValues.data(), sectionValues.size()),
            tempBuffer,
            options);
        break;
      }
      case 4: {
        Vector<uint32_t> sectionValues{&tempBuffer.getMemoryPool()};
        sectionValues.resize(valueCount);
        for (uint32_t i = 0; i < valueCount; ++i) {
          uint64_t v = 0;
          __builtin_memcpy(&v, &values[i], sizeof(physicalType));
          sectionValues[i] = static_cast<uint32_t>((v >> bitStart) & mask);
        }
        encoded = selection.template encodeNested<uint32_t>(
            static_cast<NestedEncodingIdentifier>(s),
            std::span<const uint32_t>(
                sectionValues.data(), sectionValues.size()),
            tempBuffer,
            options);
        break;
      }
      case 8: {
        Vector<uint64_t> sectionValues{&tempBuffer.getMemoryPool()};
        sectionValues.resize(valueCount);
        for (uint32_t i = 0; i < valueCount; ++i) {
          uint64_t v = 0;
          __builtin_memcpy(&v, &values[i], sizeof(physicalType));
          sectionValues[i] = (v >> bitStart) & mask;
        }
        encoded = selection.template encodeNested<uint64_t>(
            static_cast<NestedEncodingIdentifier>(s),
            std::span<const uint64_t>(
                sectionValues.data(), sectionValues.size()),
            tempBuffer,
            options);
        break;
      }
      default: {
        NIMBLE_UNREACHABLE("Invalid SubIntSplit section storage width.");
      }
    }
    sectionData.push_back(encoded);
  }

  // --- Step 3: Write final encoding to main buffer ---
  const uint32_t prefixSize =
      Encoding::serializePrefixSize(valueCount, useVarint);
  const uint32_t specificHeader =
      detail::subIntSplitSpecificHeaderSize(splitCount);
  uint32_t sectionsSize = 0;
  for (const auto& sv : sectionData) {
    sectionsSize += static_cast<uint32_t>(sv.size());
  }
  const uint32_t encodingSize = prefixSize + specificHeader + sectionsSize;

  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  Encoding::serializePrefix(
      EncodingType::SubIntSplit,
      TypeTraits<T>::dataType,
      valueCount,
      useVarint,
      pos);

  encoding::write<uint8_t>(splitCount, pos);
  encoding::write<uint8_t>(uint8_t{0}, pos); // reserved / order

  for (uint8_t s = 0; s < splitCount; ++s) {
    const auto& seg = segments[s];
    encoding::write<uint8_t>(static_cast<uint8_t>(seg.bitStart), pos);
    encoding::write<uint8_t>(static_cast<uint8_t>(seg.bitEnd), pos);
    encoding::writeUint32(static_cast<uint32_t>(sectionData[s].size()), pos);
  }
  for (const auto& sv : sectionData) {
    encoding::writeBytes(sv, pos);
  }

  NIMBLE_DCHECK_EQ(
      static_cast<uint32_t>(pos - reserved),
      encodingSize,
      "SubIntSplitEncoding: encoding size mismatch");

  return {reserved, encodingSize};
}

template <typename T>
std::string SubIntSplitEncoding<T>::debugString(int offset) const {
  std::string indent(offset, ' ');
  std::string result = indent + "SubIntSplitEncoding sections=" +
      std::to_string(sections_.size()) + "\n";
  for (size_t s = 0; s < sections_.size(); ++s) {
    const auto& sec = sections_[s];
    result += indent + "  [" + std::to_string(sec.bitStart) + ".." +
        std::to_string(sec.bitEnd) + "] storageBytes=" +
        std::to_string(sec.storageBytes) + "\n";
    result += sec.encoding->debugString(offset + 4);
    result += "\n";
  }
  return result;
}

} // namespace facebook::nimble
