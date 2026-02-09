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
#include <cmath>
#include <span>
#include <unordered_map>
#include <vector>

#include "dwio/nimble/common/Buffer.h"
#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/EncodingType.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/common/Vector.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/EncodingSelection.h"
#include "folly/container/F14Map.h"
#include "velox/common/memory/Memory.h"

// Frequency partition encoding assigns shorter bit widths to more frequent
// values, similar to Huffman coding but with random access support. Values
// are partitioned by frequency into tiers (1-bit, 2-bit, 4-bit, etc.) and
// rows are reordered to group same-tier values together.
//
// Note: This encoding reorders rows. Original row order is not preserved.
//
// Data layout is:
// Encoding::kPrefixSize bytes: standard Encoding prefix
// 4 bytes: number of partitions
// XX bytes: partition offsets encoding (nested)
// YY bytes: partition sizes encoding (nested)
// For each tier (1-bit, 2-bit, 4-bit, 8-bit, 16-bit, 32-bit):
//   ZZ bytes: dictionary encoding (nested, if tier non-empty)
//   WW bytes: keys encoding (nested, if tier non-empty)
// VV bytes: unencoded values (nested, if any)

namespace facebook::nimble {

template <typename T>
class FrequencyPartitionEncoding
    : public TypedEncoding<T, typename TypeTraits<T>::physicalType> {
 public:
  using cppDataType = T;
  using physicalType = typename TypeTraits<T>::physicalType;

  static const int kNumPartitionsOffset = Encoding::kPrefixSize;

  FrequencyPartitionEncoding(
      velox::memory::MemoryPool& pool,
      std::string_view data,
      std::function<void*(uint32_t)> stringBufferFactory = nullptr);

  void reset() final;
  void skip(uint32_t rowCount) final;
  void materialize(uint32_t rowCount, void* buffer) final;

  template <typename DecoderVisitor>
  void readWithVisitor(DecoderVisitor& visitor, ReadWithVisitorParams& params);

  static std::string_view encode(
      EncodingSelection<physicalType>& selection,
      std::span<const physicalType> values,
      Buffer& buffer);

  std::string debugString(int offset) const final;

 private:
  struct TierInfo {
    uint32_t keyBits;
    uint32_t capacity;
    Vector<T> dictionary;
    Vector<uint32_t> indices;
    uint32_t startRow;
    uint32_t size;

    TierInfo(velox::memory::MemoryPool* pool)
        : keyBits(0),
          capacity(0),
          dictionary(pool),
          indices(pool),
          startRow(0),
          size(0) {}
  };

  // Get capacity for a given key bit width
  static constexpr uint32_t getCapacity(uint32_t keyBits) {
    return (keyBits == 1)  ? 2
        : (keyBits == 2)   ? 4
        : (keyBits == 4)   ? 16
        : (keyBits == 8)   ? 256
        : (keyBits == 16)  ? (65536 - 256)
        : (keyBits == 32)  ? (4294967296ULL - 65536)
                           : 0;
  }

  static constexpr uint32_t getMaxKeyBits() {
    constexpr size_t valueSize =
        std::is_same_v<T, std::string_view> ? sizeof(int64_t) : sizeof(T);
    constexpr size_t valueBits = valueSize * 8;

    if constexpr (valueBits <= 8) {
      return 4; // For 1-byte values, can use up to 4-bit keys
    } else if constexpr (valueBits <= 16) {
      return 8; // For 2-byte values, can use up to 8-bit keys
    } else if constexpr (valueBits <= 32) {
      return 16; // For 4-byte values, can use up to 16-bit keys
    } else {
      return 32; // For 8-byte values, can use up to 32-bit keys
    }
  }

  std::vector<TierInfo> tiers_;

  Vector<T> unencodedValues_;
  uint32_t unencodedStartRow_;

  uint32_t currentTier_;
  uint32_t currentTierOffset_;

  uint32_t getTierForRow(uint32_t rowIndex) const;
};

//
// End of public API. Implementation follows.
//

template <typename T>
FrequencyPartitionEncoding<T>::FrequencyPartitionEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> /* stringBufferFactory */)
    : TypedEncoding<T, physicalType>{pool, data},
      unencodedValues_{this->pool_},
      unencodedStartRow_(0),
      currentTier_(0),
      currentTierOffset_(0) {
  const auto* pos = data.data() + kNumPartitionsOffset;
  const uint32_t numPartitions = encoding::readUint32(pos);

  const uint32_t partitionOffsetsSize = encoding::readUint32(pos);
  auto partitionOffsetsEncoding =
    EncodingFactory::decode(
      *this->pool_, std::string_view(pos, partitionOffsetsSize),
      [](uint32_t /*size*/) -> void* { return nullptr; });
  pos += partitionOffsetsSize;

  const uint32_t partitionSizesSize = encoding::readUint32(pos);
  auto partitionSizesEncoding =
    EncodingFactory::decode(
      *this->pool_, std::string_view(pos, partitionSizesSize),
      [](uint32_t /*size*/) -> void* { return nullptr; });
  pos += partitionSizesSize;

  Vector<uint32_t> partitionOffsets{this->pool_};
  Vector<uint32_t> partitionSizes{this->pool_};
  partitionOffsets.resize(numPartitions);
  partitionSizes.resize(numPartitions);
  partitionOffsetsEncoding->materialize(numPartitions, partitionOffsets.data());
  partitionSizesEncoding->materialize(numPartitions, partitionSizes.data());

  constexpr uint32_t keyBitOptions[] = {1, 2, 4, 8, 16, 32};
  constexpr uint32_t maxKeyBits = getMaxKeyBits();

  const uint32_t numCodedTiers = (numPartitions > 0) ? numPartitions - 1 : 0;

  for (uint32_t i = 0; i < numPartitions; ++i) {
    if (i < numCodedTiers) {
      TierInfo tier(this->pool_);
      tier.keyBits = keyBitOptions[i];
      tier.capacity = getCapacity(tier.keyBits);
      tier.startRow = partitionOffsets[i];
      tier.size = partitionSizes[i];

      if (tier.size > 0) {
        const uint32_t dictSize = encoding::readUint32(pos);
    auto dictEncoding =
      EncodingFactory::decode(
        *this->pool_, std::string_view(pos, dictSize),
        [](uint32_t /*size*/) -> void* { return nullptr; });
        pos += dictSize;

        const uint32_t dictCount = dictEncoding->rowCount();
        tier.dictionary.resize(dictCount);
        dictEncoding->materialize(dictCount, tier.dictionary.data());

        const uint32_t keysSize = encoding::readUint32(pos);
    auto keysEncoding =
      EncodingFactory::decode(
        *this->pool_, std::string_view(pos, keysSize),
        [](uint32_t /*size*/) -> void* { return nullptr; });
        pos += keysSize;

        tier.indices.resize(tier.size);
        keysEncoding->materialize(tier.size, tier.indices.data());
      }

      tiers_.push_back(std::move(tier));
    } else {
      unencodedStartRow_ = partitionOffsets[i];
      const uint32_t unencodedSize = partitionSizes[i];

      if (unencodedSize > 0) {
        const uint32_t valuesSize = encoding::readUint32(pos);
    auto valuesEncoding =
      EncodingFactory::decode(
        *this->pool_, std::string_view(pos, valuesSize),
        [](uint32_t /*size*/) -> void* { return nullptr; });
        pos += valuesSize;

        unencodedValues_.resize(unencodedSize);
        valuesEncoding->materialize(unencodedSize, unencodedValues_.data());
      }
    }
  }
}

template <typename T>
void FrequencyPartitionEncoding<T>::reset() {
  currentTier_ = 0;
  currentTierOffset_ = 0;
}

template <typename T>
uint32_t FrequencyPartitionEncoding<T>::getTierForRow(uint32_t rowIndex) const {
  for (uint32_t i = 0; i < tiers_.size(); ++i) {
    if (rowIndex >= tiers_[i].startRow &&
        rowIndex < tiers_[i].startRow + tiers_[i].size) {
      return i;
    }
  }
  return tiers_.size();
}

template <typename T>
void FrequencyPartitionEncoding<T>::skip(uint32_t rowCount) {
  uint32_t remaining = rowCount;
  while (remaining > 0 && currentTier_ <= tiers_.size()) {
    if (currentTier_ < tiers_.size()) {
      const auto& tier = tiers_[currentTier_];
      const uint32_t availableInTier = tier.size - currentTierOffset_;
      const uint32_t toSkip = std::min(remaining, availableInTier);
      currentTierOffset_ += toSkip;
      remaining -= toSkip;

      if (currentTierOffset_ >= tier.size) {
        ++currentTier_;
        currentTierOffset_ = 0;
      }
    } else {
      break;
    }
  }
}

template <typename T>
void FrequencyPartitionEncoding<T>::materialize(
    uint32_t rowCount,
    void* buffer) {
  T* output = static_cast<T*>(buffer);
  uint32_t remaining = rowCount;
  uint32_t outputIdx = 0;

  while (remaining > 0) {
    if (currentTier_ < tiers_.size()) {
      const auto& tier = tiers_[currentTier_];
      const uint32_t availableInTier = tier.size - currentTierOffset_;
      const uint32_t toRead = std::min(remaining, availableInTier);

      for (uint32_t i = 0; i < toRead; ++i) {
        const uint32_t index = tier.indices[currentTierOffset_ + i];
        output[outputIdx++] = tier.dictionary[index];
      }

      currentTierOffset_ += toRead;
      remaining -= toRead;

      if (currentTierOffset_ >= tier.size) {
        ++currentTier_;
        currentTierOffset_ = 0;
      }
    } else if (currentTier_ == tiers_.size()) {
      const uint32_t availableUnencoded = 
          static_cast<uint32_t>(unencodedValues_.size()) - currentTierOffset_;
      const uint32_t toRead = std::min(remaining, availableUnencoded);
      
      for (uint32_t i = 0; i < toRead; ++i) {
        output[outputIdx++] = unencodedValues_[currentTierOffset_ + i];
      }
      
      currentTierOffset_ += toRead;
      remaining -= toRead;
      
      if (currentTierOffset_ >= unencodedValues_.size()) {
        ++currentTier_; // Move past unencoded partition
        currentTierOffset_ = 0;
      }
    } else {
      break;
    }
  }
}

template <typename T>
template <typename V>
void FrequencyPartitionEncoding<T>::readWithVisitor(
    V& visitor,
    ReadWithVisitorParams& params) {
  detail::readWithVisitorSlow(visitor, params, nullptr, [&] {
    uint32_t absoluteRow = visitor.rowIndex();
    uint32_t tier = getTierForRow(absoluteRow);

    if (tier < tiers_.size()) {
      const auto& tierInfo = tiers_[tier];
      uint32_t relativeRow = absoluteRow - tierInfo.startRow;
      uint32_t index = tierInfo.indices[relativeRow];
      return tierInfo.dictionary[index];
    } else {
      // Unencoded partition
      uint32_t relativeRow = absoluteRow - unencodedStartRow_;
      return unencodedValues_[relativeRow];
    }
  });
}

template <typename T>
std::string_view FrequencyPartitionEncoding<T>::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer) {
  const uint32_t valueCount = values.size();

  // Build frequency map
  folly::F14FastMap<physicalType, uint32_t> frequencyMap;
  for (const auto& value : values) {
    frequencyMap[value]++;
  }

  // Sort by frequency (descending)
  std::vector<std::pair<physicalType, uint32_t>> freqVec;
  freqVec.reserve(frequencyMap.size());
  for (const auto& [value, freq] : frequencyMap) {
    freqVec.emplace_back(value, freq);
  }
  std::sort(
      freqVec.begin(),
      freqVec.end(),
      [](const auto& a, const auto& b) { return a.second > b.second; });

  const uint32_t uniqueCount = freqVec.size();
  constexpr uint32_t maxKeyBits = getMaxKeyBits();

  // Early exit if we can't achieve compression
  if constexpr (maxKeyBits == 0) {
    return {}; // Can't compress this type
  }

  // Calculate tier assignments
  struct TierAssignment {
    uint32_t keyBits;
    uint32_t capacity;
    Vector<physicalType> dictionary;
    folly::F14FastMap<physicalType, uint32_t> valueToKey;

    explicit TierAssignment(velox::memory::MemoryPool& pool)
        : keyBits(0), capacity(0), dictionary(&pool) {}
  };

  std::vector<TierAssignment> tierAssignments;
  tierAssignments.reserve(6); // Max 6 tiers (1, 2, 4, 8, 16, 32 bits)

  uint32_t valuesAssigned = 0;
  constexpr uint32_t keyBitOptions[] = {1, 2, 4, 8, 16, 32};

  for (uint32_t keyBits : keyBitOptions) {
    if (keyBits > maxKeyBits || valuesAssigned >= uniqueCount) {
      break;
    }

    TierAssignment tier{buffer.getMemoryPool()};
    tier.keyBits = keyBits;
    tier.capacity = getCapacity(keyBits);

    const uint32_t numToAssign =
        std::min(tier.capacity, uniqueCount - valuesAssigned);
    tier.dictionary.reserve(numToAssign);

    for (uint32_t i = 0; i < numToAssign; ++i) {
      const auto& value = freqVec[valuesAssigned + i].first;
      tier.dictionary.push_back(value);
      tier.valueToKey[value] = i;
    }

    valuesAssigned += numToAssign;
    tierAssignments.push_back(std::move(tier));
  }

  // Map remaining values to unencoded partition
  folly::F14FastSet<physicalType> unencodedSet;
  for (uint32_t i = valuesAssigned; i < uniqueCount; ++i) {
    unencodedSet.insert(freqVec[i].first);
  }

  // Build tier assignment for each row and collect rows per partition
  std::vector<std::vector<uint32_t>> tierRows(tierAssignments.size() + 1);
  for (auto& vec : tierRows) {
    vec.reserve(valueCount / tierRows.size()); // Rough estimate
  }

  // Assign rows to tiers
  for (uint32_t row = 0; row < valueCount; ++row) {
    const auto& value = values[row];
    bool found = false;

    for (size_t tierIdx = 0; tierIdx < tierAssignments.size(); ++tierIdx) {
      if (tierAssignments[tierIdx].valueToKey.find(value) !=
          tierAssignments[tierIdx].valueToKey.end()) {
        tierRows[tierIdx].push_back(row);
        found = true;
        break;
      }
    }

    if (!found) {
      // Unencoded partition
      tierRows.back().push_back(row);
    }
  }

  // Calculate partition offsets and sizes
  Vector<uint32_t> partitionOffsets(&buffer.getMemoryPool());
  Vector<uint32_t> partitionSizes(&buffer.getMemoryPool());
  partitionOffsets.reserve(tierRows.size());
  partitionSizes.reserve(tierRows.size());

  uint32_t offset = 0;
  for (const auto& rows : tierRows) {
    partitionOffsets.push_back(offset);
    partitionSizes.push_back(rows.size());
    offset += rows.size();
  }

  // Encode partition metadata
  Buffer tempBuffer{buffer.getMemoryPool()};
  std::string_view serializedOffsets =
      selection.template encodeNested<uint32_t>(
          EncodingIdentifiers::FrequencyPartition::PartitionOffsets,
          {partitionOffsets},
          tempBuffer);
  std::string_view serializedSizes =
      selection.template encodeNested<uint32_t>(
          EncodingIdentifiers::FrequencyPartition::PartitionSizes,
          {partitionSizes},
          tempBuffer);

  // Encode each tier
  std::vector<std::string_view> serializedDicts;
  std::vector<std::string_view> serializedKeys;

  for (size_t tierIdx = 0; tierIdx < tierAssignments.size(); ++tierIdx) {
    const auto& tier = tierAssignments[tierIdx];
    const auto& rows = tierRows[tierIdx];

    if (rows.empty()) {
      serializedDicts.push_back({});
      serializedKeys.push_back({});
      continue;
    }

    // Encode dictionary
    serializedDicts.push_back(selection.template encodeNested<physicalType>(
        EncodingIdentifiers::FrequencyPartition::Dict1Bit + tierIdx,
        {tier.dictionary},
        tempBuffer));

    // Build keys for this tier
    Vector<uint32_t> keys(&buffer.getMemoryPool());
    keys.reserve(rows.size());
    for (uint32_t row : rows) {
      const auto& value = values[row];
      keys.push_back(tier.valueToKey.at(value));
    }

    serializedKeys.push_back(selection.template encodeNested<uint32_t>(
        EncodingIdentifiers::FrequencyPartition::Keys1Bit + tierIdx,
        {keys},
        tempBuffer));
  }

  // Encode unencoded partition
  std::string_view serializedUnencoded;
  if (!tierRows.back().empty()) {
    Vector<physicalType> unencodedValues(&buffer.getMemoryPool());
    unencodedValues.reserve(tierRows.back().size());
    for (uint32_t row : tierRows.back()) {
      unencodedValues.push_back(values[row]);
    }
    serializedUnencoded = selection.template encodeNested<physicalType>(
        EncodingIdentifiers::FrequencyPartition::UnencodedValues,
        {unencodedValues},
        tempBuffer);
  }

  // Calculate total encoding size
  uint32_t encodingSize = Encoding::kPrefixSize + 4 + // num partitions
      4 + serializedOffsets.size() + // partition offsets
      4 + serializedSizes.size(); // partition sizes

  for (const auto& dict : serializedDicts) {
    if (!dict.empty()) {
      encodingSize += 4 + dict.size();
    }
  }
  for (const auto& keys : serializedKeys) {
    if (!keys.empty()) {
      encodingSize += 4 + keys.size();
    }
  }
  if (!serializedUnencoded.empty()) {
    encodingSize += 4 + serializedUnencoded.size();
  }

  // Write encoded data
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  Encoding::serializePrefix(
      EncodingType::FrequencyPartition,
      TypeTraits<T>::dataType,
      valueCount,
      pos);
  encoding::writeUint32(tierRows.size(), pos); // num partitions
  encoding::writeUint32(serializedOffsets.size(), pos);
  encoding::writeBytes(serializedOffsets, pos);
  encoding::writeUint32(serializedSizes.size(), pos);
  encoding::writeBytes(serializedSizes, pos);

  // Write dictionary and keys for each tier together
  for (size_t i = 0; i < serializedDicts.size(); ++i) {
    if (!serializedDicts[i].empty()) {
      encoding::writeUint32(serializedDicts[i].size(), pos);
      encoding::writeBytes(serializedDicts[i], pos);
      encoding::writeUint32(serializedKeys[i].size(), pos);
      encoding::writeBytes(serializedKeys[i], pos);
    }
  }
  if (!serializedUnencoded.empty()) {
    encoding::writeUint32(serializedUnencoded.size(), pos);
    encoding::writeBytes(serializedUnencoded, pos);
  }

  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch.");
  return {reserved, encodingSize};
}

template <typename T>
std::string FrequencyPartitionEncoding<T>::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}tiers={}, unencoded_rows={}",
      std::string(offset, ' '),
      tiers_.size(),
      unencodedValues_.size());

  for (size_t i = 0; i < tiers_.size(); ++i) {
    const auto& tier = tiers_[i];
    log += fmt::format(
        "\n{}tier[{}]: {}bit codes, {} unique values, {} rows",
        std::string(offset + 2, ' '),
        i,
        tier.keyBits,
        tier.dictionary.size(),
        tier.size);
  }

  return log;
}

} // namespace facebook::nimble
