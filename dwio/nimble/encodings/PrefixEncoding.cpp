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
#include "dwio/nimble/encodings/PrefixEncoding.h"

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble {

PrefixEncoding::PrefixEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data)
    : TypedEncoding<std::string_view, std::string_view>{pool, data},
      restartInterval_{readRestartInterval(data)},
      numRestarts_{computeNumRestarts(rowCount_, restartInterval_)},
      restartOffsets_{restartOffsets(data)},
      dataStart_{dataStart(data, numRestarts_)},
      decodedValue_{pool_},
      materializedValues_{pool_} {
  reset();
}

// static
uint32_t PrefixEncoding::readRestartInterval(std::string_view data) {
  const auto* pos = data.data() + kRestartIntervalOffset;
  return encoding::readUint32(pos);
}

// static
uint32_t PrefixEncoding::computeNumRestarts(
    uint32_t rowCount,
    uint32_t restartInterval) {
  return velox::bits::divRoundUp(rowCount, restartInterval);
}

// static
const char* PrefixEncoding::restartOffsets(std::string_view data) {
  return data.data() + kRestartOffsetsOffset;
}

// static
const char* PrefixEncoding::dataStart(
    std::string_view data,
    uint32_t numRestarts) {
  return data.data() + kRestartOffsetsOffset + (numRestarts * sizeof(uint32_t));
}

void PrefixEncoding::reset() {
  currentPos_ = dataStart_;
  currentRow_ = 0;
  decodedValue_.clear();
}

void PrefixEncoding::skip(uint32_t rowCount) {
  NIMBLE_CHECK_LE(currentRow_ + rowCount, rowCount_, "Invalid skip row count");

  if (rowCount == 0) {
    return;
  }

  const uint32_t targetRow = currentRow_ + rowCount;

  // Calculate which restart block we're currently in and which we need to reach
  const uint32_t currentRestartIndex = currentRow_ / restartInterval_;
  const uint32_t targetRestartIndex = targetRow / restartInterval_;
  NIMBLE_CHECK_LE(currentRestartIndex, targetRestartIndex);

  // If target is in a different restart block, jump to the appropriate restart
  // point
  if (targetRestartIndex > currentRestartIndex &&
      targetRestartIndex < numRestarts_) {
    seekToRestartPoint(targetRestartIndex);
  }

  // Linear scan to reach exact target row
  while (currentRow_ < targetRow) {
    decodeEntry();
  }
}

std::string_view PrefixEncoding::decodeEntry() {
  // Decode shared prefix length
  const uint32_t sharedPrefixLen = encoding::readUint32(currentPos_);
  // Decode suffix length
  const uint32_t suffixLen = encoding::readUint32(currentPos_);

  // The shared prefix is already at the beginning of decodedValue_ from the
  // previous decode. We just need to resize and append the suffix.
  const uint32_t fullLen = sharedPrefixLen + suffixLen;
  NIMBLE_CHECK_LE(
      sharedPrefixLen, decodedValue_.size(), "Invalid shared prefix length");
  decodedValue_.resize(fullLen);

  // Copy suffix
  if (suffixLen > 0) {
    std::memcpy(decodedValue_.data() + sharedPrefixLen, currentPos_, suffixLen);
    currentPos_ += suffixLen;
  }
  ++currentRow_;
  return std::string_view(decodedValue_.data(), fullLen);
}

void PrefixEncoding::materialize(uint32_t rowCount, void* buffer) {
  NIMBLE_CHECK_LE(currentRow_ + rowCount, rowCount_, "Invalid row count");

  // Clear the buffer from previous materialize call
  materializedValues_.clear();

  // First pass: decode all entries and accumulate total size needed
  // Store offsets and lengths for each entry
  Vector<std::pair<uint32_t, uint32_t>> valueOffsets{pool_};
  valueOffsets.reserve(rowCount);

  for (uint32_t i = 0; i < rowCount; ++i) {
    const std::string_view decodedValue = decodeEntry();
    const uint32_t valueOffset = materializedValues_.size();
    const uint32_t valueLength = decodedValue.size();
    materializedValues_.insert(
        materializedValues_.end(),
        decodedValue.data(),
        decodedValue.data() + valueLength);
    valueOffsets.push_back({valueOffset, valueLength});
  }

  // Second pass: create string_views pointing into materializedValues_
  std::string_view* valueOutput = static_cast<std::string_view*>(buffer);
  for (uint32_t i = 0; i < rowCount; ++i) {
    const auto [offset, len] = valueOffsets[i];
    valueOutput[i] = std::string_view(materializedValues_.data() + offset, len);
  }
}

uint32_t PrefixEncoding::restartOffset(uint32_t restartIndex) const {
  NIMBLE_CHECK_LT(restartIndex, numRestarts_, "Restart index out of bounds");
  const char* offsetPos = restartOffsets_ + (restartIndex * sizeof(uint32_t));
  return encoding::readUint32(offsetPos);
}

void PrefixEncoding::seekToRestartPoint(uint32_t restartIndex) {
  NIMBLE_CHECK_LT(restartIndex, numRestarts_, "Restart index out of bounds");

  // Seek to the restart point
  currentPos_ = dataStart_ + restartOffset(restartIndex);
  currentRow_ = restartIndex * restartInterval_;
  decodedValue_.clear();
}

std::optional<uint32_t> PrefixEncoding::seekAtOrAfter(const void* value) {
  const auto& targetValue = *static_cast<const std::string_view*>(value);

  // Binary search among restart points to find the block
  uint32_t left = 0;
  uint32_t right = numRestarts_;

  while (left < right) {
    const uint32_t mid = left + (right - left) / 2;

    seekToRestartPoint(mid);
    NIMBLE_CHECK_EQ(currentRow_, mid * restartInterval_);
    std::string_view restartValue = decodeEntry();
    if (restartValue == targetValue) {
      // decodeEntry() increments currentRow_ after decoding, so return the
      // row index before the increment
      return currentRow_ - 1;
    }

    if (restartValue < targetValue) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  // If left > 0, check the previous block
  if (left > 0) {
    --left;
  }

  // Seek to the identified restart point
  seekToRestartPoint(left);
  NIMBLE_CHECK_EQ(currentRow_, left * restartInterval_);

  // Linear scan within the block
  while (currentRow_ < rowCount_) {
    std::string_view currentValue = decodeEntry();

    if (currentValue >= targetValue) {
      // decodeEntry() increments currentRow_ after decoding, so return the
      // row index before the increment
      return currentRow_ - 1;
    }
  }

  // Target is greater than all entries
  return std::nullopt;
}

std::string_view PrefixEncoding::encode(
    EncodingSelection<physicalType>& /* unused */,
    std::span<const physicalType> values,
    Buffer& buffer) {
  const uint32_t valueCount = values.size();

  // Values must be sorted for prefix encoding to be effective
  // In production, caller should ensure this or we can add a check

  const uint32_t restartInterval = kDefaultRestartInterval;
  const auto numRestarts = computeNumRestarts(values.size(), restartInterval);
  Vector<uint32_t> restartOffsets{&buffer.getMemoryPool()};
  restartOffsets.reserve(numRestarts);
  Vector<char> encodedData{&buffer.getMemoryPool()};

  std::string_view lastValue;
  char buf[sizeof(uint32_t)];

  for (uint32_t i = 0; i < valueCount; ++i) {
    const auto& value = values[i];

    // Determine if this is a restart point
    const bool restart = (i % restartInterval == 0);

    uint32_t sharedPrefixLen = 0;
    if (!restart && i > 0) {
      // Calculate shared prefix length with previous entry
      size_t minLen = std::min(lastValue.size(), value.size());
      while (sharedPrefixLen < minLen &&
             lastValue[sharedPrefixLen] == value[sharedPrefixLen]) {
        ++sharedPrefixLen;
      }
    }

    if (restart) {
      // Record restart offset (current position in encoded data)
      restartOffsets.push_back(encodedData.size());
    }

    // Encode shared prefix length
    char* pos = buf;
    encoding::writeUint32(sharedPrefixLen, pos);
    encodedData.insert(encodedData.end(), buf, pos);

    // Encode suffix length
    const uint32_t suffixLen = value.size() - sharedPrefixLen;
    pos = buf;
    encoding::writeUint32(suffixLen, pos);
    encodedData.insert(encodedData.end(), buf, pos);

    // Encode suffix data
    if (suffixLen > 0) {
      const char* suffixStart = value.data() + sharedPrefixLen;
      encodedData.insert(
          encodedData.end(), suffixStart, suffixStart + suffixLen);
    }

    lastValue = value;
  }

  // Calculate total encoding size:
  // header + restart interval + restart offsets + encoded entries
  NIMBLE_DCHECK_EQ(
      numRestarts, restartOffsets.size(), "Restart count mismatch");
  const uint32_t restartOffsetsSize = numRestarts * sizeof(uint32_t);
  const uint32_t encodingSize =
      kRestartOffsetsOffset + restartOffsetsSize + encodedData.size();

  // Write encoded data to buffer
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  // Write encoding prefix
  Encoding::serializePrefix(
      EncodingType::Prefix,
      TypeTraits<std::string_view>::dataType,
      valueCount,
      pos);

  // Write restart interval
  encoding::writeUint32(restartInterval, pos);

  // Write restart offsets (at head for better seek performance)
  for (uint32_t offset : restartOffsets) {
    encoding::writeUint32(offset, pos);
  }

  // Write encoded entries
  encoding::writeBytes({encodedData.data(), encodedData.size()}, pos);

  NIMBLE_DCHECK_EQ(pos - reserved, encodingSize, "Encoding size mismatch");

  return {reserved, encodingSize};
}

std::string PrefixEncoding::debugString(int offset) const {
  std::string log = Encoding::debugString(offset);
  log += fmt::format(
      "\n{}restart_interval={}, num_restarts={}",
      std::string(offset, ' '),
      restartInterval_,
      numRestarts_);
  return log;
}

} // namespace facebook::nimble
