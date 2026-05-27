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

#include "dwio/nimble/encodings/common/EncodingPrimitives.h"
#include "velox/common/base/BitUtil.h"

namespace facebook::nimble {

PrefixEncoding::PrefixEncoding(
    velox::memory::MemoryPool& pool,
    std::string_view data,
    std::function<void*(uint32_t)> stringBufferFactory,
    const Encoding::Options& options)
    : TypedEncoding<std::string_view, std::string_view>{pool, data, options},
      stringBufferFactory_{std::move(stringBufferFactory)},
      restartInterval_{readRestartInterval(data, dataOffset())},
      numRestarts_{computeNumRestarts(rowCount_, restartInterval_)},
      restartOffsets_{restartOffsets(data, dataOffset())},
      dataStart_{dataStart(data, dataOffset(), numRestarts_)},
      decodedValue_{pool_} {
  reset();
}

// static
uint32_t PrefixEncoding::readRestartInterval(
    std::string_view data,
    uint32_t startOffset) {
  const auto* pos = data.data() + startOffset;
  return encoding::readUint32(pos);
}

// static
uint32_t PrefixEncoding::computeNumRestarts(
    uint32_t rowCount,
    uint32_t restartInterval) {
  return velox::bits::divRoundUp(rowCount, restartInterval);
}

// static
const char* PrefixEncoding::restartOffsets(
    std::string_view data,
    uint32_t startOffset) {
  return data.data() + startOffset + 4;
}

// static
const char* PrefixEncoding::dataStart(
    std::string_view data,
    uint32_t startOffset,
    uint32_t numRestarts) {
  return data.data() + startOffset + 4 + (numRestarts * sizeof(uint32_t));
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

// static
std::string_view PrefixEncoding::decodeEntryAt(
    const char*& pos,
    uint32_t& row,
    std::string& decoded) {
  const uint32_t sharedPrefixLen = encoding::readUint32(pos);
  const uint32_t suffixLen = encoding::readUint32(pos);
  const uint32_t fullLen = sharedPrefixLen + suffixLen;
  NIMBLE_DCHECK_LE(sharedPrefixLen, decoded.size());
  decoded.resize(fullLen);
  if (suffixLen > 0) {
    std::memcpy(decoded.data() + sharedPrefixLen, pos, suffixLen);
    pos += suffixLen;
  }
  ++row;
  return std::string_view(decoded.data(), fullLen);
}

void PrefixEncoding::allocatePage(size_t minSize) {
  const auto size = std::max(kStringPageSize, minSize);
  currentPage_ = static_cast<char*>(stringBufferFactory_(size));
  pageCapacity_ = size;
  pageUsed_ = 0;
}

std::string_view PrefixEncoding::decodeToStringBuffer() {
  const auto decoded = decodeEntry();
  if (decoded.empty()) {
    return decoded;
  }
  if (pageUsed_ + decoded.size() > pageCapacity_) {
    allocatePage(decoded.size());
  }
  std::memcpy(currentPage_ + pageUsed_, decoded.data(), decoded.size());
  auto result = std::string_view(currentPage_ + pageUsed_, decoded.size());
  pageUsed_ += decoded.size();
  return result;
}

void PrefixEncoding::materialize(uint32_t rowCount, void* buffer) {
  NIMBLE_CHECK_LE(currentRow_ + rowCount, rowCount_, "Invalid row count");

  auto* output = static_cast<std::string_view*>(buffer);
  for (uint32_t i = 0; i < rowCount; ++i) {
    output[i] = decodeToStringBuffer();
  }
}

uint32_t PrefixEncoding::restartOffset(uint32_t restartIndex) const {
  NIMBLE_CHECK_LT(restartIndex, numRestarts_, "Restart index out of bounds");
  const char* offsetPos = restartOffsets_ + (restartIndex * sizeof(uint32_t));
  return encoding::readUint32(offsetPos);
}

void PrefixEncoding::get(uint32_t row, void* value) {
  NIMBLE_CHECK_LT(row, rowCount_);

  // Thread-safe: uses only local state and const members.
  const uint32_t restartIndex = row / restartInterval_;
  const char* pos = restartPosition(restartIndex);
  uint32_t currentRow = restartIndex * restartInterval_;
  std::string decoded;

  while (currentRow <= row) {
    decodeEntryAt(pos, currentRow, decoded);
  }

  // Copy into stable string buffer so the returned string_view outlives
  // this call.
  auto* buffer = static_cast<char*>(stringBufferFactory_(decoded.size()));
  std::memcpy(buffer, decoded.data(), decoded.size());
  *static_cast<std::string_view*>(value) =
      std::string_view(buffer, decoded.size());
}

void PrefixEncoding::seekToRestartPoint(uint32_t restartIndex) {
  NIMBLE_CHECK_LT(restartIndex, numRestarts_, "Restart index out of bounds");

  // Seek to the restart point
  currentPos_ = restartPosition(restartIndex);
  currentRow_ = restartIndex * restartInterval_;
  decodedValue_.clear();
}

std::optional<uint32_t> PrefixEncoding::seek(
    const void* value,
    bool inclusive) {
  const auto& targetValue = *static_cast<const std::string_view*>(value);

  // Thread-safe: uses only local state and const members. No member mutation.
  const char* pos = nullptr;
  uint32_t row = 0;
  std::string decoded;

  // Binary search among restart points to find the block containing the target.
  uint32_t left = 0;
  uint32_t right = numRestarts_;

  while (left < right) {
    const uint32_t mid = left + (right - left) / 2;

    pos = restartPosition(mid);
    row = mid * restartInterval_;
    decoded.clear();
    const auto restartValue = decodeEntryAt(pos, row, decoded);

    if (restartValue.compare(targetValue) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  // 'left' is now the first restart point whose value >= targetValue,
  // or numRestarts_ if all restart values < targetValue.
  // We need to search from the previous restart point (if exists) because
  // the target might be within that block but after its restart value.
  if (left > 0) {
    --left;
  }

  pos = restartPosition(left);
  row = left * restartInterval_;
  decoded.clear();

  // Linear scan from the restart point. Each decodeEntryAt() builds on the
  // shared prefix left in 'decoded' by the previous call. At restart
  // boundaries, sharedPrefixLen is 0 so the full key is written regardless.
  if (inclusive) {
    while (row < rowCount_) {
      if (decodeEntryAt(pos, row, decoded) >= targetValue) {
        return row - 1;
      }
    }
  } else {
    while (row < rowCount_) {
      if (decodeEntryAt(pos, row, decoded) > targetValue) {
        return row - 1;
      }
    }
  }

  return std::nullopt;
}

// static
uint32_t PrefixEncoding::restartInterval(
    const EncodingSelection<physicalType>& selection) {
  const auto configValueOpt =
      selection.getConfig(std::string(kRestartIntervalConfigKey));
  if (!configValueOpt.has_value()) {
    return kDefaultRestartInterval;
  }
  // Use stoll (signed) to properly detect negative values before converting
  const auto intervalSigned = std::stoll(*configValueOpt);
  NIMBLE_USER_CHECK_GT(
      intervalSigned, 0, "Restart interval must be greater than 0");
  return static_cast<uint32_t>(intervalSigned);
}

std::string_view PrefixEncoding::encode(
    EncodingSelection<physicalType>& selection,
    std::span<const physicalType> values,
    Buffer& buffer,
    const Encoding::Options& options) {
  const bool useVarint = options.useVarintRowCount;
  const uint32_t valueCount = values.size();

  // Get restart interval from config, or use default
  const uint32_t restartInterval = PrefixEncoding::restartInterval(selection);
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
      Encoding::serializePrefixSize(valueCount, useVarint) + 4 +
      restartOffsetsSize + encodedData.size();

  // Write encoded data to buffer
  char* reserved = buffer.reserve(encodingSize);
  char* pos = reserved;

  // Write encoding prefix
  Encoding::serializePrefix(
      EncodingType::Prefix,
      TypeTraits<std::string_view>::dataType,
      valueCount,
      useVarint,
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
