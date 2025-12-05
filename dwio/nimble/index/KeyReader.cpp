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

#include "dwio/nimble/index/KeyReader.h"
#include <algorithm>
#include "dwio/nimble/common/Exceptions.h"

namespace facebook::nimble {

KeyReader::KeyReader(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
    uint32_t stripeIndex,
    std::shared_ptr<StripeIndexGroup> stripeIndexGroup,
    velox::memory::MemoryPool* pool)
    : stripeIndex_{stripeIndex},
      stripeIndexGroup_{std::move(stripeIndexGroup)},
      decoder_{
          std::move(input),
          /*decodeValuesWithNulls=*/false,
          pool} {}

std::unique_ptr<KeyReader> KeyReader::create(
    std::unique_ptr<velox::dwio::common::SeekableInputStream> input,
    uint32_t stripeIndex,
    std::shared_ptr<StripeIndexGroup> stripeIndexGroup,
    velox::memory::MemoryPool* pool) {
  return std::unique_ptr<KeyReader>(new KeyReader(
      std::move(input), stripeIndex, std::move(stripeIndexGroup), pool));
}

std::optional<uint32_t> KeyReader::seekAtOrAfter(
    std::string_view encodedKey,
    bool& exactMatch) {
  exactMatch = false;
  const auto chunkLocation =
      stripeIndexGroup_->lookupChunk(stripeIndex_, encodedKey);
  NIMBLE_CHECK(chunkLocation.has_value(), "chunk not found");

  const auto rowOffset = decoder_.seekAtOrAfter(
      chunkLocation->streamOffset, encodedKey, exactMatch);
  NIMBLE_CHECK(rowOffset.has_value(), "row not found");
  return chunkLocation->rowOffset + rowOffset.value();
}
} // namespace facebook::nimble
