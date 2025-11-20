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
#include "dwio/nimble/velox/VeloxWriterUtils.h"
#include "dwio/nimble/common/Bits.h"

namespace facebook::nimble {
std::vector<uint32_t> getStreamIndicesByMemoryUsage(
    const std::vector<std::unique_ptr<StreamData>>& streams) {
  constexpr size_t kMaxBits = sizeof(uint64_t) * 8;
  std::vector<std::vector<uint32_t>> sortedStreams(kMaxBits);
  for (auto streamIndex = 0; streamIndex < streams.size(); ++streamIndex) {
    const auto bitsRequired =
        bits::bitsRequired(streams[streamIndex]->memoryUsed());
    NIMBLE_DCHECK_LT(bitsRequired, kMaxBits);
    sortedStreams[bitsRequired].push_back(streamIndex);
  }

  std::vector<uint32_t> streamIndices;
  streamIndices.reserve(streams.size());
  auto bucketIndex = static_cast<int64_t>(sortedStreams.size()) - 1;
  while (bucketIndex >= 0) {
    for (const auto streamIndex : sortedStreams[bucketIndex]) {
      streamIndices.push_back(streamIndex);
    }
    bucketIndex--;
  }
  return streamIndices;
}
} // namespace facebook::nimble
