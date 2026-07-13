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

#include "dwio/nimble/serializer/Projector.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/serializer/SerializerImpl.h"

namespace facebook::nimble::serde {

namespace {

// Lightweight adapter for writing small sections (header, trailer) directly
// into an IOBuf. Satisfies the size()/resize()/data() interface required by
// detail::extend/writeSerializationHeader/writeTrailer. Avoids the std::string
// → IOBuf copy that would occur if writing into a temporary std::string first.
class IOBufSection {
 public:
  explicit IOBufSection(size_t initialCapacity)
      : buf_(folly::IOBuf::create(initialCapacity)) {}

  size_t size() const {
    return size_;
  }

  void resize(size_t newSize) {
    if (newSize > buf_->capacity()) {
      auto newBuf =
          folly::IOBuf::create(std::max(newSize, buf_->capacity() * 2));
      if (size_ > 0) {
        std::memcpy(newBuf->writableData(), buf_->data(), size_);
      }
      buf_ = std::move(newBuf);
    }
    size_ = newSize;
  }

  char* data() {
    return reinterpret_cast<char*>(buf_->writableData());
  }

  // Finalizes the IOBuf by setting its length and returns it.
  std::unique_ptr<folly::IOBuf> build() && {
    buf_->append(size_);
    return std::move(buf_);
  }

 private:
  std::unique_ptr<folly::IOBuf> buf_;
  size_t size_{0};
};

inline void setNullBarrierRequiredFlag(
    folly::IOBuf& header,
    size_t flagsOffset,
    bool outputRequiresNullBarrier) {
  NIMBLE_CHECK_LT(flagsOffset, header.length(), "Invalid flags byte offset");
  header.writableData()[flagsOffset] =
      detail::makeFlagsByte(outputRequiresNullBarrier);
}

} // namespace

Projector::Projector(
    std::shared_ptr<const Type> inputSchema,
    const std::vector<Subfield>& projectSubfields,
    velox::memory::MemoryPool* pool,
    Options options)
    : selector_(
          std::move(inputSchema),
          projectSubfields,
          StreamSelector::Options{
              .projectVersion = options.projectVersion,
              .projectType = std::move(options.projectType)}),
      streamIndicesEncodingType_(options.streamIndicesEncodingType),
      streamSizesEncodingType_(options.streamSizesEncodingType) {}

folly::IOBuf Projector::buildProjectedOutput(
    const std::vector<uint32_t>& outputStreamSizes,
    SerializationVersion version,
    std::unique_ptr<folly::IOBuf> output) const {
  IOBufSection trailer(
      detail::estimateTrailerSize(
          version,
          outputStreamSizes.size(),
          streamIndicesEncodingType_,
          streamSizesEncodingType_));
  detail::writeTrailer(
      outputStreamSizes,
      streamIndicesEncodingType_,
      streamSizesEncodingType_,
      trailer);
  output->appendToChain(std::move(trailer).build());
  return std::move(*output);
}

folly::IOBuf Projector::project(const folly::IOBuf& input) const {
  // Step 1: select the projected streams (zero-copy). Step 2 (below): frame
  // them with a serialization header and a stream-sizes trailer into a
  // standalone blob. The Deserializer reuses step 1 directly and skips step 2.
  auto streams = selector_.selectStreams(input);

  IOBufSection header(
      estimateSerializationHeaderSize(streams.version, streams.rowCount));
  const auto flagsOffset =
      writeSerializationHeader(header, streams.version, streams.rowCount);
  auto output = std::move(header).build();
  setNullBarrierRequiredFlag(*output, flagsOffset, streams.requiresNullBarrier);
  output->appendToChain(
      std::make_unique<folly::IOBuf>(std::move(streams.streamData)));

  return buildProjectedOutput(
      streams.streamSizes, streams.version, std::move(output));
}

folly::IOBuf Projector::project(std::string_view input) const {
  return project(folly::IOBuf::wrapBufferAsValue(input.data(), input.size()));
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<std::string_view>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<folly::IOBuf>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

} // namespace facebook::nimble::serde
