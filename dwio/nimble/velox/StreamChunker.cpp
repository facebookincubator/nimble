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

#include "dwio/nimble/velox/StreamChunker.h"

namespace facebook::nimble {
bool omitStream(
    const StreamData& streamData,
    uint64_t minChunkSize,
    bool isNullStream,
    bool hasEmptyStreamContent,
    bool isLastChunk) {
  bool shouldChunkStream;
  minChunkSize = isLastChunk ? 0 : minChunkSize;
  if (isNullStream) {
    // When all values are non-nulls, we omit the entire null stream.
    shouldChunkStream =
        streamData.hasNulls() && streamData.nonNulls().size() > minChunkSize;
    if (!shouldChunkStream && !hasEmptyStreamContent) {
      shouldChunkStream = !streamData.empty();
    }
  } else {
    // When all values are null, the values stream is omitted.
    shouldChunkStream = streamData.data().size() > minChunkSize;
    if (!shouldChunkStream && !hasEmptyStreamContent) {
      shouldChunkStream = !streamData.nonNulls().empty();
    }
  }

  return !shouldChunkStream;
}

template <typename T>
std::unique_ptr<StreamChunker> getStreamChunkerTyped(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool ensureFullChunks,
    bool emptyStreamContent,
    bool isNullStream,
    bool isLastChunk) {
  const auto& streamDataType = streamData.type();
  if (streamDataType == ContentStreamData<T>::TYPE_NAME) {
    return std::make_unique<ContentStreamChunker<T>>(
        static_cast<ContentStreamData<T>&>(streamData),
        maxChunkSize,
        minChunkSize,
        ensureFullChunks,
        isLastChunk);
  } else if (streamDataType == NullsStreamData::TYPE_NAME) {
    return std::make_unique<NullsStreamChunker>(
        static_cast<NullsStreamData&>(streamData),
        maxChunkSize,
        minChunkSize,
        ensureFullChunks,
        emptyStreamContent,
        isNullStream,
        isLastChunk);
  } else if (streamDataType == NullableContentStreamData<T>::TYPE_NAME) {
    return std::make_unique<NullableContentStreamChunker<T>>(
        static_cast<NullableContentStreamData<T>&>(streamData),
        maxChunkSize,
        minChunkSize,
        ensureFullChunks,
        emptyStreamContent,
        isNullStream,
        isLastChunk);
  }
  NIMBLE_UNREACHABLE(
      fmt::format("Unsupported streamData type {}", streamDataType))
}

std::unique_ptr<StreamChunker> getStreamChunker(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool ensureFullChunks,
    bool emptyStreamContent,
    bool isNullStream,
    bool isLastChunk) {
  const auto scalarKind = streamData.descriptor().scalarKind();
  switch (scalarKind) {
#define HANDLE_SCALAR_KIND(kind, type)  \
  case ScalarKind::kind:                \
    return getStreamChunkerTyped<type>( \
        streamData,                     \
        maxChunkSize,                   \
        minChunkSize,                   \
        ensureFullChunks,               \
        emptyStreamContent,             \
        isNullStream,                   \
        isLastChunk);
    HANDLE_SCALAR_KIND(Bool, bool);
    HANDLE_SCALAR_KIND(Int8, int8_t);
    HANDLE_SCALAR_KIND(Int16, int16_t);
    HANDLE_SCALAR_KIND(Int32, int32_t);
    HANDLE_SCALAR_KIND(UInt32, uint32_t);
    HANDLE_SCALAR_KIND(Int64, int64_t);
    HANDLE_SCALAR_KIND(Float, float);
    HANDLE_SCALAR_KIND(Double, double);
    HANDLE_SCALAR_KIND(String, std::string_view);
    HANDLE_SCALAR_KIND(Binary, std::string_view);
    default:
      NIMBLE_UNREACHABLE(
          fmt::format("Unsupported scalar kind {}", toString(scalarKind)));
  }
}
} // namespace facebook::nimble
