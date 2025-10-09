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
template <typename T>
std::unique_ptr<StreamChunker> encodeStreamTyped(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool emptyStreamContent,
    bool isNullStream) {
  const auto& streamDataType = streamData.type();
  if (streamDataType == ContentStreamData<T>::TYPE_NAME) {
    return std::make_unique<ContentStreamChunker<T>>(
        static_cast<ContentStreamData<T>&>(streamData),
        maxChunkSize,
        minChunkSize);
  } else if (streamDataType == NullsStreamData::TYPE_NAME) {
    return std::make_unique<NullsStreamChunker>(
        static_cast<NullsStreamData&>(streamData),
        maxChunkSize,
        minChunkSize,
        emptyStreamContent);
  } else if (streamDataType == NullableContentStreamData<T>::TYPE_NAME) {
    return std::make_unique<NullableContentStreamChunker<T>>(
        static_cast<NullableContentStreamData<T>&>(streamData),
        maxChunkSize,
        minChunkSize,
        emptyStreamContent,
        isNullStream);
  }
  NIMBLE_UNREACHABLE(
      fmt::format("Unsupported streamData type {}", streamDataType))
}

std::unique_ptr<StreamChunker> getStreamChunker(
    StreamData& streamData,
    uint64_t maxChunkSize,
    uint64_t minChunkSize,
    bool emptyStreamContent,
    bool isNullStream) {
  const auto scalarKind = streamData.descriptor().scalarKind();
  switch (scalarKind) {
    case ScalarKind::Bool:
      return encodeStreamTyped<bool>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Int8:
      return encodeStreamTyped<int8_t>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Int16:
      return encodeStreamTyped<int16_t>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Int32:
      return encodeStreamTyped<int32_t>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::UInt32:
      return encodeStreamTyped<uint32_t>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Int64:
      return encodeStreamTyped<int64_t>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Float:
      return encodeStreamTyped<float>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::Double:
      return encodeStreamTyped<double>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    case ScalarKind::String:
    case ScalarKind::Binary:
      return encodeStreamTyped<std::string_view>(
          streamData,
          maxChunkSize,
          minChunkSize,
          emptyStreamContent,
          isNullStream);
    default:
      NIMBLE_UNREACHABLE(
          fmt::format("Unsupported scalar kind {}", toString(scalarKind)));
  }
}
} // namespace facebook::nimble
