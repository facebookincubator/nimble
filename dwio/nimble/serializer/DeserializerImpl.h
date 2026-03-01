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

#include <functional>

#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/serializer/Options.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"

namespace facebook::nimble::serde {

class StreamData {
 public:
  /// Constructor for thrift decoder: creates an empty stream that will be
  /// populated later via reset().
  /// @param kind Scalar kind for the stream data.
  /// @param pool Memory pool for encoding buffer allocation. Required when
  ///             reset() will be called with encodingEnabled=true.
  StreamData(ScalarKind kind, velox::memory::MemoryPool* pool)
      : kind_{kind}, pool_{pool}, encodingEnabled_{false} {}

  /// @param kind Scalar kind for the stream data.
  /// @param encodingEnabled True for nimble encoding, false for legacy
  /// compression.
  /// @param data Stream data to initialize with.
  /// @param pool Required when encodingEnabled is true, nullptr for legacy.
  StreamData(
      ScalarKind kind,
      bool encodingEnabled,
      std::string_view data,
      velox::memory::MemoryPool* pool);

  uint32_t copyTo(char* output, uint32_t bufferSize);

  uint32_t decodeStrings(uint32_t count, std::string_view* output);

  /// Decode nimble-encoded data to output. Dispatches to typed materialize
  /// based on width. Only valid when hasEncoding() is true.
  /// Returns the number of values actually decoded (may be less than count
  /// if encoding has fewer remaining rows).
  uint32_t
  decode(void* output, uint32_t offset, uint32_t count, uint32_t width);

  /// Simplified decode for thrift decoder: decodes 'count' values to output.
  /// Uses sizeof(T) as width and offset 0.
  template <typename T>
  void decode(T* output, uint32_t count) {
    decode(output, /*offset=*/0, count, sizeof(T));
  }

  /// Reset the stream with new data. Used by thrift decoder between rows.
  /// @param data New stream data to initialize with.
  /// @param encodingEnabled True for nimble encoding, false for legacy.
  void reset(std::string_view data, bool encodingEnabled);

  ScalarKind kind() const {
    return kind_;
  }

  bool hasEncoding() const {
    return encoding_ != nullptr;
  }

 private:
  // Initialize with data. For encoding path, creates Encoding object.
  // For legacy path, decompresses if not string/binary type.
  void init(std::string_view data);

  // Decompress legacy zstd-compressed data. Reads compression type prefix and
  // decompresses into decompressionBuffer_ if needed.
  void decompress();

  // Prepare nimble-encoded data for reading. Creates an Encoding object that
  // can materialize values on demand.
  void prepareForDecoding(std::string_view data);

  // Returns the number of rows remaining in the encoding.
  uint32_t remainingRows() const {
    NIMBLE_CHECK_NOT_NULL(encoding_);
    return encoding_->rowCount() - readRows_;
  }

  // Materialize values from nimble encoding to typed output.
  template <typename T>
  void materialize(uint32_t count, T* output);

  const ScalarKind kind_{ScalarKind::Undefined};
  velox::memory::MemoryPool* const pool_{nullptr};
  // Whether nimble encoding is enabled. Non-const to allow reset() to change.
  bool encodingEnabled_{false};

  const char* pos_{nullptr};
  const char* end_{nullptr};
  std::string decompressionBuffer_;
  std::unique_ptr<Encoding> encoding_;
  // Track consumed rows for nimble encoding path.
  uint32_t readRows_{0};
  // Buffers for string data from nimble encoding.
  // Each buffer is allocated separately to avoid pointer invalidation when
  // the vector grows. Uses velox::AlignedBuffer for memory tracking.
  std::vector<velox::BufferPtr> stringBuffers_;
};

template <typename T>
void StreamData::materialize(uint32_t count, T* output) {
  if (count == 0) {
    return;
  }
  NIMBLE_CHECK_NOT_NULL(encoding_);
  encoding_->materialize(count, output);
  readRows_ += count;
}

class StreamDataReader {
 public:
  explicit StreamDataReader(const DeserializerOptions& options);

  /// Returns number of rows serialized.
  /// Validates that the version in serialized data matches options.
  uint32_t initialize(std::string_view data);

  void iterateStreams(
      const std::function<void(uint32_t offset, std::string_view data)>&
          callback);

 private:
  const DeserializerOptions& options_;
  const char* pos_{nullptr};
  const char* end_{nullptr};
};

} // namespace facebook::nimble::serde
