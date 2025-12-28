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

#include <string>
#include <vector>

#include "dwio/nimble/encodings/EncodingLayout.h"

namespace facebook::nimble {

/// Configuration for index generation.
/// The index allows efficient filtering and pruning of data based on
/// the specified index columns.
struct IndexConfig {
  /// Columns to be indexed for data pruning.
  /// These columns will be encoded using KeyWriter to generate index keys
  /// that enable efficient data skipping during reads.
  std::vector<std::string> columns;
  /// Specifies the key encoding layout.
  EncodingLayout encodingLayout = defaultEncodingLayout();
  /// If true, enforces that encoded keys must be in non-descending order across
  /// stripes. This ensures that stripe boundaries maintain sorted order for
  /// efficient range-based filtering. An exception is thrown if keys are found
  /// to be out of order.
  bool enforceKeyOrder{false};
  /// When flushing key stream into chunks, key stream with raw data size
  /// smaller than this threshold will not be flushed.
  /// Note: this threshold is ignored when it is time to flush a stripe.
  uint64_t minChunkRawSize{512 << 10};
  /// When flushing key stream into chunks, key stream with raw data size
  /// larger than this threshold will be broken down into multiple smaller
  /// chunks.
  uint64_t maxChunkRawSize{20 << 20};

  /// Returns the default encoding layout for index key stream.
  /// TrivialEncoding for string_view needs a nested encoding for string
  /// lengths.
  static EncodingLayout defaultEncodingLayout(
      CompressionType compressionType = CompressionType::Zstd) {
    return EncodingLayout{
        EncodingType::Trivial,
        compressionType,
        {EncodingLayout{EncodingType::Trivial, CompressionType::Uncompressed}}};
  }
};

} // namespace facebook::nimble
