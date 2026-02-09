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

#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingLayout.h"
#include "dwio/nimble/index/SortOrder.h"

namespace facebook::nimble {

/// Configuration for index generation.
/// The index allows efficient filtering and pruning of data based on
/// the specified index columns.
struct IndexConfig {
  /// Columns to be indexed for data pruning.
  /// These columns will be encoded using KeyWriter to generate index keys
  /// that enable efficient data skipping during reads.
  std::vector<std::string> columns;
  /// Specifies the sort order for each index column.
  /// If empty, defaults to ascending order for all columns.
  /// If not empty, must have the same size as 'columns'.
  std::vector<SortOrder> sortOrders;
  /// If true, enforces that encoded keys must be in ascending order.
  /// This ensures that stripe boundaries maintain sorted order for efficient
  /// range-based filtering. An exception is thrown if keys are found to be
  /// out of order. Duplicate keys are allowed unless noDuplicateKey is also
  /// set.
  bool enforceKeyOrder{false};
  /// If true, enforces that encoded keys must be in strictly ascending order
  /// with no duplicate keys allowed. If enforceKeyOrder is true, setting this
  /// option enforces the no duplicates check. An exception is thrown if keys
  /// are out of order or if duplicate keys are detected.
  bool noDuplicateKey{false};
  /// The encoding layout for the key stream.
  /// Only Prefix and Trivial encodings are supported.
  /// Users should pass a fully constructed EncodingLayout.
  /// For PrefixEncoding, use EncodingConfig with key "prefixRestartInterval"
  /// to control the restart interval (default: 16).
  /// For Trivial encoding, include a child EncodingLayout for the lengths
  /// stream.
  EncodingLayout encodingLayout{
      EncodingType::Prefix,
      {},
      CompressionType::Uncompressed};
  /// When flushing key stream into chunks, key stream with raw data size
  /// smaller than this threshold will not be flushed.
  /// Note: this threshold is ignored when it is time to flush a stripe.
  uint64_t minChunkRawSize{512 << 10};
  /// When flushing key stream into chunks, key stream with raw data size
  /// larger than this threshold will be broken down into multiple smaller
  /// chunks.
  uint64_t maxChunkRawSize{20 << 20};
};

} // namespace facebook::nimble
