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

#include "velox/dwio/common/Options.h"

namespace facebook::nimble {

/// Format-specific options for Nimble row readers.
class NimbleRowReaderOptions
    : public velox::dwio::common::FormatSpecificOptions {
 public:
  NimbleRowReaderOptions() = default;

  /// Sets whether to use the cluster index for filter-based row pruning.
  /// When enabled (default), filters from ScanSpec are converted to index
  /// bounds for efficient row skipping based on the file's cluster index.
  /// When disabled, all rows are scanned without index-based pruning.
  void setIndexEnabled(bool enabled) {
    indexEnabled_ = enabled;
  }

  /// Returns true if cluster index-based filtering is enabled.
  bool indexEnabled() const {
    return indexEnabled_;
  }

 private:
  bool indexEnabled_{true};
};

} // namespace facebook::nimble
