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

namespace facebook::nimble {

/// Configuration for index generation.
/// The index allows efficient filtering and pruning of data based on
/// the specified index columns.
struct IndexConfig {
  /// Columns to be indexed for data pruning.
  /// These columns will be encoded using KeyWriter to generate index keys
  /// that enable efficient data skipping during reads.
  std::vector<std::string> columns;
};

} // namespace facebook::nimble
