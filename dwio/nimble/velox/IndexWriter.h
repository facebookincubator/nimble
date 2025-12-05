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

#include <memory>
#include <optional>

#include "dwio/nimble/index/IndexConfig.h"
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"

namespace facebook::nimble {

class IndexWriter {
 public:
  static std::unique_ptr<IndexWriter> create(
      const std::optional<IndexConfig>& config,
      const velox::TypePtr& inputType,
      FieldWriterContext& context);

  ~IndexWriter() = default;

  IndexWriter(const IndexWriter&) = delete;
  IndexWriter& operator=(const IndexWriter&) = delete;
  IndexWriter(IndexWriter&&) = default;
  IndexWriter& operator=(IndexWriter&&) = default;

  void write(const velox::VectorPtr& input);

  void reset();

  void close();

  KeyStreamData* keyStreamData() {
    return keyStream_.get();
  }

 private:
  IndexWriter(
      const std::vector<std::string>& indexColumns,
      const velox::RowTypePtr& inputType,
      FieldWriterContext& context);

  /// Validates that the cluster index column name doesn't conflict with any
  /// existing top-level column in the table schema. This ensures that the
  /// generated cluster index column can be added to the table without
  /// naming conflicts.
  void validate();

  FieldWriterContext* const context_;
  const std::vector<std::string> indexColumns_;
  const velox::RowTypePtr inputType_;
  const std::unique_ptr<KeyEncoder> keyEncoder_;
  std::unique_ptr<KeyStreamData> keyStream_;
};

} // namespace facebook::nimble
