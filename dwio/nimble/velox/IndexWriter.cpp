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
#include "dwio/nimble/velox/IndexWriter.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/index/KeyEncoder.h"
#include "dwio/nimble/velox/VeloxWriterOptions.h"
#include "velox/type/Type.h"

namespace facebook::nimble {
std::unique_ptr<IndexWriter> IndexWriter::create(
    const std::optional<IndexConfig>& config,
    const velox::TypePtr& inputType,
    FieldWriterContext& context) {
  if (!config.has_value()) {
    return nullptr;
  }
  return std::unique_ptr<IndexWriter>(
      new IndexWriter(config->columns, velox::asRowType(inputType), context));
}

IndexWriter::IndexWriter(
    const std::vector<std::string>& indexColumns,
    const velox::RowTypePtr& inputType,
    FieldWriterContext& context)
    : context_{&context},
      indexColumns_{indexColumns},
      inputType_{inputType},
      keyEncoder_{KeyEncoder::create(
          indexColumns_,
          inputType_,
          std::vector<velox::core::SortOrder>{
              indexColumns_.size(),
              velox::core::SortOrder{true, true}},
          context_->bufferMemoryPool().get())},
      keyStream_{std::make_unique<KeyStreamData>(
          context_->bufferMemoryPool(),
          context_->schemaBuilder().createKeyTypeBuilder()->scalarDescriptor(),
          *context_->inputBufferGrowthPolicy())} {
  validate();
}

void IndexWriter::validate() {
  for (const auto& columnName : indexColumns_) {
    const auto tableChannelOpt = inputType_->getChildIdxIfExists(columnName);
    NIMBLE_CHECK(
        tableChannelOpt.has_value(),
        "Index column {} doesn exist in table columns",
        columnName);
    const auto& columnType = inputType_->childAt(tableChannelOpt.value());
    NIMBLE_CHECK(
        columnType->isPrimitiveType(),
        "Index column {} type {} not supported",
        columnName,
        columnType->toString());
  }
}

void IndexWriter::write(const velox::VectorPtr& input) {
  const uint64_t newSize = keyStream_->keys().size() + input->size();
  keyStream_->ensureMutableDataCapacity(newSize);
  keyEncoder_->encode(
      input, keyStream_->mutableKeys(), context_->stringBuffer());
}

void IndexWriter::reset() {
  keyStream_->reset();
}

void IndexWriter::close() {
  reset();
}
} // namespace facebook::nimble
