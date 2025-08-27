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

#include "dwio/nimble/velox/selective/FlatMapColumnReader.h"

#include "dwio/nimble/velox/selective/ChunkedDecoder.h"
#include "dwio/nimble/velox/selective/ColumnReader.h"
#include "dwio/nimble/velox/selective/StructColumnReader.h"
#include "velox/dwio/common/FlatMapHelper.h"
#include "velox/dwio/common/SelectiveFlatMapColumnReader.h"
#include "velox/vector/FlatMapVector.h"

namespace facebook::nimble {

using namespace facebook::velox;

namespace {

template <typename T>
struct KeyNode {
  dwio::common::flatmap::KeyValue<T> key;
  std::unique_ptr<ChunkedDecoder> inMap;
  std::unique_ptr<dwio::common::SelectiveColumnReader> reader;

  explicit KeyNode(dwio::common::flatmap::KeyValue<T> key)
      : key(std::move(key)) {}
};

template <typename T>
std::vector<KeyNode<T>> makeKeyNodes(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleParams& params,
    common::ScanSpec& scanSpec,
    dwio::common::flatmap::FlatMapOutput outputType,
    velox::memory::MemoryPool& memoryPool) {
  using namespace dwio::common::flatmap;
  std::vector<KeyNode<T>> keyNodes;
  auto& requestedValueType = requestedType->childAt(1);
  auto& fileValueType = fileType->childAt(1);
  common::ScanSpec* keysSpec = nullptr;
  common::ScanSpec* valuesSpec = nullptr;

  folly::F14FastMap<KeyValue<T>, common::ScanSpec*, KeyValueHash<T>> childSpecs;

  auto& nimbleType = params.nimbleType()->asFlatMap();
  int childrenCount = nimbleType.childrenCount();

  // When flatmap is empty, writer creates dummy child with empty name to
  // carry schema information. We need to capture actual children count.
  if (childrenCount == 1 && nimbleType.nameAt(0).empty()) {
    childrenCount = 0;
  }

  // Adjust the scan spec according to the output type.
  switch (outputType) {
    // For a kMap output, just need a scan spec for map keys and one for map
    // values.
    case FlatMapOutput::kMap: {
      keysSpec = scanSpec.getOrCreateChild(
          common::Subfield(common::ScanSpec::kMapKeysFieldName));
      valuesSpec = scanSpec.getOrCreateChild(
          common::Subfield(common::ScanSpec::kMapValuesFieldName));
      VELOX_CHECK(!valuesSpec->hasFilter());
      keysSpec->setProjectOut(true);
      valuesSpec->setProjectOut(true);
      break;
    }
    // For a kFlatMap output, need to find the streams (distinct keys) to read
    // from the file (nimbleType).
    case FlatMapOutput::kFlatMap: {
      for (int i = 0; i < childrenCount; ++i) {
        auto key = parseKeyValue<T>(nimbleType.nameAt(i));
        auto spec = scanSpec.getOrCreateChild(nimbleType.nameAt(i));
        spec->setProjectOut(true);
        spec->setChannel(i);
        childSpecs[key] = spec;
      }
      break;
    }
    // For a kStruct output, the streams to be read are part of the scan spec
    // already.
    case FlatMapOutput::kStruct: {
      for (auto& c : scanSpec.children()) {
        T key;
        if constexpr (std::is_same_v<T, StringView>) {
          key = StringView(c->fieldName());
        } else {
          key = folly::to<T>(c->fieldName());
        }
        childSpecs[KeyValue<T>(key)] = c.get();
      }
      break;
    }
  }

  // Create column readers for each stream and populate the keyNodes vector.
  for (int i = 0; i < childrenCount; ++i) {
    KeyNode<T> node(parseKeyValue<T>(nimbleType.nameAt(i)));
    common::ScanSpec* childSpec;
    if (auto it = childSpecs.find(node.key);
        it != childSpecs.end() && !it->second->isConstant()) {
      childSpec = it->second;
    } else if (outputType != FlatMapOutput::kMap) {
      // Column not selected in 'scanSpec', skipping it.
      continue;
    } else {
      if (keysSpec && keysSpec->filter() &&
          !common::applyFilter(*keysSpec->filter(), node.key.get())) {
        continue; // Subfield pruning
      }
      childSpecs[node.key] = childSpec = valuesSpec;
    }
    auto inMapInput =
        params.streams().enqueue(nimbleType.inMapDescriptorAt(i).offset());
    if (!inMapInput) {
      continue;
    }
    node.inMap = std::make_unique<ChunkedDecoder>(
        std::move(inMapInput), memoryPool, /*decodeValuesWithNulls=*/false);
    auto childParams = params.makeChildParams(nimbleType.childAt(i));
    childParams.setInMapDecoder(node.inMap.get());
    node.reader = buildColumnReader(
        requestedValueType, fileValueType, childParams, *childSpec, false);
    keyNodes.push_back(std::move(node));
  }
  return keyNodes;
}

template <typename T>
class FlatMapAsStructColumnReader : public StructColumnReaderBase {
 public:
  FlatMapAsStructColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      common::ScanSpec& scanSpec)
      : StructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec,
            false),
        keyNodes_(makeKeyNodes<T>(
            requestedType,
            fileType,
            params,
            scanSpec,
            dwio::common::flatmap::FlatMapOutput::kStruct,
            *memoryPool_)) {
    children_.resize(keyNodes_.size());
    for (auto& childSpec : scanSpec.children()) {
      childSpec->setSubscript(kConstantChildSpecSubscript);
    }
    for (int i = 0; i < keyNodes_.size(); ++i) {
      keyNodes_[i].reader->scanSpec()->setSubscript(i);
      children_[i] = keyNodes_[i].reader.get();
    }
  }

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const final {
    auto* nulls = formatData().template as<NimbleData>().nullsDecoder();
    auto* inMap = formatData().template as<NimbleData>().inMapDecoder();
    if (nulls) {
      auto nullsRowCount = nulls->estimateRowCount();
      if (!nullsRowCount.has_value()) {
        return false;
      }
      rowCount = *nullsRowCount;
    } else if (inMap) {
      auto inMapRowCount = inMap->estimateRowCount();
      if (!inMapRowCount.has_value()) {
        return false;
      }
      rowCount = *inMapRowCount;
    } else {
      rowCount = 0;
    }
    size_t rowSize = 0;
    for (auto& child : children_) {
      size_t childByteSize, childRowCount;
      if (!child->estimateMaterializedSize(childByteSize, childRowCount)) {
        return false;
      }
      if (!nulls && !inMap) {
        rowCount = childRowCount;
      }
      if (childRowCount > 0) {
        rowSize += childByteSize / childRowCount;
      }
    }
    byteSize = rowSize * rowCount;
    if (nulls || inMap) {
      byteSize += rowCount / 8;
    }
    return true;
  }

 private:
  std::vector<KeyNode<T>> keyNodes_;
};

template <typename T>
class FlatMapColumnReader
    : public velox::dwio::common::SelectiveFlatMapColumnReader {
 public:
  FlatMapColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      common::ScanSpec& scanSpec)
      : SelectiveFlatMapColumnReader(requestedType, fileType, params, scanSpec),
        keyNodes_(makeKeyNodes<T>(
            requestedType,
            fileType,
            params,
            scanSpec,
            dwio::common::flatmap::FlatMapOutput::kFlatMap,
            *memoryPool_)) {
    // Instantiate and populate distinct keys vector.
    keysVector_ = BaseVector::create(
        CppToType<T>::create(),
        (vector_size_t)keyNodes_.size(),
        &params.pool());
    auto rawKeys = keysVector_->values()->asMutable<T>();
    children_.resize(keyNodes_.size());

    for (int i = 0; i < keyNodes_.size(); ++i) {
      keyNodes_[i].reader->scanSpec()->setSubscript(i);
      children_[i] = keyNodes_[i].reader.get();

      rawKeys[i] = keyNodes_[i].key.get();
    }
  }

  const BufferPtr& inMapBuffer(column_index_t childIndex) const override {
    return children_[childIndex]
        ->formatData()
        .template as<NimbleData>()
        .inMapBuffer();
  }

  // Same as FlatMapAsStructColumnReader.
  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const final {
    auto* nulls = formatData().template as<NimbleData>().nullsDecoder();
    auto* inMap = formatData().template as<NimbleData>().inMapDecoder();
    if (nulls) {
      auto nullsRowCount = nulls->estimateRowCount();
      if (!nullsRowCount.has_value()) {
        return false;
      }
      rowCount = *nullsRowCount;
    } else if (inMap) {
      auto inMapRowCount = inMap->estimateRowCount();
      if (!inMapRowCount.has_value()) {
        return false;
      }
      rowCount = *inMapRowCount;
    } else {
      rowCount = 0;
    }
    size_t rowSize = 0;
    for (auto& child : children_) {
      size_t childByteSize, childRowCount;
      if (!child->estimateMaterializedSize(childByteSize, childRowCount)) {
        return false;
      }
      if (!nulls && !inMap) {
        rowCount = childRowCount;
      }
      if (childRowCount > 0) {
        rowSize += childByteSize / childRowCount;
      }
    }
    byteSize = rowSize * rowCount;
    if (nulls || inMap) {
      byteSize += rowCount / 8;
    }
    return true;
  }

  void seekToRowGroup(int64_t /*index*/) final {
    VELOX_UNREACHABLE();
  }

  void advanceFieldReader(SelectiveColumnReader* /*reader*/, int64_t /*offset*/)
      final {
    // No-op, there is no index for fast skipping and we need to skip in the
    // decoders.
  }

 private:
  std::vector<KeyNode<T>> keyNodes_;
};

template <typename T>
class FlatMapAsMapColumnReader : public StructColumnReaderBase {
 public:
  FlatMapAsMapColumnReader(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      NimbleParams& params,
      common::ScanSpec& scanSpec)
      : StructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec,
            false),
        flatMap_(
            *this,
            makeKeyNodes<T>(
                requestedType,
                fileType,
                params,
                scanSpec,
                dwio::common::flatmap::FlatMapOutput::kMap,
                *memoryPool_)) {}

  void read(int64_t offset, const RowSet& rows, const uint64_t* incomingNulls)
      override {
    flatMap_.read(offset, rows, incomingNulls);
  }

  void getValues(const RowSet& rows, VectorPtr* result) override {
    flatMap_.getValues(rows, result);
  }

  bool estimateMaterializedSize(size_t& byteSize, size_t& rowCount)
      const final {
    auto* nulls = formatData().template as<NimbleData>().nullsDecoder();
    auto* inMap = formatData().template as<NimbleData>().inMapDecoder();
    if (nulls) {
      auto nullsRowCount = nulls->estimateRowCount();
      if (!nullsRowCount.has_value()) {
        return false;
      }
      rowCount = *nullsRowCount;
    } else if (inMap) {
      auto inMapRowCount = inMap->estimateRowCount();
      if (!inMapRowCount.has_value()) {
        return false;
      }
      rowCount = *inMapRowCount;
    } else {
      rowCount = 0;
    }
    size_t rowSize = 8;
    for (auto& child : children_) {
      size_t childByteSize, childRowCount;
      if (!child->estimateMaterializedSize(childByteSize, childRowCount)) {
        return false;
      }
      if (!nulls && !inMap) {
        rowCount = childRowCount;
      }
      if (childRowCount > 0) {
        rowSize += childByteSize / childRowCount;
        rowSize += sizeof(T);
      }
    }
    byteSize = rowSize * rowCount;
    if (nulls || inMap) {
      byteSize += rowCount / 8;
    }
    return true;
  }

 private:
  dwio::common::SelectiveFlatMapColumnReaderHelper<T, KeyNode<T>, NimbleData>
      flatMap_;
};

template <typename T>
std::unique_ptr<dwio::common::SelectiveColumnReader> createReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    NimbleParams& params,
    common::ScanSpec& scanSpec) {
  if (scanSpec.isFlatMapAsStruct()) {
    return std::make_unique<FlatMapAsStructColumnReader<T>>(
        requestedType, fileType, params, scanSpec);
  } else if (params.preserveFlatMapsInMemory()) {
    return std::make_unique<FlatMapColumnReader<T>>(
        requestedType, fileType, params, scanSpec);
  } else {
    return std::make_unique<FlatMapAsMapColumnReader<T>>(
        requestedType, fileType, params, scanSpec);
  }
}

} // namespace

std::unique_ptr<velox::dwio::common::SelectiveColumnReader>
createFlatMapColumnReader(
    const TypePtr& requestedType,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& fileType,
    NimbleParams& params,
    velox::common::ScanSpec& scanSpec) {
  VELOX_DCHECK(requestedType->isMap());
  auto kind = requestedType->childAt(0)->kind();
  switch (kind) {
    case TypeKind::TINYINT:
      return createReader<int8_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::SMALLINT:
      return createReader<int16_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::INTEGER:
      return createReader<int32_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::BIGINT:
      return createReader<int64_t>(requestedType, fileType, params, scanSpec);
    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      return createReader<StringView>(
          requestedType, fileType, params, scanSpec);
    default:
      VELOX_UNSUPPORTED("Not supported key type: {}", kind);
  }
}

} // namespace facebook::nimble
