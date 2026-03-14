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

#include "dwio/nimble/serializer/Projector.h"

#include <algorithm>
#include <numeric>
#include <set>

#include "dwio/nimble/common/EncodingPrimitives.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Varint.h"
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "folly/container/F14Map.h"
#include "folly/io/Cursor.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

using SubfieldKind = velox::common::SubfieldKind;

namespace {

// Lightweight adapter for writing small sections (header, trailer) directly
// into an IOBuf. Satisfies the size()/resize()/data() interface required by
// detail::extend/writeHeader/writeTrailer. Avoids the std::string → IOBuf
// copy that would occur if writing into a temporary std::string first.
class IOBufSection {
 public:
  explicit IOBufSection(size_t initialCapacity)
      : buf_(folly::IOBuf::create(initialCapacity)) {}

  size_t size() const {
    return size_;
  }

  void resize(size_t newSize) {
    if (newSize > buf_->capacity()) {
      auto newBuf =
          folly::IOBuf::create(std::max(newSize, buf_->capacity() * 2));
      if (size_ > 0) {
        std::memcpy(newBuf->writableData(), buf_->data(), size_);
      }
      buf_ = std::move(newBuf);
    }
    size_ = newSize;
  }

  char* data() {
    return reinterpret_cast<char*>(buf_->writableData());
  }

  // Finalizes the IOBuf by setting its length and returns it.
  std::unique_ptr<folly::IOBuf> build() && {
    buf_->append(size_);
    return std::move(buf_);
  }

 private:
  std::unique_ptr<folly::IOBuf> buf_;
  size_t size_{0};
};

// Reads a varint32 from a Cursor, advancing past the encoded bytes.
uint32_t readVarint32(folly::io::Cursor& cursor) {
  uint32_t value = 0;
  uint32_t shift = 0;
  while (true) {
    auto byte = cursor.read<uint8_t>();
    value |= static_cast<uint32_t>(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return value;
    }
    shift += 7;
  }
}

// Forward declaration for recursive calls from per-type helpers.
std::shared_ptr<const Type> updateColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::Type& projectType);

// Updates row column names via positional matching with velox RowType.
std::shared_ptr<const Type> updateRowColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::RowType& veloxRow) {
  const auto& nimbleRow = inputType->asRow();
  std::vector<std::string> newNames;
  std::vector<std::shared_ptr<const Type>> newChildren;
  newNames.reserve(nimbleRow.childrenCount());
  newChildren.reserve(nimbleRow.childrenCount());

  bool changed{false};
  const auto minChildren =
      std::min<size_t>(veloxRow.size(), nimbleRow.childrenCount());
  for (size_t i = 0; i < minChildren; ++i) {
    newNames.emplace_back(veloxRow.nameOf(i));
    changed |= (newNames.back() != nimbleRow.nameAt(i));
    auto newChild =
        updateColumnNames(nimbleRow.childAt(i), *veloxRow.childAt(i));
    changed |= (newChild != nimbleRow.childAt(i));
    newChildren.emplace_back(std::move(newChild));
  }

  if (!changed) {
    return inputType;
  }

  // Keep remaining children as-is (file has more columns than table).
  newNames.insert(
      newNames.end(),
      nimbleRow.names().begin() + minChildren,
      nimbleRow.names().end());
  newChildren.insert(
      newChildren.end(),
      nimbleRow.children().begin() + minChildren,
      nimbleRow.children().end());
  return std::make_shared<RowType>(
      nimbleRow.nullsDescriptor(), std::move(newNames), std::move(newChildren));
}

// Updates column names within array elements.
// Handles both Array and ArrayWithOffsets nimble types.
std::shared_ptr<const Type> updateArrayColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::ArrayType& veloxArray) {
  if (inputType->kind() == Kind::ArrayWithOffsets) {
    const auto& array = inputType->asArrayWithOffsets();
    auto newElements =
        updateColumnNames(array.elements(), *veloxArray.elementType());
    if (newElements == array.elements()) {
      return inputType;
    }
    return std::make_shared<ArrayWithOffsetsType>(
        array.offsetsDescriptor(),
        array.lengthsDescriptor(),
        std::move(newElements));
  }
  const auto& array = inputType->asArray();
  auto newElements =
      updateColumnNames(array.elements(), *veloxArray.elementType());
  if (newElements == array.elements()) {
    return inputType;
  }
  return std::make_shared<ArrayType>(
      array.lengthsDescriptor(), std::move(newElements));
}

// Updates column names within FlatMap value types.
// FlatMap keys are data-determined, not renamed.
std::shared_ptr<const Type> updateFlatMapColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::MapType& veloxMap) {
  const auto& flatMap = inputType->asFlatMap();
  std::vector<std::shared_ptr<const Type>> newChildren;
  newChildren.reserve(flatMap.childrenCount());
  bool changed{false};
  for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
    auto newChild =
        updateColumnNames(flatMap.childAt(i), *veloxMap.valueType());
    changed |= (newChild != flatMap.childAt(i));
    newChildren.emplace_back(std::move(newChild));
  }
  if (!changed) {
    return inputType;
  }
  // Clone names and inMapDescriptors.
  std::vector<std::string> newNames;
  newNames.reserve(flatMap.childrenCount());
  std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors;
  inMapDescriptors.reserve(flatMap.childrenCount());
  for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
    newNames.emplace_back(flatMap.nameAt(i));
    const auto& desc = flatMap.inMapDescriptorAt(i);
    inMapDescriptors.emplace_back(
        std::make_unique<StreamDescriptor>(desc.offset(), desc.scalarKind()));
  }
  return std::make_shared<FlatMapType>(
      flatMap.nullsDescriptor(),
      flatMap.keyScalarKind(),
      std::move(newNames),
      std::move(inMapDescriptors),
      std::move(newChildren));
}

// Updates column names within Map/SlidingWindowMap key and value types.
std::shared_ptr<const Type> updateMapColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::MapType& veloxMap) {
  switch (inputType->kind()) {
    case Kind::SlidingWindowMap: {
      const auto& map = inputType->asSlidingWindowMap();
      auto newKeys = updateColumnNames(map.keys(), *veloxMap.keyType());
      auto newValues = updateColumnNames(map.values(), *veloxMap.valueType());
      if (newKeys == map.keys() && newValues == map.values()) {
        return inputType;
      }
      return std::make_shared<SlidingWindowMapType>(
          map.offsetsDescriptor(),
          map.lengthsDescriptor(),
          std::move(newKeys),
          std::move(newValues));
    }
    case Kind::Map: {
      const auto& map = inputType->asMap();
      auto newKeys = updateColumnNames(map.keys(), *veloxMap.keyType());
      auto newValues = updateColumnNames(map.values(), *veloxMap.valueType());
      if (newKeys == map.keys() && newValues == map.values()) {
        return inputType;
      }
      return std::make_shared<MapType>(
          map.lengthsDescriptor(), std::move(newKeys), std::move(newValues));
    }
    default:
      NIMBLE_UNREACHABLE(
          "updateMapColumnNames called with unsupported kind: {}",
          inputType->kind());
  }
}

// Updates column names in nimble schema to match projectType (velox) using
// positional matching. Same algorithm as Reader::updateColumnNames but across
// velox/nimble type trees.
std::shared_ptr<const Type> updateColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::Type& projectType) {
  switch (projectType.kind()) {
    case velox::TypeKind::ROW:
      return updateRowColumnNames(inputType, projectType.asRow());
    case velox::TypeKind::ARRAY:
      return updateArrayColumnNames(inputType, projectType.asArray());
    case velox::TypeKind::MAP:
      if (inputType->isFlatMap()) {
        return updateFlatMapColumnNames(inputType, projectType.asMap());
      }
      return updateMapColumnNames(inputType, projectType.asMap());
    default:
      return inputType;
  }
}

// Matches Projector::SelectedChildrenMap.
using SelectedChildrenMap = folly::F14FastMap<const Type*, std::set<size_t>>;

// Inserts a stream offset and asserts it's unique.
inline void insertUniqueStream(std::set<uint32_t>& indices, uint32_t offset) {
  const auto [_, inserted] = indices.insert(offset);
  NIMBLE_CHECK(inserted, "Duplicate stream offset: {}", offset);
}

// Collects all stream indices for a type subtree.
inline void collectTypeStreams(const Type& type, std::set<uint32_t>& indices) {
  switch (type.kind()) {
    case Kind::Scalar: {
      insertUniqueStream(indices, type.asScalar().scalarDescriptor().offset());
      break;
    }

    case Kind::TimestampMicroNano: {
      const auto& ts = type.asTimestampMicroNano();
      insertUniqueStream(indices, ts.microsDescriptor().offset());
      insertUniqueStream(indices, ts.nanosDescriptor().offset());
      break;
    }

    case Kind::Row: {
      const auto& row = type.asRow();
      insertUniqueStream(indices, row.nullsDescriptor().offset());
      for (size_t i = 0; i < row.childrenCount(); ++i) {
        collectTypeStreams(*row.childAt(i), indices);
      }
      break;
    }

    case Kind::Array: {
      const auto& array = type.asArray();
      insertUniqueStream(indices, array.lengthsDescriptor().offset());
      collectTypeStreams(*array.elements(), indices);
      break;
    }

    case Kind::ArrayWithOffsets: {
      const auto& array = type.asArrayWithOffsets();
      insertUniqueStream(indices, array.offsetsDescriptor().offset());
      insertUniqueStream(indices, array.lengthsDescriptor().offset());
      collectTypeStreams(*array.elements(), indices);
      break;
    }

    case Kind::Map: {
      const auto& map = type.asMap();
      insertUniqueStream(indices, map.lengthsDescriptor().offset());
      collectTypeStreams(*map.keys(), indices);
      collectTypeStreams(*map.values(), indices);
      break;
    }

    case Kind::SlidingWindowMap: {
      const auto& map = type.asSlidingWindowMap();
      insertUniqueStream(indices, map.offsetsDescriptor().offset());
      insertUniqueStream(indices, map.lengthsDescriptor().offset());
      collectTypeStreams(*map.keys(), indices);
      collectTypeStreams(*map.values(), indices);
      break;
    }

    case Kind::FlatMap: {
      const auto& flatMap = type.asFlatMap();
      insertUniqueStream(indices, flatMap.nullsDescriptor().offset());
      for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
        insertUniqueStream(indices, flatMap.inMapDescriptorAt(i).offset());
        collectTypeStreams(*flatMap.childAt(i), indices);
      }
      break;
    }

    default:
      NIMBLE_FAIL("Unsupported type kind: {}", static_cast<int>(type.kind()));
  }
}

// Counts total number of streams in a type tree.
inline size_t countTotalStreams(const Type& type) {
  std::set<uint32_t> indices;
  collectTypeStreams(type, indices);
  return indices.size();
}

// Resolves a subfield path to a type node and collects its stream indices.
// Also tracks which children are selected for building output schema.
void resolveSubfield(
    const Type* inputSchema,
    const Subfield& subfield,
    std::set<uint32_t>& indices,
    SelectedChildrenMap& selectedChildren) {
  const auto& path = subfield.path();
  NIMBLE_CHECK(!path.empty(), "Empty subfield path");

  const Type* current = inputSchema;

  // Traverse the path.
  for (const auto& element : path) {
    const auto kind = element->kind();
    if (kind == SubfieldKind::kNestedField) {
      const auto* nested = element->asChecked<Subfield::NestedField>();
      const auto& name = nested->name();

      if (current->isRow()) {
        const auto& row = current->asRow();
        const auto childIdx = row.findChild(name);
        NIMBLE_CHECK(
            childIdx.has_value(), "Field '{}' not found in RowType", name);

        // Include parent's nulls stream only on first child selection.
        const bool firstChild = selectedChildren[current].empty();
        selectedChildren[current].insert(*childIdx);
        if (firstChild) {
          indices.insert(row.nullsDescriptor().offset());
        }

        current = row.childAt(*childIdx).get();
      } else {
        NIMBLE_FAIL(
            "Cannot access nested field '{}' on non-Row type in path '{}'",
            name,
            subfield.toString());
      }
    } else if (kind == SubfieldKind::kStringSubscript) {
      const auto* subscript = element->asChecked<Subfield::StringSubscript>();
      const auto& keyName = subscript->index();

      if (current->isFlatMap()) {
        const auto& flatMap = current->asFlatMap();
        const auto childIdx = flatMap.findChild(keyName);
        NIMBLE_CHECK(
            childIdx.has_value(), "Key '{}' not found in FlatMapType", keyName);

        // Include flatmap's nulls stream only on first key selection.
        const bool firstKey = selectedChildren[current].empty();
        selectedChildren[current].insert(*childIdx);
        if (firstKey) {
          indices.insert(flatMap.nullsDescriptor().offset());
        }
        indices.insert(flatMap.inMapDescriptorAt(*childIdx).offset());

        current = flatMap.childAt(*childIdx).get();
      } else {
        NIMBLE_FAIL(
            "String subscript '{}' only supported on FlatMap in path '{}'",
            keyName,
            subfield.toString());
      }
    } else {
      NIMBLE_FAIL(
          "Unsupported subfield kind {} in path '{}'",
          static_cast<int>(kind),
          subfield.toString());
    }
  }

  // Collect all streams for the final resolved type.
  collectTypeStreams(*current, indices);
}

using OffsetMap = folly::F14FastMap<uint32_t, uint32_t>;

inline uint32_t mapOffset(const OffsetMap& offsetMap, uint32_t inputOffset) {
  const auto it = offsetMap.find(inputOffset);
  NIMBLE_CHECK(
      it != offsetMap.end(),
      "Input stream offset {} not found in offset map",
      inputOffset);
  return it->second;
}

std::shared_ptr<const Type> buildProjectedType(
    const Type* inputType,
    const OffsetMap& offsetMap,
    const SelectedChildrenMap& selectedChildren) {
  const auto kind = inputType->kind();
  switch (kind) {
    case Kind::Scalar: {
      const auto inputOffset =
          inputType->asScalar().scalarDescriptor().offset();
      return std::make_shared<ScalarType>(StreamDescriptor{
          mapOffset(offsetMap, inputOffset),
          inputType->asScalar().scalarDescriptor().scalarKind()});
    }

    case Kind::TimestampMicroNano: {
      const auto& ts = inputType->asTimestampMicroNano();
      return std::make_shared<TimestampMicroNanoType>(
          StreamDescriptor{
              mapOffset(offsetMap, ts.microsDescriptor().offset()),
              ts.microsDescriptor().scalarKind()},
          StreamDescriptor{
              mapOffset(offsetMap, ts.nanosDescriptor().offset()),
              ts.nanosDescriptor().scalarKind()});
    }

    case Kind::Row: {
      const auto& row = inputType->asRow();
      auto nullsDesc = StreamDescriptor{
          mapOffset(offsetMap, row.nullsDescriptor().offset()),
          row.nullsDescriptor().scalarKind()};

      std::vector<std::string> names;
      std::vector<std::shared_ptr<const Type>> children;

      const auto it = selectedChildren.find(inputType);
      if (it != selectedChildren.end()) {
        names.reserve(it->second.size());
        children.reserve(it->second.size());
        for (size_t idx : it->second) {
          names.emplace_back(row.nameAt(idx));
          children.emplace_back(buildProjectedType(
              row.childAt(idx).get(), offsetMap, selectedChildren));
        }
      } else {
        names.reserve(row.childrenCount());
        children.reserve(row.childrenCount());
        for (size_t i = 0; i < row.childrenCount(); ++i) {
          names.emplace_back(row.nameAt(i));
          children.emplace_back(buildProjectedType(
              row.childAt(i).get(), offsetMap, selectedChildren));
        }
      }

      return std::make_shared<RowType>(
          std::move(nullsDesc), std::move(names), std::move(children));
    }

    case Kind::Array: {
      const auto& array = inputType->asArray();
      auto lengthsDesc = StreamDescriptor{
          mapOffset(offsetMap, array.lengthsDescriptor().offset()),
          array.lengthsDescriptor().scalarKind()};
      auto elements = buildProjectedType(
          array.elements().get(), offsetMap, selectedChildren);
      return std::make_shared<ArrayType>(
          std::move(lengthsDesc), std::move(elements));
    }

    case Kind::ArrayWithOffsets: {
      const auto& array = inputType->asArrayWithOffsets();
      auto offsetsDesc = StreamDescriptor{
          mapOffset(offsetMap, array.offsetsDescriptor().offset()),
          array.offsetsDescriptor().scalarKind()};
      auto lengthsDesc = StreamDescriptor{
          mapOffset(offsetMap, array.lengthsDescriptor().offset()),
          array.lengthsDescriptor().scalarKind()};
      auto elements = buildProjectedType(
          array.elements().get(), offsetMap, selectedChildren);
      return std::make_shared<ArrayWithOffsetsType>(
          std::move(offsetsDesc), std::move(lengthsDesc), std::move(elements));
    }

    case Kind::Map: {
      const auto& map = inputType->asMap();
      auto lengthsDesc = StreamDescriptor{
          mapOffset(offsetMap, map.lengthsDescriptor().offset()),
          map.lengthsDescriptor().scalarKind()};
      auto keys =
          buildProjectedType(map.keys().get(), offsetMap, selectedChildren);
      auto values =
          buildProjectedType(map.values().get(), offsetMap, selectedChildren);
      return std::make_shared<MapType>(
          std::move(lengthsDesc), std::move(keys), std::move(values));
    }

    case Kind::SlidingWindowMap: {
      const auto& map = inputType->asSlidingWindowMap();
      auto offsetsDesc = StreamDescriptor{
          mapOffset(offsetMap, map.offsetsDescriptor().offset()),
          map.offsetsDescriptor().scalarKind()};
      auto lengthsDesc = StreamDescriptor{
          mapOffset(offsetMap, map.lengthsDescriptor().offset()),
          map.lengthsDescriptor().scalarKind()};
      auto keys =
          buildProjectedType(map.keys().get(), offsetMap, selectedChildren);
      auto values =
          buildProjectedType(map.values().get(), offsetMap, selectedChildren);
      return std::make_shared<SlidingWindowMapType>(
          std::move(offsetsDesc),
          std::move(lengthsDesc),
          std::move(keys),
          std::move(values));
    }

    case Kind::FlatMap: {
      const auto& flatMap = inputType->asFlatMap();
      auto nullsDesc = StreamDescriptor{
          mapOffset(offsetMap, flatMap.nullsDescriptor().offset()),
          flatMap.nullsDescriptor().scalarKind()};

      std::vector<std::string> names;
      std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors;
      std::vector<std::shared_ptr<const Type>> children;

      auto it = selectedChildren.find(inputType);
      if (it != selectedChildren.end()) {
        names.reserve(it->second.size());
        inMapDescriptors.reserve(it->second.size());
        children.reserve(it->second.size());
        for (size_t idx : it->second) {
          names.emplace_back(flatMap.nameAt(idx));
          children.emplace_back(buildProjectedType(
              flatMap.childAt(idx).get(), offsetMap, selectedChildren));
          inMapDescriptors.emplace_back(
              std::make_unique<StreamDescriptor>(
                  mapOffset(offsetMap, flatMap.inMapDescriptorAt(idx).offset()),
                  flatMap.inMapDescriptorAt(idx).scalarKind()));
        }
      } else {
        names.reserve(flatMap.childrenCount());
        inMapDescriptors.reserve(flatMap.childrenCount());
        children.reserve(flatMap.childrenCount());
        for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
          names.emplace_back(flatMap.nameAt(i));
          children.emplace_back(buildProjectedType(
              flatMap.childAt(i).get(), offsetMap, selectedChildren));
          inMapDescriptors.emplace_back(
              std::make_unique<StreamDescriptor>(
                  mapOffset(offsetMap, flatMap.inMapDescriptorAt(i).offset()),
                  flatMap.inMapDescriptorAt(i).scalarKind()));
        }
      }

      return std::make_shared<FlatMapType>(
          std::move(nullsDesc),
          flatMap.keyScalarKind(),
          std::move(names),
          std::move(inMapDescriptors),
          std::move(children));
    }

    default:
      NIMBLE_FAIL("Unsupported type kind for output schema: {}", kind);
  }
}

} // namespace

void Projector::buildProjectedSchema(
    const SelectedChildrenMap& selectedChildren) {
  NIMBLE_CHECK_NULL(projectedSchema_, "Projected schema already built");

  folly::F14FastMap<uint32_t, uint32_t> offsetMap;
  offsetMap.reserve(inputStreamIndices_.size());
  for (uint32_t i = 0; i < inputStreamIndices_.size(); ++i) {
    offsetMap[inputStreamIndices_[i]] = i;
  }
  projectedSchema_ =
      buildProjectedType(inputSchema_.get(), offsetMap, selectedChildren);

  // Sanity check: verify all offsets were used by validating projected schema
  // stream count matches selected input streams.
  std::set<uint32_t> projectedIndices;
  collectTypeStreams(*projectedSchema_, projectedIndices);
  NIMBLE_CHECK_EQ(
      projectedIndices.size(),
      inputStreamIndices_.size(),
      "Stream count mismatch");
}

Projector::Projector(
    std::shared_ptr<const Type> inputSchema,
    const std::vector<Subfield>& projectSubfields,
    velox::memory::MemoryPool* pool,
    Options options)
    : pool_(pool),
      options_(std::move(options)),
      streamSizesEncodingBuffer_(*pool, /*initialChunkSize=*/4096),
      inputSchema_(std::move(inputSchema)) {
  NIMBLE_CHECK_NOT_NULL(pool_, "Memory pool cannot be null");
  NIMBLE_CHECK_NOT_NULL(inputSchema_, "Input schema cannot be null");
  NIMBLE_CHECK(
      inputSchema_->isRow(),
      "Input schema must be a RowType, got: {}",
      inputSchema_->kind());
  NIMBLE_CHECK(!projectSubfields.empty(), "Must project at least one subfield");
  NIMBLE_CHECK(
      isCompactFormat(options_.projectVersion),
      "Projection output version must be kCompact or kCompactRaw, got: {}",
      options_.projectVersion);

  // Update inputSchema_ with projectType names for schema evolution.
  if (options_.projectType) {
    inputSchema_ = updateColumnNames(inputSchema_, *options_.projectType);
  }

  // Build projection plan: resolve subfields and collect stream indices.
  std::set<uint32_t> uniqueIndices;
  SelectedChildrenMap selectedChildren;

  for (const auto& subfield : projectSubfields) {
    NIMBLE_CHECK(subfield.valid(), "Invalid subfield: {}", subfield.toString());
    resolveSubfield(
        inputSchema_.get(), subfield, uniqueIndices, selectedChildren);
  }

  // Assign sorted indices for sequential access during projection.
  inputStreamIndices_.assign(uniqueIndices.begin(), uniqueIndices.end());

  buildProjectedSchema(selectedChildren);

  // Check if all streams are selected (enables pass-through optimization).
  passThrough_ =
      (inputStreamIndices_.size() == countTotalStreams(*inputSchema_));
}

namespace {

// Validates the input version header is kCompact or kCompactRaw.
SerializationVersion getAndValidateInputVersion(const folly::IOBuf& input) {
  const auto version = static_cast<SerializationVersion>(*input.data());
  NIMBLE_CHECK(
      isCompactFormat(version),
      "Input must be kCompact or kCompactRaw format, got: {}",
      version);
  return version;
}

// Projects selected streams from a contiguous IOBuf into the output chain.
// Merges contiguous selected streams into a single zero-copy IOBuf clone to
// minimize allocations and chain linking overhead. The input IOBuf must outlive
// the output chain.
std::vector<uint32_t> projectStreamsContiguous(
    const folly::IOBuf& input,
    size_t streamOffset,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<uint32_t>& selectedIndices,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(selectedIndices.size(), 0);

  // Track the start offset and byte count of a contiguous run.
  size_t runStart = 0;
  size_t runBytes = 0;

  auto flushRun = [&]() {
    if (runBytes == 0) {
      return;
    }
    // Zero-copy: clone the input and trim to the run's sub-range.
    auto buf = input.cloneOne();
    buf->trimStart(runStart);
    buf->trimEnd(buf->length() - runBytes);
    output->appendToChain(std::move(buf));
    runBytes = 0;
  };

  size_t nextSelectedIdx = 0;
  size_t offset = streamOffset;
  for (size_t i = 0;
       i < streamSizes.size() && nextSelectedIdx < selectedIndices.size();
       ++i) {
    if (i == selectedIndices[nextSelectedIdx]) {
      if (streamSizes[i] > 0) {
        outputSizes[nextSelectedIdx] = streamSizes[i];
        if (runBytes == 0) {
          runStart = offset;
        }
        runBytes += streamSizes[i];
      }
      ++nextSelectedIdx;
    } else if (streamSizes[i] > 0 && runBytes > 0) {
      flushRun();
    }
    offset += streamSizes[i];
  }
  flushRun();

  return outputSizes;
}

// Projects selected streams from a chained IOBuf via cursor.
// Merges contiguous selected streams into zero-copy IOBuf clones.
std::vector<uint32_t> projectStreamsChained(
    folly::io::Cursor& cursor,
    const std::vector<uint32_t>& streamSizes,
    const std::vector<uint32_t>& selectedIndices,
    std::unique_ptr<folly::IOBuf>& output) {
  std::vector<uint32_t> outputSizes(selectedIndices.size(), 0);
  size_t nextSelectedIdx = 0;
  size_t runBytes = 0;

  auto flushRun = [&]() {
    if (runBytes == 0) {
      return;
    }
    // Zero-copy: clone the run's sub-range from the cursor, sharing the
    // underlying IOBuf buffers via refcounting.
    std::unique_ptr<folly::IOBuf> buf;
    cursor.clone(buf, runBytes);
    output->appendToChain(std::move(buf));
    runBytes = 0;
  };

  for (size_t i = 0;
       i < streamSizes.size() && nextSelectedIdx < selectedIndices.size();
       ++i) {
    if (i == selectedIndices[nextSelectedIdx]) {
      if (streamSizes[i] > 0) {
        outputSizes[nextSelectedIdx] = streamSizes[i];
        runBytes += streamSizes[i];
      }
      ++nextSelectedIdx;
    } else if (streamSizes[i] > 0) {
      if (runBytes > 0) {
        flushRun();
      }
      cursor.skip(streamSizes[i]);
    }
  }
  flushRun();
  return outputSizes;
}

} // namespace

folly::IOBuf Projector::buildProjectedOutput(
    const std::vector<uint32_t>& outputStreamSizes,
    std::unique_ptr<folly::IOBuf> output) const {
  IOBufSection trailer(
      detail::estimateTrailerSize(
          options_.projectVersion,
          outputStreamSizes.size(),
          options_.streamSizesEncodingType));
  detail::writeTrailer(
      options_.projectVersion,
      outputStreamSizes,
      options_.streamSizesEncodingType,
      streamSizesEncodingBuffer_,
      trailer);
  output->appendToChain(std::move(trailer).build());
  return std::move(*output);
}

folly::IOBuf Projector::project(const folly::IOBuf& input) const {
  const auto inputVersion = getAndValidateInputVersion(input);

  // Fast path: pass-through when all streams selected.
  if (passThrough_) {
    return input.cloneAsValue();
  }

  if (!input.isChained()) {
    return projectContiguous(input, inputVersion);
  }
  return projectChained(input, inputVersion);
}

folly::IOBuf Projector::projectContiguous(
    const folly::IOBuf& input,
    SerializationVersion inputVersion) const {
  const auto* data = reinterpret_cast<const char*>(input.data());
  // Skip version byte (already validated and passed in).
  const auto* pos = data + sizeof(uint8_t);
  const uint32_t rowCount = varint::readVarint32(&pos);

  // Build header: [version byte][varint rowCount].
  IOBufSection header(
      detail::estimateHeaderSize(options_.projectVersion, rowCount));
  detail::writeHeader(header, options_.projectVersion, rowCount);
  auto output = std::move(header).build();

  const auto inputStreamSizes =
      detail::readStreamSizes(input, inputVersion, pool_);

  // Extract selected streams as zero-copy sub-range clones.
  auto outputStreamSizes = projectStreamsContiguous(
      input,
      /*streamOffset=*/pos - data,
      inputStreamSizes,
      inputStreamIndices_,
      output);

  return buildProjectedOutput(outputStreamSizes, std::move(output));
}

folly::IOBuf Projector::projectChained(
    const folly::IOBuf& input,
    SerializationVersion inputVersion) const {
  folly::io::Cursor cursor(&input);
  // Skip version byte (already validated and passed in).
  cursor.skip(sizeof(uint8_t));
  const uint32_t rowCount = readVarint32(cursor);

  // Build header: [version byte][varint rowCount].
  IOBufSection header(
      detail::estimateHeaderSize(options_.projectVersion, rowCount));
  detail::writeHeader(header, options_.projectVersion, rowCount);
  auto output = std::move(header).build();

  const auto inputStreamSizes =
      detail::readStreamSizes(input, inputVersion, pool_);

  // Extract selected streams as zero-copy clones via cursor.
  auto outputStreamSizes = projectStreamsChained(
      cursor, inputStreamSizes, inputStreamIndices_, output);

  return buildProjectedOutput(outputStreamSizes, std::move(output));
}

folly::IOBuf Projector::project(std::string_view input) const {
  return project(folly::IOBuf::wrapBufferAsValue(input.data(), input.size()));
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<std::string_view>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

std::vector<folly::IOBuf> Projector::project(
    const std::vector<folly::IOBuf>& inputs) const {
  std::vector<folly::IOBuf> results;
  results.reserve(inputs.size());
  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

} // namespace facebook::nimble::serde
