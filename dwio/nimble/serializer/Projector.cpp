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
#include "dwio/nimble/serializer/SerializerImpl.h"
#include "folly/container/F14Map.h"
#include "velox/type/Type.h"

namespace facebook::nimble::serde {

using SubfieldKind = velox::common::SubfieldKind;

namespace {

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
  const size_t minChildren =
      std::min<size_t>(veloxRow.size(), nimbleRow.childrenCount());
  for (size_t i = 0; i < minChildren; ++i) {
    newNames.emplace_back(veloxRow.nameOf(i));
    if (newNames.back() != nimbleRow.nameAt(i)) {
      changed = true;
    }
    auto newChild =
        updateColumnNames(nimbleRow.childAt(i), *veloxRow.childAt(i));
    if (newChild != nimbleRow.childAt(i)) {
      changed = true;
    }
    newChildren.emplace_back(std::move(newChild));
  }

  if (!changed) {
    return inputType;
  }

  // Keep remaining children as-is (file has more columns than table).
  for (size_t i = minChildren; i < nimbleRow.childrenCount(); ++i) {
    newNames.emplace_back(nimbleRow.nameAt(i));
    newChildren.emplace_back(nimbleRow.childAt(i));
  }
  return std::make_shared<RowType>(
      nimbleRow.nullsDescriptor(), std::move(newNames), std::move(newChildren));
}

// Updates column names within array elements.
// Handles both Array and ArrayWithOffsets nimble types.
std::shared_ptr<const Type> updateArrayColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::ArrayType& veloxArray) {
  const auto isArrayWithOffsets = inputType->kind() == Kind::ArrayWithOffsets;
  const auto& elements = isArrayWithOffsets
      ? inputType->asArrayWithOffsets().elements()
      : inputType->asArray().elements();
  auto newElements = updateColumnNames(elements, *veloxArray.elementType());
  if (newElements == elements) {
    return inputType;
  }
  if (isArrayWithOffsets) {
    const auto& array = inputType->asArrayWithOffsets();
    return std::make_shared<ArrayWithOffsetsType>(
        array.offsetsDescriptor(),
        array.lengthsDescriptor(),
        std::move(newElements));
  }
  return std::make_shared<ArrayType>(
      inputType->asArray().lengthsDescriptor(), std::move(newElements));
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
    if (newChild != flatMap.childAt(i)) {
      changed = true;
    }
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
    inMapDescriptors.emplace_back(
        std::make_unique<StreamDescriptor>(
            flatMap.inMapDescriptorAt(i).offset(),
            flatMap.inMapDescriptorAt(i).scalarKind()));
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
  NIMBLE_CHECK_NE(inputType->kind(), Kind::FlatMap);
  const auto isSlidingWindow = inputType->kind() == Kind::SlidingWindowMap;
  const auto& keys = isSlidingWindow ? inputType->asSlidingWindowMap().keys()
                                     : inputType->asMap().keys();
  const auto& values = isSlidingWindow
      ? inputType->asSlidingWindowMap().values()
      : inputType->asMap().values();
  auto newKeys = updateColumnNames(keys, *veloxMap.keyType());
  auto newValues = updateColumnNames(values, *veloxMap.valueType());
  if (newKeys == keys && newValues == values) {
    return inputType;
  }
  if (isSlidingWindow) {
    const auto& map = inputType->asSlidingWindowMap();
    return std::make_shared<SlidingWindowMapType>(
        map.offsetsDescriptor(),
        map.lengthsDescriptor(),
        std::move(newKeys),
        std::move(newValues));
  }
  return std::make_shared<MapType>(
      inputType->asMap().lengthsDescriptor(),
      std::move(newKeys),
      std::move(newValues));
}

// Updates column names in nimble schema to match projectType (velox) using
// positional matching. Same algorithm as Reader::updateColumnNames but across
// velox/nimble type trees.
std::shared_ptr<const Type> updateColumnNames(
    const std::shared_ptr<const Type>& inputType,
    const velox::Type& projectType) {
  if (projectType.isPrimitiveType()) {
    return inputType;
  }
  if (projectType.isRow()) {
    return updateRowColumnNames(inputType, projectType.asRow());
  }
  if (projectType.isArray()) {
    return updateArrayColumnNames(inputType, projectType.asArray());
  }
  if (projectType.isMap()) {
    if (inputType->isFlatMap()) {
      return updateFlatMapColumnNames(inputType, projectType.asMap());
    }
    return updateMapColumnNames(inputType, projectType.asMap());
  }
  return inputType;
}

// Tracks which children are selected at each RowType/FlatMapType.
// Key: pointer to Type node, Value: set of selected child indices.
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

// Forward declaration for recursive call.
std::shared_ptr<const Type> buildProjectedSchema(
    const Type* inputType,
    uint32_t& nextStreamOffset,
    const SelectedChildrenMap& selectedChildren);

// Builds the projected schema with only selected fields.
std::shared_ptr<const Type> buildProjectedSchema(
    const Type* inputType,
    uint32_t& nextStreamOffset,
    const SelectedChildrenMap& selectedChildren) {
  const auto kind = inputType->kind();
  switch (kind) {
    case Kind::Scalar: {
      return std::make_shared<ScalarType>(StreamDescriptor{
          nextStreamOffset++,
          inputType->asScalar().scalarDescriptor().scalarKind()});
    }

    case Kind::TimestampMicroNano: {
      const auto& ts = inputType->asTimestampMicroNano();
      auto microsOffset = nextStreamOffset++;
      auto nanosOffset = nextStreamOffset++;
      return std::make_shared<TimestampMicroNanoType>(
          StreamDescriptor{microsOffset, ts.microsDescriptor().scalarKind()},
          StreamDescriptor{nanosOffset, ts.nanosDescriptor().scalarKind()});
    }

    case Kind::Row: {
      const auto& row = inputType->asRow();
      auto nullsDesc = StreamDescriptor{
          nextStreamOffset++, row.nullsDescriptor().scalarKind()};

      std::vector<std::string> names;
      std::vector<std::shared_ptr<const Type>> children;

      // Check if we have selected children for this type.
      const auto it = selectedChildren.find(inputType);
      if (it != selectedChildren.end()) {
        // Only include selected children.
        names.reserve(it->second.size());
        children.reserve(it->second.size());
        for (size_t idx : it->second) {
          names.emplace_back(row.nameAt(idx));
          children.emplace_back(buildProjectedSchema(
              row.childAt(idx).get(), nextStreamOffset, selectedChildren));
        }
      } else {
        // Include all children (this is a nested type within a projected
        // field).
        names.reserve(row.childrenCount());
        children.reserve(row.childrenCount());
        for (size_t i = 0; i < row.childrenCount(); ++i) {
          names.emplace_back(row.nameAt(i));
          children.emplace_back(buildProjectedSchema(
              row.childAt(i).get(), nextStreamOffset, selectedChildren));
        }
      }

      return std::make_shared<RowType>(
          std::move(nullsDesc), std::move(names), std::move(children));
    }

    case Kind::Array: {
      const auto& array = inputType->asArray();
      auto lengthsDesc = StreamDescriptor{
          nextStreamOffset++, array.lengthsDescriptor().scalarKind()};
      auto elements = buildProjectedSchema(
          array.elements().get(), nextStreamOffset, selectedChildren);
      return std::make_shared<ArrayType>(
          std::move(lengthsDesc), std::move(elements));
    }

    case Kind::ArrayWithOffsets: {
      const auto& array = inputType->asArrayWithOffsets();
      auto offsetsDesc = StreamDescriptor{
          nextStreamOffset++, array.offsetsDescriptor().scalarKind()};
      auto lengthsDesc = StreamDescriptor{
          nextStreamOffset++, array.lengthsDescriptor().scalarKind()};
      auto elements = buildProjectedSchema(
          array.elements().get(), nextStreamOffset, selectedChildren);
      return std::make_shared<ArrayWithOffsetsType>(
          std::move(offsetsDesc), std::move(lengthsDesc), std::move(elements));
    }

    case Kind::Map: {
      const auto& map = inputType->asMap();
      auto lengthsDesc = StreamDescriptor{
          nextStreamOffset++, map.lengthsDescriptor().scalarKind()};
      auto keys = buildProjectedSchema(
          map.keys().get(), nextStreamOffset, selectedChildren);
      auto values = buildProjectedSchema(
          map.values().get(), nextStreamOffset, selectedChildren);
      return std::make_shared<MapType>(
          std::move(lengthsDesc), std::move(keys), std::move(values));
    }

    case Kind::SlidingWindowMap: {
      const auto& map = inputType->asSlidingWindowMap();
      auto offsetsDesc = StreamDescriptor{
          nextStreamOffset++, map.offsetsDescriptor().scalarKind()};
      auto lengthsDesc = StreamDescriptor{
          nextStreamOffset++, map.lengthsDescriptor().scalarKind()};
      auto keys = buildProjectedSchema(
          map.keys().get(), nextStreamOffset, selectedChildren);
      auto values = buildProjectedSchema(
          map.values().get(), nextStreamOffset, selectedChildren);
      return std::make_shared<SlidingWindowMapType>(
          std::move(offsetsDesc),
          std::move(lengthsDesc),
          std::move(keys),
          std::move(values));
    }

    case Kind::FlatMap: {
      const auto& flatMap = inputType->asFlatMap();
      auto nullsDesc = StreamDescriptor{
          nextStreamOffset++, flatMap.nullsDescriptor().scalarKind()};

      std::vector<std::string> names;
      std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors;
      std::vector<std::shared_ptr<const Type>> children;

      auto it = selectedChildren.find(inputType);
      if (it != selectedChildren.end()) {
        // Only include selected keys.
        names.reserve(it->second.size());
        inMapDescriptors.reserve(it->second.size());
        children.reserve(it->second.size());
        for (size_t idx : it->second) {
          names.emplace_back(flatMap.nameAt(idx));
          // Child value streams are allocated BEFORE inMap in the Serializer.
          // Match this order when assigning output stream offsets.
          children.emplace_back(buildProjectedSchema(
              flatMap.childAt(idx).get(), nextStreamOffset, selectedChildren));
          inMapDescriptors.emplace_back(
              std::make_unique<StreamDescriptor>(
                  nextStreamOffset++,
                  flatMap.inMapDescriptorAt(idx).scalarKind()));
        }
      } else {
        // Include all children.
        names.reserve(flatMap.childrenCount());
        inMapDescriptors.reserve(flatMap.childrenCount());
        children.reserve(flatMap.childrenCount());
        for (size_t i = 0; i < flatMap.childrenCount(); ++i) {
          names.emplace_back(flatMap.nameAt(i));
          // Child value streams are allocated BEFORE inMap in the Serializer.
          children.emplace_back(buildProjectedSchema(
              flatMap.childAt(i).get(), nextStreamOffset, selectedChildren));
          inMapDescriptors.emplace_back(
              std::make_unique<StreamDescriptor>(
                  nextStreamOffset++,
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

Projector::Projector(
    std::shared_ptr<const Type> inputSchema,
    const std::vector<Subfield>& projectSubfields,
    Options options)
    : options_(std::move(options)), inputSchema_(std::move(inputSchema)) {
  NIMBLE_CHECK_NOT_NULL(inputSchema_, "Input schema cannot be null");
  NIMBLE_CHECK(
      inputSchema_->isRow(),
      "Input schema must be a RowType, got: {}",
      inputSchema_->kind());
  NIMBLE_CHECK(!projectSubfields.empty(), "Must project at least one subfield");

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

  // Build output schema with compact stream offsets.
  uint32_t nextStreamOffset{0};
  projectedSchema_ = buildProjectedSchema(
      inputSchema_.get(), nextStreamOffset, selectedChildren);

  // Sanity check: output stream count must match selected input streams.
  NIMBLE_CHECK_EQ(
      nextStreamOffset, inputStreamIndices_.size(), "Stream count mismatch");

  // Check if all streams are selected (enables pass-through optimization).
  passThrough_ =
      (inputStreamIndices_.size() == countTotalStreams(*inputSchema_));
}

std::string Projector::project(std::string_view input) const {
  const char* pos = input.data();
  const char* end = input.data() + input.size();

  // Parse input header.
  SerializationVersion inputVersion = SerializationVersion::kDense;
  if (options_.inputHasVersionHeader) {
    inputVersion = static_cast<SerializationVersion>(*pos++);
  }

  // Verify input/output format compatibility.
  // Projector copies raw bytes, so encoding type must match:
  // - kDense/kSparse use legacy raw encoding
  // - kDenseEncoded/kSparseEncoded use nimble encoding
  const bool inputEncoded =
      (inputVersion == SerializationVersion::kDenseEncoded ||
       inputVersion == SerializationVersion::kSparseEncoded);
  const bool outputEncoded =
      (options_.projectVersion == SerializationVersion::kDenseEncoded ||
       options_.projectVersion == SerializationVersion::kSparseEncoded);
  NIMBLE_CHECK_EQ(
      inputEncoded,
      outputEncoded,
      "Incompatible input/output formats: input={}, output={}. "
      "Cannot project between raw and encoded formats.",
      static_cast<int>(inputVersion),
      static_cast<int>(options_.projectVersion));

  // Fast path: pass-through when all streams selected and formats match.
  if (passThrough_ && options_.inputHasVersionHeader &&
      inputVersion == options_.projectVersion) {
    return std::string(input);
  }

  const uint32_t rowCount = encoding::readUint32(pos);

  // Parse all input streams using shared implementation.
  const auto inputStreams = detail::parseStreams(pos, end, inputVersion);

  // Collect streams to project.
  // For sparse output, only include non-empty streams.
  // For dense output, include all streams (empty data written as size=0).
  const bool inputSparse =
      (inputVersion == SerializationVersion::kSparse ||
       inputVersion == SerializationVersion::kSparseEncoded);
  const bool outputSparse =
      (options_.projectVersion == SerializationVersion::kSparse ||
       options_.projectVersion == SerializationVersion::kSparseEncoded);

  std::vector<std::pair<uint32_t, std::string_view>> streamsToWrite;
  streamsToWrite.reserve(inputStreamIndices_.size());

  for (uint32_t outputIdx = 0; outputIdx < inputStreamIndices_.size();
       ++outputIdx) {
    const uint32_t inputIdx = inputStreamIndices_[outputIdx];

    // For sparse input, stream may not exist (missing or 0-row input).
    // For dense input, all streams are present.
    std::string_view data;
    if (inputSparse) {
      if (inputIdx < inputStreams.size()) {
        data = inputStreams[inputIdx];
      }
      // Skip empty streams for sparse output.
      if (outputSparse && data.empty()) {
        continue;
      }
    } else {
      // Dense input: all streams must be present.
      NIMBLE_CHECK_LT(
          inputIdx,
          inputStreams.size(),
          "Stream index {} out of range (have {} streams)",
          inputIdx,
          inputStreams.size());
      data = inputStreams[inputIdx];
    }

    streamsToWrite.emplace_back(outputIdx, data);
  }

  // Build output buffer.
  std::string output;
  output.reserve(input.size()); // Pessimistic reserve.

  // Build stream offsets for sparse format.
  std::vector<uint32_t> streamOffsets;
  if (outputSparse) {
    streamOffsets.reserve(streamsToWrite.size());
    for (const auto& [outputIdx, _] : streamsToWrite) {
      streamOffsets.emplace_back(outputIdx);
    }
  }

  // Write header using shared implementation.
  detail::writeHeader(output, options_.projectVersion, rowCount, streamOffsets);

  // Copy projected streams.
  for (const auto& [_, data] : streamsToWrite) {
    detail::writeStream(output, data);
  }

  return output;
}

std::vector<std::string> Projector::project(
    const std::vector<std::string_view>& inputs) const {
  std::vector<std::string> results;
  results.reserve(inputs.size());

  for (const auto& input : inputs) {
    results.emplace_back(project(input));
  }
  return results;
}

} // namespace facebook::nimble::serde
