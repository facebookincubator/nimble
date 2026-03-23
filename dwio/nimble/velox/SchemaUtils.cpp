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
#include "dwio/nimble/velox/SchemaUtils.h"

#include <set>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "folly/container/F14Map.h"

namespace facebook::nimble {

using SubfieldKind = velox::common::SubfieldKind;
using Subfield = velox::common::Subfield;

//
// Common helpers shared by multiple public APIs.
//

namespace {

velox::TypePtr convertToVeloxScalarType(ScalarKind scalarKind) {
  switch (scalarKind) {
    case ScalarKind::Int8:
      return velox::ScalarType<velox::TypeKind::TINYINT>::create();
    case ScalarKind::Int16:
      return velox::ScalarType<velox::TypeKind::SMALLINT>::create();
    case ScalarKind::Int32:
      return velox::ScalarType<velox::TypeKind::INTEGER>::create();
    case ScalarKind::Int64:
      return velox::ScalarType<velox::TypeKind::BIGINT>::create();
    case ScalarKind::Float:
      return velox::ScalarType<velox::TypeKind::REAL>::create();
    case ScalarKind::Double:
      return velox::ScalarType<velox::TypeKind::DOUBLE>::create();
    case ScalarKind::Bool:
      return velox::ScalarType<velox::TypeKind::BOOLEAN>::create();
    case ScalarKind::String:
      return velox::ScalarType<velox::TypeKind::VARCHAR>::create();
    case ScalarKind::Binary:
      return velox::ScalarType<velox::TypeKind::VARBINARY>::create();
    case ScalarKind::UInt8:
    case ScalarKind::UInt16:
    case ScalarKind::UInt32:
    case ScalarKind::UInt64:
    case ScalarKind::Undefined:
      NIMBLE_UNSUPPORTED(
          "Scalar kind {} is not supported by Velox.", toString(scalarKind));
  }
  NIMBLE_UNREACHABLE("Unknown scalarKind: {}.", toString(scalarKind));
}

std::shared_ptr<TypeBuilder> convertToNimbleType(
    SchemaBuilder& builder,
    const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return builder.createScalarTypeBuilder(ScalarKind::Bool);
    case velox::TypeKind::TINYINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int8);
    case velox::TypeKind::SMALLINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int16);
    case velox::TypeKind::INTEGER:
      return builder.createScalarTypeBuilder(ScalarKind::Int32);
    case velox::TypeKind::BIGINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int64);
    case velox::TypeKind::REAL:
      return builder.createScalarTypeBuilder(ScalarKind::Float);
    case velox::TypeKind::DOUBLE:
      return builder.createScalarTypeBuilder(ScalarKind::Double);
    case velox::TypeKind::VARCHAR:
      return builder.createScalarTypeBuilder(ScalarKind::String);
    case velox::TypeKind::VARBINARY:
      return builder.createScalarTypeBuilder(ScalarKind::Binary);
    case velox::TypeKind::TIMESTAMP:
      return builder.createTimestampMicroNanoTypeBuilder();
    case velox::TypeKind::ARRAY: {
      auto& arrayType = type.asArray();
      auto nimbleType = builder.createArrayTypeBuilder();
      nimbleType->setChildren(
          convertToNimbleType(builder, *arrayType.elementType()));
      return nimbleType;
    }
    case velox::TypeKind::MAP: {
      auto& mapType = type.asMap();
      auto nimbleType = builder.createMapTypeBuilder();
      auto key = convertToNimbleType(builder, *mapType.keyType());
      auto value = convertToNimbleType(builder, *mapType.valueType());
      nimbleType->setChildren(key, value);
      return nimbleType;
    }
    case velox::TypeKind::ROW: {
      auto& rowType = type.asRow();
      auto nimbleType = builder.createRowTypeBuilder(rowType.size());
      for (size_t i = 0; i < rowType.size(); ++i) {
        nimbleType->addChild(
            rowType.nameOf(i),
            convertToNimbleType(builder, *rowType.childAt(i)));
      }
      return nimbleType;
    }
    default:
      NIMBLE_UNSUPPORTED("Unsupported type kind {}.", type.kind());
  }
}

} // namespace

//
// Simple type conversions.
//

velox::TypePtr convertToVeloxType(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar: {
      return convertToVeloxScalarType(
          type.asScalar().scalarDescriptor().scalarKind());
    }
    case Kind::TimestampMicroNano: {
      return velox::ScalarType<velox::TypeKind::TIMESTAMP>::create();
    }
    case Kind::Row: {
      const auto& rowType = type.asRow();
      std::vector<std::string> names;
      std::vector<velox::TypePtr> children;
      names.reserve(rowType.childrenCount());
      children.reserve(rowType.childrenCount());
      for (size_t i = 0; i < rowType.childrenCount(); ++i) {
        names.push_back(rowType.nameAt(i));
        children.push_back(convertToVeloxType(*rowType.childAt(i)));
      }
      return std::make_shared<const velox::RowType>(
          std::move(names), std::move(children));
    }
    case Kind::Array: {
      const auto& arrayType = type.asArray();
      return std::make_shared<const velox::ArrayType>(
          convertToVeloxType(*arrayType.elements()));
    }
    case Kind::ArrayWithOffsets: {
      const auto& arrayWithOffsetsType = type.asArrayWithOffsets();
      return std::make_shared<const velox::ArrayType>(
          convertToVeloxType(*arrayWithOffsetsType.elements()));
    }
    case Kind::Map: {
      const auto& mapType = type.asMap();
      return std::make_shared<const velox::MapType>(
          convertToVeloxType(*mapType.keys()),
          convertToVeloxType(*mapType.values()));
    }
    case Kind::SlidingWindowMap: {
      const auto& mapType = type.asSlidingWindowMap();
      return std::make_shared<const velox::MapType>(
          convertToVeloxType(*mapType.keys()),
          convertToVeloxType(*mapType.values()));
    }
    case Kind::FlatMap: {
      const auto& flatMapType = type.asFlatMap();
      return std::make_shared<const velox::MapType>(
          convertToVeloxScalarType(flatMapType.keyScalarKind()),
          // When Flatmap is empty, we always insert dummy key/value
          // to it, so it is guaranteed that flatMapType.childAt(0)
          // is always valid.
          convertToVeloxType(*flatMapType.childAt(0)));
    }
    default:
      NIMBLE_UNREACHABLE("Unknown type kind {}.", toString(type.kind()));
  }
}

std::shared_ptr<const Type> convertToNimbleType(const velox::Type& type) {
  SchemaBuilder builder;
  convertToNimbleType(builder, type);
  return SchemaReader::getSchema(builder.schemaNodes());
}

//
// buildProjectedNimbleType and its helpers.
//

namespace {

ScalarKind toScalarKind(velox::TypeKind kind) {
  switch (kind) {
    case velox::TypeKind::BOOLEAN:
      return ScalarKind::Bool;
    case velox::TypeKind::TINYINT:
      return ScalarKind::Int8;
    case velox::TypeKind::SMALLINT:
      return ScalarKind::Int16;
    case velox::TypeKind::INTEGER:
      return ScalarKind::Int32;
    case velox::TypeKind::BIGINT:
      return ScalarKind::Int64;
    case velox::TypeKind::REAL:
      return ScalarKind::Float;
    case velox::TypeKind::DOUBLE:
      return ScalarKind::Double;
    case velox::TypeKind::VARCHAR:
      return ScalarKind::String;
    case velox::TypeKind::VARBINARY:
      return ScalarKind::Binary;
    default:
      NIMBLE_FAIL("Unsupported velox TypeKind for nimble ScalarKind: {}", kind);
  }
}

// Tracks which children to include at each Row/Map node during velox type
// projection. Same pattern as NimbleSelectedChildrenMap (nimble type) but keyed
// by velox type pointer. When a node appears in this map, only its selected
// children are included. When absent, all children are included.
//
// For ROW types, children are indexed by position (matching velox RowType).
// For MAP types, children are keyed by name (stored as strings in a separate
// map) — since Map children don't have positional indices, we use a name-based
// map and convert to FlatMap during nimble type construction.
using VeloxSelectedChildrenMap =
    folly::F14FastMap<const velox::Type*, std::set<std::string>>;

// Resolves a single subfield path against a velox type tree, populating
// selectedChildren with which children to include at each Row/Map node.
// Similar to resolveSubfield for nimble types but walks velox types instead.
void resolveVeloxSubfield(
    const velox::Type* type,
    const velox::common::Subfield& subfield,
    VeloxSelectedChildrenMap& selectedChildren) {
  const auto& path = subfield.path();
  NIMBLE_CHECK(!path.empty(), "Empty subfield path");

  const auto* current = type;

  for (const auto& element : path) {
    const auto kind = element->kind();
    if (kind == SubfieldKind::kNestedField) {
      const auto& name = element->asChecked<Subfield::NestedField>()->name();

      NIMBLE_CHECK_EQ(
          current->kind(),
          velox::TypeKind::ROW,
          "Cannot access nested field '{}' on non-ROW type in subfield path '{}'",
          name,
          subfield.toString());
      const auto& row = current->asRow();
      auto childIdx = row.getChildIdxIfExists(name);
      NIMBLE_CHECK(
          childIdx.has_value(),
          "Field '{}' not found in RowType: {}",
          name,
          row.toString());

      selectedChildren[current].insert(name);
      current = row.childAt(*childIdx).get();
    } else if (
        kind == SubfieldKind::kStringSubscript ||
        kind == SubfieldKind::kLongSubscript) {
      const auto keyName = (kind == SubfieldKind::kStringSubscript)
          ? element->asChecked<Subfield::StringSubscript>()->index()
          : std::to_string(
                element->asChecked<Subfield::LongSubscript>()->index());

      NIMBLE_CHECK_EQ(
          current->kind(),
          velox::TypeKind::MAP,
          "Subscript '{}' only supported on MAP type in subfield path '{}'",
          keyName,
          subfield.toString());

      selectedChildren[current].insert(keyName);
      // All map keys share the same value type, so descend into it.
      current = current->asMap().valueType().get();
    } else {
      NIMBLE_FAIL(
          "Unsupported subfield kind {} in subfield path '{}'",
          kind,
          subfield.toString());
    }
  }
}

// Recursively converts a velox type to a projected nimble type. Handles both
// projected nodes (in selectedChildren) and full subtree conversion, with
// encoding-hint-aware type creation for top-level children.
//
// - If the node is in selectedChildren: ROW creates a projected Row with only
//   selected children; MAP creates a FlatMap with selected keys sorted
//   alphabetically.
// - If the node is NOT in selectedChildren: converts the full subtree.
//   Encoding hints (useDictionaryArray, useDeduplicatedMap, useFlatMap) create
//   ArrayWithOffsets, SlidingWindowMap, or FlatMap respectively. These flags
//   are only set for top-level children by the caller; recursive calls always
//   pass false.
//
// When useFlatMap is true and the MAP column is projected without key
// subscripts, fails because key names/order cannot be recovered from the
// velox type alone.
std::shared_ptr<TypeBuilder> buildProjectedNimbleType(
    SchemaBuilder& builder,
    const velox::Type& type,
    const VeloxSelectedChildrenMap& selectedChildren,
    bool useDictionaryArray = false,
    bool useDeduplicatedMap = false,
    bool useFlatMap = false) {
  const auto it = selectedChildren.find(&type);
  const bool hasSelectedChildren = (it != selectedChildren.end());

  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return builder.createScalarTypeBuilder(ScalarKind::Bool);
    case velox::TypeKind::TINYINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int8);
    case velox::TypeKind::SMALLINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int16);
    case velox::TypeKind::INTEGER:
      return builder.createScalarTypeBuilder(ScalarKind::Int32);
    case velox::TypeKind::BIGINT:
      return builder.createScalarTypeBuilder(ScalarKind::Int64);
    case velox::TypeKind::REAL:
      return builder.createScalarTypeBuilder(ScalarKind::Float);
    case velox::TypeKind::DOUBLE:
      return builder.createScalarTypeBuilder(ScalarKind::Double);
    case velox::TypeKind::VARCHAR:
      return builder.createScalarTypeBuilder(ScalarKind::String);
    case velox::TypeKind::VARBINARY:
      return builder.createScalarTypeBuilder(ScalarKind::Binary);
    case velox::TypeKind::TIMESTAMP:
      return builder.createTimestampMicroNanoTypeBuilder();

    case velox::TypeKind::ARRAY: {
      const auto& arrayType = type.asArray();
      if (useDictionaryArray) {
        auto nimbleType = builder.createArrayWithOffsetsTypeBuilder();
        nimbleType->setChildren(buildProjectedNimbleType(
            builder, *arrayType.elementType(), selectedChildren));
        return nimbleType;
      }
      auto nimbleType = builder.createArrayTypeBuilder();
      nimbleType->setChildren(buildProjectedNimbleType(
          builder,
          *arrayType.elementType(),
          selectedChildren,
          /*useDictionaryArray=*/false,
          /*useDeduplicatedMap=*/false,
          /*useFlatMap=*/false));
      return nimbleType;
    }

    case velox::TypeKind::MAP: {
      const auto& mapType = type.asMap();
      if (hasSelectedChildren) {
        NIMBLE_CHECK(
            useFlatMap,
            "FlatMap key-level projection requires the column to be in "
            "flatMapColumns and must be a top-level MAP column: {}",
            type.toString());
        auto keyScalarKind = toScalarKind(mapType.keyType()->kind());
        auto flatMap = builder.createFlatMapTypeBuilder(keyScalarKind);
        // std::set is already sorted alphabetically.
        for (const auto& key : it->second) {
          flatMap->addChild(
              key,
              buildProjectedNimbleType(
                  builder,
                  *mapType.valueType(),
                  selectedChildren,
                  /*useDictionaryArray=*/false,
                  /*useDeduplicatedMap=*/false,
                  /*useFlatMap=*/false));
        }
        return flatMap;
      }

      NIMBLE_CHECK(
          !useFlatMap,
          "Cannot project entire FlatMap column without key subscripts. "
          "FlatMap key names and order are not available from the velox type. "
          "Use key-level projection (e.g., map[\"key\"]) or the "
          "nimble-schema-based buildProjectedNimbleType overload instead.");

      if (useDeduplicatedMap) {
        auto nimbleType = builder.createSlidingWindowMapTypeBuilder();
        auto key = buildProjectedNimbleType(
            builder,
            *mapType.keyType(),
            selectedChildren,
            /*useDictionaryArray=*/false,
            /*useDeduplicatedMap=*/false,
            /*useFlatMap=*/false);
        auto value = buildProjectedNimbleType(
            builder,
            *mapType.valueType(),
            selectedChildren,
            /*useDictionaryArray=*/false,
            /*useDeduplicatedMap=*/false,
            /*useFlatMap=*/false);
        nimbleType->setChildren(key, value);
        return nimbleType;
      }

      auto nimbleType = builder.createMapTypeBuilder();
      auto key = buildProjectedNimbleType(
          builder,
          *mapType.keyType(),
          selectedChildren,
          /*useDictionaryArray=*/false,
          /*useDeduplicatedMap=*/false,
          /*useFlatMap=*/false);
      auto value = buildProjectedNimbleType(
          builder,
          *mapType.valueType(),
          selectedChildren,
          /*useDictionaryArray=*/false,
          /*useDeduplicatedMap=*/false,
          /*useFlatMap=*/false);
      nimbleType->setChildren(key, value);
      return nimbleType;
    }

    case velox::TypeKind::ROW: {
      const auto& rowType = type.asRow();
      if (hasSelectedChildren) {
        const auto& selectedChildNames = it->second;
        auto nimbleRow =
            builder.createRowTypeBuilder(selectedChildNames.size());
        // Iterate in velox type order for consistent child ordering.
        for (size_t i = 0; i < rowType.size(); ++i) {
          const auto& childName = rowType.nameOf(i);
          if (!selectedChildNames.contains(childName)) {
            continue;
          }
          nimbleRow->addChild(
              childName,
              buildProjectedNimbleType(
                  builder,
                  *rowType.childAt(i),
                  selectedChildren,
                  /*useDictionaryArray=*/false,
                  /*useDeduplicatedMap=*/false,
                  /*useFlatMap=*/false));
        }
        return nimbleRow;
      }

      auto nimbleType = builder.createRowTypeBuilder(rowType.size());
      for (size_t i = 0; i < rowType.size(); ++i) {
        nimbleType->addChild(
            rowType.nameOf(i),
            buildProjectedNimbleType(
                builder,
                *rowType.childAt(i),
                selectedChildren,
                /*useDictionaryArray=*/false,
                /*useDeduplicatedMap=*/false,
                /*useFlatMap=*/false));
      }
      return nimbleType;
    }

    default:
      NIMBLE_UNSUPPORTED("Unsupported type kind {}.", type.kind());
  }
}

} // namespace

std::shared_ptr<const Type> buildProjectedNimbleType(
    const velox::RowType& type,
    const std::vector<velox::common::Subfield>& projectedSubfields,
    const ColumnEncodings& columnEncodings) {
  NIMBLE_CHECK(
      !projectedSubfields.empty(), "projectedSubfields must not be empty");

  VeloxSelectedChildrenMap selectedChildren;
  for (const auto& subfield : projectedSubfields) {
    resolveVeloxSubfield(&type, subfield, selectedChildren);
  }

  const auto& selectedTopLevelColumns = selectedChildren[&type];
  NIMBLE_CHECK(
      !selectedTopLevelColumns.empty(),
      "No top-level columns resolved from projectedSubfields");

  SchemaBuilder builder;
  auto root = builder.createRowTypeBuilder(selectedTopLevelColumns.size());

  // Iterate in velox type order for consistent child ordering.
  for (size_t i = 0; i < type.size(); ++i) {
    const auto& name = type.nameOf(i);
    if (!selectedTopLevelColumns.contains(name)) {
      continue;
    }

    const bool useDictionaryArray =
        columnEncodings.dictionaryArrayColumns.contains(name);
    const bool useDeduplicatedMap =
        columnEncodings.deduplicatedMapColumns.contains(name);
    const bool useFlatMap = columnEncodings.flatMapColumns.contains(name);

    root->addChild(
        name,
        buildProjectedNimbleType(
            builder,
            *type.childAt(i),
            selectedChildren,
            useDictionaryArray,
            useDeduplicatedMap,
            useFlatMap));
  }

  return SchemaReader::getSchema(builder.schemaNodes());
}

//
// buildProjectedNimbleType and its helpers.
//

namespace {

// Tracks which children to include at each Row/FlatMap node during projection.
// When a node appears in this map, only its selected children are included.
// When absent, all children are included.
using NimbleSelectedChildrenMap =
    folly::F14FastMap<const Type*, std::set<size_t>>;

// Resolves a single subfield path against a nimble schema tree, populating
// selectedChildren with which children to include at each Row/FlatMap node.
void resolveSubfield(
    const Type* type,
    const velox::common::Subfield& subfield,
    NimbleSelectedChildrenMap& selectedChildren) {
  const auto& path = subfield.path();
  NIMBLE_CHECK(!path.empty(), "Empty subfield path");

  const auto* current = type;

  for (const auto& element : path) {
    const auto kind = element->kind();
    if (kind == SubfieldKind::kNestedField) {
      const auto* nested = element->asChecked<Subfield::NestedField>();
      const auto& name = nested->name();

      NIMBLE_CHECK(
          current->isRow(),
          "Cannot access nested field '{}' on non-Row type in subfield path '{}'",
          name,
          subfield.toString());
      const auto& row = current->asRow();
      const auto childIdx = row.findChild(name);
      NIMBLE_CHECK(
          childIdx.has_value(), "Field '{}' not found in RowType", name);

      selectedChildren[current].insert(*childIdx);
      current = row.childAt(*childIdx).get();
    } else if (
        kind == SubfieldKind::kStringSubscript ||
        kind == SubfieldKind::kLongSubscript) {
      // Convert subscript to string key name. FlatMap keys are always strings
      // internally, so LongSubscript is converted via std::to_string.
      const auto keyName = (kind == SubfieldKind::kStringSubscript)
          ? element->asChecked<Subfield::StringSubscript>()->index()
          : std::to_string(
                element->asChecked<Subfield::LongSubscript>()->index());

      NIMBLE_CHECK(
          current->isFlatMap(),
          "Subscript '{}' only supported on FlatMap in subfield path '{}'",
          keyName,
          subfield.toString());
      const auto& flatMap = current->asFlatMap();
      const auto childIdx = flatMap.findChild(keyName);
      if (!childIdx.has_value()) {
        // Key not in file schema — skip this subfield silently.
        return;
      }

      selectedChildren[current].insert(*childIdx);
      current = flatMap.childAt(*childIdx).get();
    } else {
      NIMBLE_FAIL(
          "Unsupported subfield kind {} in subfield path '{}'",
          kind,
          subfield.toString());
    }
  }
}

// Allocates an output stream offset on-demand during schema traversal and
// records the corresponding input stream offset.
StreamDescriptor allocateStreamOffset(
    const StreamDescriptor& inputDesc,
    uint32_t& nextOffset,
    std::vector<uint32_t>& inputStreamOffsets) {
  inputStreamOffsets.push_back(inputDesc.offset());
  return StreamDescriptor{nextOffset++, inputDesc.scalarKind()};
}

// Sorts FlatMap children indices alphabetically by name for canonical ordering
// that matches buildProjectedNimbleType's stream offset assignment.
std::vector<size_t> sortedFlatMapChildIndices(
    const FlatMapType& flatMap,
    std::vector<size_t> indices) {
  std::sort(indices.begin(), indices.end(), [&](auto a, auto b) {
    return flatMap.nameAt(a) < flatMap.nameAt(b);
  });
  return indices;
}

// Returns true if the kind is an encoding-specific type that should only
// appear as a direct child of the top-level Row.
bool specificEncodingKind(Kind kind) {
  return kind == Kind::FlatMap || kind == Kind::ArrayWithOffsets ||
      kind == Kind::SlidingWindowMap;
}

// Builds a projected nimble type from the input type, including only selected
// children. Assigns output stream offsets sequentially in DFS traversal order,
// matching how SchemaBuilder assigns offsets in buildProjectedNimbleType.
// FlatMap children are sorted alphabetically by name for canonical ordering.
//
// Encoding-specific types (FlatMap, ArrayWithOffsets, SlidingWindowMap) must
// only appear as direct children of the top-level Row. The public function
// buildProjectedNimbleType handles the root Row and passes isTopLevelChild=true
// for its direct children.
std::shared_ptr<const Type> buildProjectedType(
    const Type* inputType,
    const NimbleSelectedChildrenMap& selectedChildren,
    uint32_t& nextOffset,
    std::vector<uint32_t>& inputStreamOffsets,
    bool isTopLevelChild = false) {
  const auto kind = inputType->kind();
  NIMBLE_CHECK(
      isTopLevelChild || !specificEncodingKind(kind),
      "Encoding-specific type {} must be a direct child of the top-level Row",
      kind);
  switch (kind) {
    case Kind::Scalar: {
      return std::make_shared<ScalarType>(allocateStreamOffset(
          inputType->asScalar().scalarDescriptor(),
          nextOffset,
          inputStreamOffsets));
    }

    case Kind::TimestampMicroNano: {
      const auto& ts = inputType->asTimestampMicroNano();
      auto microsDesc = allocateStreamOffset(
          ts.microsDescriptor(), nextOffset, inputStreamOffsets);
      auto nanosDesc = allocateStreamOffset(
          ts.nanosDescriptor(), nextOffset, inputStreamOffsets);
      return std::make_shared<TimestampMicroNanoType>(
          std::move(microsDesc), std::move(nanosDesc));
    }

    case Kind::Row: {
      const auto& row = inputType->asRow();
      auto nullsDesc = allocateStreamOffset(
          row.nullsDescriptor(), nextOffset, inputStreamOffsets);

      std::vector<std::string> names;
      std::vector<std::shared_ptr<const Type>> children;

      const auto it = selectedChildren.find(inputType);
      if (it != selectedChildren.end()) {
        names.reserve(it->second.size());
        children.reserve(it->second.size());
        for (size_t columnIdx : it->second) {
          names.emplace_back(row.nameAt(columnIdx));
          children.emplace_back(buildProjectedType(
              row.childAt(columnIdx).get(),
              selectedChildren,
              nextOffset,
              inputStreamOffsets));
        }
      } else {
        names.reserve(row.childrenCount());
        children.reserve(row.childrenCount());
        for (size_t i = 0; i < row.childrenCount(); ++i) {
          names.emplace_back(row.nameAt(i));
          children.emplace_back(buildProjectedType(
              row.childAt(i).get(),
              selectedChildren,
              nextOffset,
              inputStreamOffsets));
        }
      }

      return std::make_shared<RowType>(
          std::move(nullsDesc), std::move(names), std::move(children));
    }

    case Kind::Array: {
      const auto& array = inputType->asArray();
      auto lengthsDesc = allocateStreamOffset(
          array.lengthsDescriptor(), nextOffset, inputStreamOffsets);
      auto elements = buildProjectedType(
          array.elements().get(),
          selectedChildren,
          nextOffset,
          inputStreamOffsets);
      return std::make_shared<ArrayType>(
          std::move(lengthsDesc), std::move(elements));
    }

    case Kind::ArrayWithOffsets: {
      const auto& array = inputType->asArrayWithOffsets();
      auto offsetsDesc = allocateStreamOffset(
          array.offsetsDescriptor(), nextOffset, inputStreamOffsets);
      auto lengthsDesc = allocateStreamOffset(
          array.lengthsDescriptor(), nextOffset, inputStreamOffsets);
      auto elements = buildProjectedType(
          array.elements().get(),
          selectedChildren,
          nextOffset,
          inputStreamOffsets);
      return std::make_shared<ArrayWithOffsetsType>(
          std::move(offsetsDesc), std::move(lengthsDesc), std::move(elements));
    }

    case Kind::Map: {
      const auto& map = inputType->asMap();
      auto lengthsDesc = allocateStreamOffset(
          map.lengthsDescriptor(), nextOffset, inputStreamOffsets);
      auto keys = buildProjectedType(
          map.keys().get(), selectedChildren, nextOffset, inputStreamOffsets);
      auto values = buildProjectedType(
          map.values().get(), selectedChildren, nextOffset, inputStreamOffsets);
      return std::make_shared<MapType>(
          std::move(lengthsDesc), std::move(keys), std::move(values));
    }

    case Kind::SlidingWindowMap: {
      const auto& map = inputType->asSlidingWindowMap();
      auto offsetsDesc = allocateStreamOffset(
          map.offsetsDescriptor(), nextOffset, inputStreamOffsets);
      auto lengthsDesc = allocateStreamOffset(
          map.lengthsDescriptor(), nextOffset, inputStreamOffsets);
      auto keys = buildProjectedType(
          map.keys().get(), selectedChildren, nextOffset, inputStreamOffsets);
      auto values = buildProjectedType(
          map.values().get(), selectedChildren, nextOffset, inputStreamOffsets);
      return std::make_shared<SlidingWindowMapType>(
          std::move(offsetsDesc),
          std::move(lengthsDesc),
          std::move(keys),
          std::move(values));
    }

    case Kind::FlatMap: {
      const auto& flatMap = inputType->asFlatMap();

      // FlatMap must have selected children (key subscripts). Projecting
      // an entire FlatMap without specifying keys is not supported because
      // the Projector requires explicit key selection for FlatMap columns.
      const auto it = selectedChildren.find(inputType);
      NIMBLE_CHECK(
          it != selectedChildren.end(),
          "Cannot project entire FlatMap column without key subscripts. "
          "Use key-level projection (e.g., map[\"key\"]).");

      auto nullsDesc = allocateStreamOffset(
          flatMap.nullsDescriptor(), nextOffset, inputStreamOffsets);

      std::vector<std::string> names;
      std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors;
      std::vector<std::shared_ptr<const Type>> children;

      // Build sorted child indices — always sort by name for canonical
      // ordering that matches buildProjectedNimbleType.
      std::vector<size_t> indices;
      indices.assign(it->second.begin(), it->second.end());
      indices = sortedFlatMapChildIndices(flatMap, std::move(indices));

      names.reserve(indices.size());
      inMapDescriptors.reserve(indices.size());
      children.reserve(indices.size());
      for (size_t columnIdx : indices) {
        names.emplace_back(flatMap.nameAt(columnIdx));
        children.emplace_back(buildProjectedType(
            flatMap.childAt(columnIdx).get(),
            selectedChildren,
            nextOffset,
            inputStreamOffsets));
        inMapDescriptors.emplace_back(
            std::make_unique<StreamDescriptor>(allocateStreamOffset(
                flatMap.inMapDescriptorAt(columnIdx),
                nextOffset,
                inputStreamOffsets)));
      }

      return std::make_shared<FlatMapType>(
          std::move(nullsDesc),
          flatMap.keyScalarKind(),
          std::move(names),
          std::move(inMapDescriptors),
          std::move(children));
    }

    default:
      NIMBLE_FAIL("Unsupported type kind for projected schema: {}", kind);
  }
}

} // namespace

std::shared_ptr<const Type> buildProjectedNimbleType(
    const Type* type,
    const std::vector<velox::common::Subfield>& projectedSubfields,
    std::vector<uint32_t>& projectedStreamOffsets) {
  NIMBLE_CHECK(
      !projectedSubfields.empty(), "projectedSubfields must not be empty");
  NIMBLE_CHECK(
      projectedStreamOffsets.empty(),
      "projectedStreamOffsets must be empty, got size: {}",
      projectedStreamOffsets.size());
  NIMBLE_CHECK(type->isRow(), "Root type must be a Row, got: {}", type->kind());

  NimbleSelectedChildrenMap selectedChildren;
  for (const auto& subfield : projectedSubfields) {
    resolveSubfield(type, subfield, selectedChildren);
  }

  const auto& rootRow = type->asRow();
  uint32_t nextOffset = 0;
  auto nullsDesc = allocateStreamOffset(
      rootRow.nullsDescriptor(), nextOffset, projectedStreamOffsets);

  const auto& selectedTopLevelColumns = selectedChildren[type];
  NIMBLE_CHECK(
      !selectedTopLevelColumns.empty(),
      "No top-level columns resolved from projectedSubfields");

  std::vector<std::string> names;
  std::vector<std::shared_ptr<const Type>> children;
  names.reserve(selectedTopLevelColumns.size());
  children.reserve(selectedTopLevelColumns.size());
  for (size_t columnIdx : selectedTopLevelColumns) {
    names.emplace_back(rootRow.nameAt(columnIdx));
    children.emplace_back(buildProjectedType(
        rootRow.childAt(columnIdx).get(),
        selectedChildren,
        nextOffset,
        projectedStreamOffsets,
        /*isTopLevelChild=*/true));
  }

  return std::make_shared<RowType>(
      std::move(nullsDesc), std::move(names), std::move(children));
}

} // namespace facebook::nimble
