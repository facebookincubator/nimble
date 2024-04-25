/*
 * Copyright (c) Meta Platforms, Inc. and its affiliates.
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
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "folly/container/F14Map.h"

namespace facebook::nimble {

namespace {

inline std::string getKindName(Kind kind) {
  static folly::F14FastMap<Kind, std::string> names{
      {Kind::Scalar, "Scalar"},
      {Kind::Row, "Row"},
      {Kind::Array, "Array"},
      {Kind::Map, "Map"},
      {Kind::FlatMap, "FlatMap"},
  };

  auto it = names.find(kind);
  if (UNLIKELY(it == names.end())) {
    return folly::to<std::string>("<Unknown Kind ", kind, ">");
  }

  return it->second;
}

struct NamedType {
  std::shared_ptr<const Type> type;
  std::optional<std::string> name;
};

} // namespace

Type::Type(Kind kind) : kind_{kind} {}

Kind Type::kind() const {
  return kind_;
}

bool Type::isScalar() const {
  return kind_ == Kind::Scalar;
}

bool Type::isRow() const {
  return kind_ == Kind::Row;
}

bool Type::isArray() const {
  return kind_ == Kind::Array;
}

bool Type::isArrayWithOffsets() const {
  return kind_ == Kind::ArrayWithOffsets;
}

bool Type::isMap() const {
  return kind_ == Kind::Map;
}

bool Type::isFlatMap() const {
  return kind_ == Kind::FlatMap;
}

const ScalarType& Type::asScalar() const {
  NIMBLE_ASSERT(
      isScalar(),
      fmt::format(
          "Cannot cast to Scalar. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const ScalarType&>(*this);
}

const RowType& Type::asRow() const {
  NIMBLE_ASSERT(
      isRow(),
      fmt::format(
          "Cannot cast to Row. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const RowType&>(*this);
}

const ArrayType& Type::asArray() const {
  NIMBLE_ASSERT(
      isArray(),
      fmt::format(
          "Cannot cast to Array. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const ArrayType&>(*this);
}

const ArrayWithOffsetsType& Type::asArrayWithOffsets() const {
  NIMBLE_ASSERT(
      isArrayWithOffsets(),
      fmt::format(
          "Cannot cast to ArrayWithOffsets. Current type is {}.",
          getKindName(kind_)));
  return dynamic_cast<const ArrayWithOffsetsType&>(*this);
}

const MapType& Type::asMap() const {
  NIMBLE_ASSERT(
      isMap(),
      fmt::format(
          "Cannot cast to Map. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const MapType&>(*this);
}

const FlatMapType& Type::asFlatMap() const {
  NIMBLE_ASSERT(
      isFlatMap(),
      fmt::format(
          "Cannot cast to FlatMap. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const FlatMapType&>(*this);
}

ScalarType::ScalarType(StreamDescriptor scalarDescriptor)
    : Type(Kind::Scalar), scalarDescriptor_{std::move(scalarDescriptor)} {}

const StreamDescriptor& ScalarType::scalarDescriptor() const {
  return scalarDescriptor_;
}

ArrayType::ArrayType(
    StreamDescriptor lengthsDescriptor,
    std::shared_ptr<const Type> elements)
    : Type(Kind::Array),
      lengthsDescriptor_{std::move(lengthsDescriptor)},
      elements_{std::move(elements)} {}

const StreamDescriptor& ArrayType::lengthsDescriptor() const {
  return lengthsDescriptor_;
}

const std::shared_ptr<const Type>& ArrayType::elements() const {
  return elements_;
}

MapType::MapType(
    StreamDescriptor lengthsDescriptor,
    std::shared_ptr<const Type> keys,
    std::shared_ptr<const Type> values)
    : Type(Kind::Map),
      lengthsDescriptor_{std::move(lengthsDescriptor)},
      keys_{std::move(keys)},
      values_{std::move(values)} {}

const StreamDescriptor& MapType::lengthsDescriptor() const {
  return lengthsDescriptor_;
}

const std::shared_ptr<const Type>& MapType::keys() const {
  return keys_;
}

const std::shared_ptr<const Type>& MapType::values() const {
  return values_;
}

RowType::RowType(
    StreamDescriptor nullsDescriptor,
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const Type>> children)
    : Type(Kind::Row),
      nullsDescriptor_{std::move(nullsDescriptor)},
      names_{std::move(names)},
      children_{std::move(children)} {
  NIMBLE_ASSERT(
      names_.size() == children_.size(),
      fmt::format(
          "Size mismatch. names: {} vs children: {}.",
          names_.size(),
          children_.size()));
}

const StreamDescriptor& RowType::nullsDescriptor() const {
  return nullsDescriptor_;
}

size_t RowType::childrenCount() const {
  return children_.size();
}

const std::shared_ptr<const Type>& RowType::childAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return children_[index];
}

const std::string& RowType::nameAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return names_[index];
}

FlatMapType::FlatMapType(
    StreamDescriptor nullsDescriptor,
    ScalarKind keyScalarKind,
    std::vector<std::string> names,
    std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors,
    std::vector<std::shared_ptr<const Type>> children)
    : Type(Kind::FlatMap),
      nullsDescriptor_{nullsDescriptor},
      keyScalarKind_{keyScalarKind},
      names_{std::move(names)},
      inMapDescriptors_{std::move(inMapDescriptors)},
      children_{std::move(children)} {
  NIMBLE_ASSERT(
      names_.size() == children_.size() &&
          inMapDescriptors_.size() == children_.size(),
      fmt::format(
          "Size mismatch. names: {} vs inMaps: {} vs children: {}.",
          names_.size(),
          inMapDescriptors_.size(),
          children_.size()));
}

const StreamDescriptor& FlatMapType::nullsDescriptor() const {
  return nullsDescriptor_;
}

const StreamDescriptor& FlatMapType::inMapDescriptorAt(size_t index) const {
  NIMBLE_ASSERT(
      index < inMapDescriptors_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.",
          index,
          inMapDescriptors_.size()));
  return *inMapDescriptors_[index];
}

ScalarKind FlatMapType::keyScalarKind() const {
  return keyScalarKind_;
}

size_t FlatMapType::childrenCount() const {
  return children_.size();
}

const std::shared_ptr<const Type>& FlatMapType::childAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return children_[index];
}

const std::string& FlatMapType::nameAt(size_t index) const {
  NIMBLE_ASSERT(
      index < names_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, names_.size()));
  return names_[index];
}

ArrayWithOffsetsType::ArrayWithOffsetsType(
    StreamDescriptor offsetsDescriptor,
    StreamDescriptor lengthsDescriptor,
    std::shared_ptr<const Type> elements)
    : Type(Kind::ArrayWithOffsets),
      offsetsDescriptor_{std::move(offsetsDescriptor)},
      lengthsDescriptor_{std::move(lengthsDescriptor)},
      elements_{std::move(elements)} {}

const StreamDescriptor& ArrayWithOffsetsType::offsetsDescriptor() const {
  return offsetsDescriptor_;
}

const StreamDescriptor& ArrayWithOffsetsType::lengthsDescriptor() const {
  return lengthsDescriptor_;
}

const std::shared_ptr<const Type>& ArrayWithOffsetsType::elements() const {
  return elements_;
}

NamedType getType(
    offset_size& index,
    const std::vector<std::unique_ptr<const SchemaNode>>& nodes) {
  NIMBLE_DASSERT(index < nodes.size(), "Index out of range.");
  auto& node = nodes[index++];
  auto offset = node->offset();
  auto kind = node->kind();
  switch (kind) {
    case Kind::Scalar: {
      return {
          .type = std::make_shared<ScalarType>(
              StreamDescriptor{offset, node->scalarKind()}),
          .name = node->name()};
    }
    case Kind::Array: {
      auto elements = getType(index, nodes).type;
      return {
          .type = std::make_shared<ArrayType>(
              StreamDescriptor{offset, ScalarKind::UInt32},
              std::move(elements)),
          .name = node->name()};
    }
    case Kind::Map: {
      auto keys = getType(index, nodes).type;
      auto values = getType(index, nodes).type;
      return {
          .type = std::make_shared<MapType>(
              StreamDescriptor{offset, ScalarKind::UInt32},
              std::move(keys),
              std::move(values)),
          .name = node->name()};
    }
    case Kind::Row: {
      auto childrenCount = node->childrenCount();
      std::vector<std::string> names{childrenCount};
      std::vector<std::shared_ptr<const Type>> children{childrenCount};
      for (auto i = 0; i < childrenCount; ++i) {
        auto namedType = getType(index, nodes);
        NIMBLE_ASSERT(
            namedType.name.has_value(), "Row fields must have names.");
        children[i] = namedType.type;
        names[i] = namedType.name.value();
      }
      return {
          .type = std::make_shared<RowType>(
              StreamDescriptor{offset, ScalarKind::Bool},
              std::move(names),
              std::move(children)),
          .name = node->name()};
    }
    case Kind::FlatMap: {
      auto childrenCount = node->childrenCount();

      std::vector<std::string> names{childrenCount};
      std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors{
          childrenCount};
      std::vector<std::shared_ptr<const Type>> children{childrenCount};

      for (auto i = 0; i < childrenCount; ++i) {
        NIMBLE_DASSERT(index < nodes.size(), "Unexpected node index.");
        auto& inMapNode = nodes[index++];
        NIMBLE_ASSERT(
            inMapNode->kind() == Kind::Scalar &&
                inMapNode->scalarKind() == ScalarKind::Bool,
            "Flat map in-map field must have a boolean scalar type.");
        NIMBLE_ASSERT(
            inMapNode->name().has_value(), "Flat map fields must have names.");
        auto field = getType(index, nodes);
        names[i] = inMapNode->name().value();
        inMapDescriptors[i] = std::make_unique<StreamDescriptor>(
            inMapNode->offset(), inMapNode->scalarKind());
        children[i] = field.type;
      }
      return {
          .type = std::make_shared<FlatMapType>(
              StreamDescriptor{offset, ScalarKind::Bool},
              node->scalarKind(),
              std::move(names),
              std::move(inMapDescriptors),
              std::move(children)),
          .name = node->name()};
    }
    case Kind::ArrayWithOffsets: {
      auto& offsetsNode = nodes[index++];
      NIMBLE_ASSERT(
          offsetsNode->kind() == Kind::Scalar &&
              offsetsNode->scalarKind() == ScalarKind::UInt32,
          "Array with offsets field must have a uint32 scalar type.");
      auto elements = getType(index, nodes).type;
      return {
          .type = std::make_shared<ArrayWithOffsetsType>(
              StreamDescriptor{
                  offsetsNode->offset(), offsetsNode->scalarKind()},
              StreamDescriptor{offset, ScalarKind::UInt32},
              std::move(elements)),
          .name = node->name()};
    }

    default: {
      NIMBLE_UNREACHABLE(fmt::format("Unknown node kind: ", toString(kind)));
    }
  }
}

std::shared_ptr<const Type> SchemaReader::getSchema(
    const std::vector<std::unique_ptr<const SchemaNode>>& nodes) {
  offset_size index = 0;
  auto namedType = getType(index, nodes);
  return namedType.type;
}

void traverseSchema(
    size_t& index,
    uint32_t level,
    const std::shared_ptr<const Type>& type,
    const std::function<
        void(uint32_t, const Type&, const SchemaReader::NodeInfo&)>& visitor,
    const SchemaReader::NodeInfo& info) {
  visitor(level, *type, info);
  ++index;
  switch (type->kind()) {
    case Kind::Scalar:
      break;
    case Kind::Row: {
      auto& row = type->asRow();
      auto childrenCount = row.childrenCount();
      for (size_t i = 0; i < childrenCount; ++i) {
        traverseSchema(
            index,
            level + 1,
            row.childAt(i),
            visitor,
            {.name = row.nameAt(i),
             .parentType = type.get(),
             .placeInSibling = i});
      }
      break;
    }
    case Kind::Array: {
      auto& array = type->asArray();
      traverseSchema(
          index,
          level + 1,
          array.elements(),
          visitor,
          {.name = "elements", .parentType = type.get()});
      break;
    }
    case Kind::ArrayWithOffsets: {
      auto& arrayWithOffsets = type->asArrayWithOffsets();
      traverseSchema(
          index,
          level + 1,
          arrayWithOffsets.elements(),
          visitor,
          {.name = "elements", .parentType = type.get()});

      break;
    }
    case Kind::Map: {
      auto& map = type->asMap();
      traverseSchema(
          index,
          level + 1,
          map.keys(),
          visitor,
          {.name = "keys", .parentType = type.get(), .placeInSibling = 0});
      traverseSchema(
          index,
          level + 1,
          map.values(),
          visitor,
          {.name = "values", .parentType = type.get(), .placeInSibling = 1});
      break;
    }
    case Kind::FlatMap: {
      auto& map = type->asFlatMap();
      for (size_t i = 0; i < map.childrenCount(); ++i) {
        traverseSchema(
            index,
            level + 1,
            map.childAt(i),
            visitor,
            {.name = map.nameAt(i),
             .parentType = type.get(),
             .placeInSibling = i});
      }
      break;
    }
  }
}

void SchemaReader::traverseSchema(
    const std::shared_ptr<const Type>& root,
    std::function<void(uint32_t, const Type&, const SchemaReader::NodeInfo&)>
        visitor) {
  size_t index = 0;
  nimble::traverseSchema(
      index, 0, root, visitor, {.name = "root", .parentType = nullptr});
}

std::ostream& operator<<(
    std::ostream& out,
    const std::shared_ptr<const Type>& root) {
  SchemaReader::traverseSchema(
      root,
      [&out](uint32_t level, const Type& type, const SchemaReader::NodeInfo&) {
        out << std::string((std::basic_string<char>::size_type)level * 2, ' ');
        if (type.isScalar()) {
          auto& scalar = type.asScalar();
          out << "[" << scalar.scalarDescriptor().offset() << "]"
              << toString(scalar.scalarDescriptor().scalarKind()) << "\n";
        } else if (type.isArray()) {
          out << "[" << type.asArray().lengthsDescriptor().offset() << "]"
              << "ARRAY\n";
        } else if (type.isArrayWithOffsets()) {
          const auto& array = type.asArrayWithOffsets();
          out << "[o:" << array.offsetsDescriptor().offset()
              << ",l:" << array.lengthsDescriptor().offset() << "]"
              << "OFFSETARRAY\n";
        } else if (type.isRow()) {
          auto& row = type.asRow();
          out << "[" << row.nullsDescriptor().offset() << "]" << "ROW[";
          for (auto i = 0; i < row.childrenCount(); ++i) {
            out << row.nameAt(i) << (i < row.childrenCount() - 1 ? "," : "");
          }
          out << "]\n";
        } else if (type.isMap()) {
          out << "[" << type.asMap().lengthsDescriptor().offset() << "]"
              << "MAP\n";
        } else if (type.isFlatMap()) {
          auto& map = type.asFlatMap();
          out << "[" << map.nullsDescriptor().offset() << "]" << "FLATMAP[";
          for (auto i = 0; i < map.childrenCount(); ++i) {
            out << map.nameAt(i) << (i < map.childrenCount() - 1 ? "," : "");
          }
          out << "]\n";
        }
      });
  return out;
}

} // namespace facebook::nimble
