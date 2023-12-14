// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "folly/container/F14Map.h"

namespace facebook::alpha {

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

Type::Type(offset_size offset, Kind kind) : offset_{offset}, kind_{kind} {}

Kind Type::kind() const {
  return kind_;
}

offset_size Type::offset() const {
  return offset_;
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
  ALPHA_ASSERT(
      isScalar(),
      fmt::format(
          "Cannot cast to Scalar. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const ScalarType&>(*this);
}

const RowType& Type::asRow() const {
  ALPHA_ASSERT(
      isRow(),
      fmt::format(
          "Cannot cast to Row. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const RowType&>(*this);
}

const ArrayType& Type::asArray() const {
  ALPHA_ASSERT(
      isArray(),
      fmt::format(
          "Cannot cast to Array. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const ArrayType&>(*this);
}

const ArrayWithOffsetsType& Type::asArrayWithOffsets() const {
  ALPHA_ASSERT(
      isArrayWithOffsets(),
      fmt::format(
          "Cannot cast to ArrayWithOffsets. Current type is {}.",
          getKindName(kind_)));
  return dynamic_cast<const ArrayWithOffsetsType&>(*this);
}

const MapType& Type::asMap() const {
  ALPHA_ASSERT(
      isMap(),
      fmt::format(
          "Cannot cast to Map. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const MapType&>(*this);
}

const FlatMapType& Type::asFlatMap() const {
  ALPHA_ASSERT(
      isFlatMap(),
      fmt::format(
          "Cannot cast to FlatMap. Current type is {}.", getKindName(kind_)));
  return dynamic_cast<const FlatMapType&>(*this);
}

ScalarType::ScalarType(offset_size offset, ScalarKind scalarKind)
    : Type(offset, Kind::Scalar), scalarKind_{scalarKind} {}

ScalarKind ScalarType::scalarKind() const {
  return scalarKind_;
}

ArrayType::ArrayType(offset_size offset, std::shared_ptr<const Type> elements)
    : Type(offset, Kind::Array), elements_{std::move(elements)} {}

const std::shared_ptr<const Type>& ArrayType::elements() const {
  return elements_;
}

ArrayWithOffsetsType::ArrayWithOffsetsType(
    offset_size offset,
    std::shared_ptr<const ScalarType> offsets,
    std::shared_ptr<const Type> elements)
    : Type(offset, Kind::ArrayWithOffsets),
      offsets_{std::move(offsets)},
      elements_{std::move(elements)} {}

const std::shared_ptr<const ScalarType>& ArrayWithOffsetsType::offsets() const {
  return offsets_;
}
const std::shared_ptr<const Type>& ArrayWithOffsetsType::elements() const {
  return elements_;
}

RowType::RowType(
    offset_size offset,
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const Type>> children)
    : Type(offset, Kind::Row),
      names_{std::move(names)},
      children_{std::move(children)} {
  ALPHA_ASSERT(
      names_.size() == children_.size(),
      fmt::format(
          "Size mismatch. names: {} vs children: {}.",
          names_.size(),
          children_.size()));
}

size_t RowType::childrenCount() const {
  return children_.size();
}

const std::shared_ptr<const Type>& RowType::childAt(size_t index) const {
  ALPHA_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return children_[index];
}

const std::string& RowType::nameAt(size_t index) const {
  ALPHA_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return names_[index];
}

MapType::MapType(
    offset_size offset,
    std::shared_ptr<const Type> keys,
    std::shared_ptr<const Type> values)
    : Type(offset, Kind::Map),
      keys_{std::move(keys)},
      values_{std::move(values)} {}

const std::shared_ptr<const Type>& MapType::keys() const {
  return keys_;
}

const std::shared_ptr<const Type>& MapType::values() const {
  return values_;
}

FlatMapType::FlatMapType(
    offset_size offset,
    ScalarKind keyScalarKind,
    std::vector<std::string> names,
    std::vector<std::shared_ptr<const ScalarType>> inMaps,
    std::vector<std::shared_ptr<const Type>> children)
    : Type(offset, Kind::FlatMap),
      keyScalarKind_{keyScalarKind},
      names_{std::move(names)},
      inMaps_{std::move(inMaps)},
      children_{std::move(children)} {
  ALPHA_ASSERT(
      names_.size() == children_.size() && inMaps_.size() == children_.size(),
      fmt::format(
          "Size mismatch. names: {} vs inMaps: {} vs children: {}.",
          names_.size(),
          inMaps_.size(),
          children_.size()));
}

ScalarKind FlatMapType::keyScalarKind() const {
  return keyScalarKind_;
}

size_t FlatMapType::childrenCount() const {
  return children_.size();
}

const std::shared_ptr<const ScalarType>& FlatMapType::inMapAt(
    size_t index) const {
  ALPHA_ASSERT(
      index < inMaps_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, inMaps_.size()));
  return inMaps_[index];
}

const std::shared_ptr<const Type>& FlatMapType::childAt(size_t index) const {
  ALPHA_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return children_[index];
}

const std::string& FlatMapType::nameAt(size_t index) const {
  ALPHA_ASSERT(
      index < names_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, names_.size()));
  return names_[index];
}

NamedType getType(
    offset_size& index,
    const std::vector<std::unique_ptr<const SchemaNode>>& nodes) {
  ALPHA_DASSERT(index < nodes.size(), "Index out of range.");
  auto& node = nodes[index++];
  auto offset = node->offset();
  auto kind = node->kind();
  switch (kind) {
    case Kind::Scalar: {
      return {
          .type = std::make_shared<ScalarType>(offset, node->scalarKind()),
          .name = node->name()};
    }
    case Kind::Array: {
      auto elements = getType(index, nodes).type;
      return {
          .type = std::make_shared<ArrayType>(offset, std::move(elements)),
          .name = node->name()};
    }
    case Kind::ArrayWithOffsets: {
      auto offsets = getType(index, nodes).type;
      ALPHA_ASSERT(
          offsets->kind() == Kind::Scalar &&
              offsets->asScalar().scalarKind() == ScalarKind::UInt32,
          "Array with offsets field must have a uint32 scalar type.");
      auto offsetsScalarType =
          std::dynamic_pointer_cast<const ScalarType>(offsets);
      auto elements = getType(index, nodes).type;
      return {
          .type = std::make_shared<ArrayWithOffsetsType>(
              offset, std::move(offsetsScalarType), std::move(elements)),
          .name = node->name()};
    }
    case Kind::Row: {
      auto childrenCount = node->childrenCount();
      std::vector<std::string> names{childrenCount};
      std::vector<std::shared_ptr<const Type>> children{childrenCount};
      for (auto i = 0; i < childrenCount; ++i) {
        auto namedType = getType(index, nodes);
        ALPHA_ASSERT(namedType.name.has_value(), "Row fields must have names.");
        children[i] = namedType.type;
        names[i] = namedType.name.value();
      }
      return {
          .type = std::make_shared<RowType>(
              offset, std::move(names), std::move(children)),
          .name = node->name()};
    }
    case Kind::Map: {
      auto keys = getType(index, nodes).type;
      auto values = getType(index, nodes).type;
      return {
          .type = std::make_shared<MapType>(
              offset, std::move(keys), std::move(values)),
          .name = node->name()};
    }
    case Kind::FlatMap: {
      auto childrenCount = node->childrenCount();

      std::vector<std::string> names{childrenCount};
      std::vector<std::shared_ptr<const ScalarType>> inMaps{childrenCount};
      std::vector<std::shared_ptr<const Type>> children{childrenCount};

      for (auto i = 0; i < childrenCount; ++i) {
        auto inMap = getType(index, nodes);
        ALPHA_ASSERT(
            inMap.type->kind() == Kind::Scalar &&
                inMap.type->asScalar().scalarKind() == ScalarKind::Bool,
            "Flat map in-map field must have a boolean scalar type.");
        ALPHA_ASSERT(
            inMap.name.has_value(), "Flat map fields must have names.");
        auto field = getType(index, nodes);
        names[i] = inMap.name.value();
        inMaps[i] = std::dynamic_pointer_cast<const ScalarType>(inMap.type);
        children[i] = field.type;
      }
      return {
          .type = std::make_shared<FlatMapType>(
              offset,
              node->scalarKind(),
              std::move(names),
              std::move(inMaps),
              std::move(children)),
          .name = node->name()};
    }
    default: {
      ALPHA_UNREACHABLE(fmt::format("Unknown node kind: ", kind));
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
          arrayWithOffsets.offsets(),
          visitor,
          {.name = "offsets", .parentType = type.get()});
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
            map.inMapAt(i),
            visitor,
            {.name = "inMap",
             .parentType = type.get(),
             .placeInSibling = i * 2});
        traverseSchema(
            index,
            level + 1,
            map.childAt(i),
            visitor,
            {.name = map.nameAt(i),
             .parentType = type.get(),
             .placeInSibling = i * 2 + 1});
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
  alpha::traverseSchema(
      index, 0, root, visitor, {.name = "root", .parentType = nullptr});
}

std::ostream& operator<<(
    std::ostream& out,
    const std::shared_ptr<const Type>& root) {
  std::optional<std::string> lastName;
  SchemaReader::traverseSchema(
      root,
      [&out, &lastName](
          uint32_t level, const Type& type, const SchemaReader::NodeInfo&) {
        out << std::string((std::basic_string<char>::size_type)level * 2, ' ')
            << "[" << type.offset() << "]"
            << (lastName.has_value() ? lastName.value() + ":" : "");
        if (type.isScalar()) {
          auto& scalar = type.asScalar();
          out << toString(scalar.scalarKind()) << "\n";
        } else if (type.isArray()) {
          out << "ARRAY\n";
        } else if (type.isArrayWithOffsets()) {
          out << "OFFSETARRAY\n";
        } else if (type.isRow()) {
          auto& row = type.asRow();
          out << "ROW[";
          for (auto i = 0; i < row.childrenCount(); ++i) {
            out << row.nameAt(i) << (i < row.childrenCount() - 1 ? "," : "");
          }
          out << "]\n";
        } else if (type.isMap()) {
          out << "MAP\n";
        } else if (type.isFlatMap()) {
          auto& map = type.asFlatMap();
          out << "FLATMAP[";
          for (auto i = 0; i < map.childrenCount(); ++i) {
            out << map.nameAt(i) << (i < map.childrenCount() - 1 ? "," : "");
          }
          out << "]\n";
        }
      });
  return out;
}

} // namespace facebook::alpha
