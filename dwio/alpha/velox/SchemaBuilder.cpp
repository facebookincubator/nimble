// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/SchemaTypes.h"

namespace facebook::alpha {

TypeBuilder::TypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset,
    Kind kind)
    : Type{offset, kind}, schemaBuilder_{schemaBuilder} {}

ScalarTypeBuilder::ScalarTypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset,
    ScalarKind scalarKind)
    : Type{offset, Kind::Scalar},
      TypeBuilder{schemaBuilder, offset, Kind::Scalar},
      ScalarType{offset, scalarKind} {}

ArrayTypeBuilder::ArrayTypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset)
    : Type{offset, Kind::Array},
      TypeBuilder(schemaBuilder, offset, Kind::Array),
      ArrayType{offset, nullptr} {}

const TypeBuilder& ArrayTypeBuilder::elements() const {
  return dynamic_cast<const TypeBuilder&>(*ArrayType::elements());
}

void ArrayTypeBuilder::setChildren(std::shared_ptr<TypeBuilder> elements) {
  ALPHA_ASSERT(!elements_, "ArrayTypeBuilder elements already initialized.");
  schemaBuilder_.registerChild(elements);
  elements_ = std::move(elements);
}

ArrayWithOffsetsTypeBuilder::ArrayWithOffsetsTypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset)
    : Type{offset, Kind::ArrayWithOffsets},
      TypeBuilder(schemaBuilder, offset, Kind::ArrayWithOffsets),
      ArrayWithOffsetsType{offset, nullptr, nullptr} {}

const ScalarTypeBuilder& ArrayWithOffsetsTypeBuilder::offsets() const {
  return dynamic_cast<const ScalarTypeBuilder&>(
      *ArrayWithOffsetsType::offsets());
}

const TypeBuilder& ArrayWithOffsetsTypeBuilder::elements() const {
  return dynamic_cast<const ScalarTypeBuilder&>(
      *ArrayWithOffsetsType::elements());
}

void ArrayWithOffsetsTypeBuilder::setChildren(
    std::shared_ptr<ScalarTypeBuilder> offsets,
    std::shared_ptr<TypeBuilder> elements) {
  ALPHA_ASSERT(
      !elements_, "ArrayWithOffsetsTypeBuilder elements already initialized.");
  ALPHA_ASSERT(
      !offsets_, "ArrayWithOffsetsTypeBuilder offsets already initialized.");
  ALPHA_ASSERT(
      offsets->scalarKind() == ScalarKind::UInt32,
      "ArrayWithOffsets offsets should be of type uint32_t")
  schemaBuilder_.registerChild(offsets);
  schemaBuilder_.registerChild(elements);
  offsets_ = std::move(offsets);
  elements_ = std::move(elements);
}

RowTypeBuilder::RowTypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset,
    size_t childrenCount)
    : Type{offset, Kind::Row},
      TypeBuilder{schemaBuilder, offset, Kind::Row},
      RowType{offset, {}, {}} {
  names_.reserve(childrenCount);
  children_.reserve(childrenCount);
}

const TypeBuilder& RowTypeBuilder::childAt(size_t index) const {
  return dynamic_cast<const TypeBuilder&>(*RowType::childAt(index));
}

void RowTypeBuilder::addChild(
    std::string name,
    std::shared_ptr<TypeBuilder> child) {
  ALPHA_DASSERT(
      children_.size() < children_.capacity(),
      fmt::format(
          "Registering more row children than expected. Capacity: {}",
          children_.capacity()));
  schemaBuilder_.registerChild(child);
  names_.push_back(std::move(name));
  children_.push_back(std::move(child));
}

MapTypeBuilder::MapTypeBuilder(SchemaBuilder& schemaBuilder, offset_size offset)
    : Type{offset, Kind::Map},
      TypeBuilder{schemaBuilder, offset, Kind::Map},
      MapType{offset, nullptr, nullptr} {}

const TypeBuilder& MapTypeBuilder::keys() const {
  return dynamic_cast<const TypeBuilder&>(*MapType::keys());
}

const TypeBuilder& MapTypeBuilder::values() const {
  return dynamic_cast<const TypeBuilder&>(*MapType::values());
}

void MapTypeBuilder::setChildren(
    std::shared_ptr<TypeBuilder> keys,
    std::shared_ptr<TypeBuilder> values) {
  ALPHA_ASSERT(!keys_, "MapTypeBuilder keys already initialized.");
  ALPHA_ASSERT(!values_, "MapTypeBuilder values already initialized.");
  schemaBuilder_.registerChild(keys);
  schemaBuilder_.registerChild(values);
  keys_ = std::move(keys);
  values_ = std::move(values);
}

FlatMapTypeBuilder::FlatMapTypeBuilder(
    SchemaBuilder& schemaBuilder,
    offset_size offset,
    ScalarKind keyScalarKind)
    : Type{offset, Kind::FlatMap},
      TypeBuilder{schemaBuilder, offset, Kind::FlatMap},
      FlatMapType{offset, keyScalarKind, {}, {}, {}} {}

const ScalarTypeBuilder& FlatMapTypeBuilder::inMapAt(size_t index) const {
  return dynamic_cast<const ScalarTypeBuilder&>(*FlatMapType::inMapAt(index));
}

const TypeBuilder& FlatMapTypeBuilder::childAt(size_t index) const {
  return dynamic_cast<const TypeBuilder&>(*FlatMapType::childAt(index));
}

void FlatMapTypeBuilder::addChild(
    std::string name,
    std::shared_ptr<ScalarTypeBuilder> inMap,
    std::shared_ptr<TypeBuilder> child) {
  schemaBuilder_.registerChild(inMap);
  schemaBuilder_.registerChild(child);

  ALPHA_ASSERT(
      inMap->scalarKind() == ScalarKind::Bool,
      "Flat map in-map field must have a boolean scalar type.");
  names_.push_back(std::move(name));
  inMaps_.push_back(std::move(inMap));
  // TODO: ideally, need to check that children are of the same type.
  children_.push_back(std::move(child));
}

std::shared_ptr<ScalarTypeBuilder> SchemaBuilder::createScalarTypeBuilder(
    ScalarKind scalarKind) {
  struct MakeSharedEnabler : public ScalarTypeBuilder {
    MakeSharedEnabler(
        SchemaBuilder& schemaBuilder,
        offset_size offset,
        ScalarKind scalarKind)
        : Type{offset, Kind::Scalar},
          TypeBuilder{schemaBuilder, offset, Kind::Scalar},
          ScalarType{offset, scalarKind},
          ScalarTypeBuilder{schemaBuilder, offset, scalarKind} {}
  };

  auto type =
      std::make_shared<MakeSharedEnabler>(*this, currentOffset_++, scalarKind);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<ArrayTypeBuilder> SchemaBuilder::createArrayTypeBuilder() {
  struct MakeSharedEnabler : public ArrayTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, offset_size offset)
        : Type{offset, Kind::Array},
          TypeBuilder{schemaBuilder, offset, Kind::Array},
          ArrayType{offset, nullptr},
          ArrayTypeBuilder{schemaBuilder, offset} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this, currentOffset_++);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<ArrayWithOffsetsTypeBuilder>
SchemaBuilder::createArrayWithOffsetsTypeBuilder() {
  struct MakeSharedEnabler : public ArrayWithOffsetsTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, offset_size offset)
        : Type{offset, Kind::ArrayWithOffsets},
          TypeBuilder{schemaBuilder, offset, Kind::ArrayWithOffsets},
          ArrayWithOffsetsType{offset, nullptr, nullptr},
          ArrayWithOffsetsTypeBuilder{schemaBuilder, offset} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this, currentOffset_++);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<RowTypeBuilder> SchemaBuilder::createRowTypeBuilder(
    size_t childrenCount) {
  struct MakeSharedEnabler : public RowTypeBuilder {
    MakeSharedEnabler(
        SchemaBuilder& schemaBuilder,
        offset_size offset,
        size_t childrenCount)
        : Type{offset, Kind::Row},
          TypeBuilder{schemaBuilder, offset, Kind::Row},
          RowType{offset, {}, {}},
          RowTypeBuilder{schemaBuilder, offset, childrenCount} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(
      *this, currentOffset_++, childrenCount);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<MapTypeBuilder> SchemaBuilder::createMapTypeBuilder() {
  struct MakeSharedEnabler : public MapTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, offset_size offset)
        : Type{offset, Kind::Map},
          TypeBuilder{schemaBuilder, offset, Kind::Map},
          MapType{offset, nullptr, nullptr},
          MapTypeBuilder{schemaBuilder, offset} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this, currentOffset_++);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<FlatMapTypeBuilder> SchemaBuilder::createFlatMapTypeBuilder(
    ScalarKind keyScalarKind) {
  struct MakeSharedEnabler : public FlatMapTypeBuilder {
    MakeSharedEnabler(
        SchemaBuilder& schemaBuilder,
        offset_size offset,
        ScalarKind keyScalarKind)
        : Type{offset, Kind::FlatMap},
          TypeBuilder{schemaBuilder, offset, Kind::FlatMap},
          FlatMapType{offset, keyScalarKind, {}, {}, {}},
          FlatMapTypeBuilder(schemaBuilder, offset, keyScalarKind) {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(
      *this, currentOffset_++, keyScalarKind);
  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

offset_size SchemaBuilder::nodeCount() const {
  return currentOffset_;
}

const std::shared_ptr<const TypeBuilder>& SchemaBuilder::getRoot() const {
  // When retreiving schema nodes, we return a vector ordered based on the
  // schema tree DFS order. To be able to flatten the schema tree to a flat
  // ordered vector, we need to guarantee that the schema tree has a single
  // root node, where we start traversing from.
  ALPHA_ASSERT(
      roots_.size() == 1,
      fmt::format(
          "Unable to determine schema root. List of roots contain {} entries.",
          roots_.size()));
  return *roots_.cbegin();
}

void SchemaBuilder::registerChild(const std::shared_ptr<TypeBuilder>& type) {
  std::scoped_lock<std::mutex> l(mutex_);
  // If we try to attach a node to a parent, but this node doesn't exist in
  // the roots list, it means that either this node was already attached to a
  // parent before (and therefore was removed from the roots list), or the
  // node was created using a different schema builder instance (and therefore
  // belongs to a roots list in the other schema builder instance).
  ALPHA_ASSERT(
      roots_.find(type) != roots_.end(),
      "Child type not found. This can happen if child is registered more than once, "
      "or if a different Schema Builder was used to create the child.");

  // Now that the node is attached to a parent, it is no longer a "root" of a
  // tree, and should be removed from the list of roots.
  roots_.erase(type);
}

void SchemaBuilder::addNode(
    std::vector<std::unique_ptr<const SchemaNode>>& nodes,
    const TypeBuilder& type,
    std::optional<std::string> name) const {
  switch (type.kind()) {
    case Kind::Scalar: {
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(),
          type.offset(),
          std::move(name),
          dynamic_cast<const ScalarTypeBuilder&>(type).scalarKind()));
      break;
    }
    case Kind::Array: {
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(), type.offset(), std::move(name)));
      addNode(nodes, dynamic_cast<const ArrayTypeBuilder&>(type).elements());
      break;
    }
    case Kind::ArrayWithOffsets: {
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(), type.offset(), std::move(name)));
      auto& arrayWithOffsets =
          dynamic_cast<const ArrayWithOffsetsTypeBuilder&>(type);
      addNode(nodes, arrayWithOffsets.offsets());
      addNode(nodes, arrayWithOffsets.elements());
      break;
    }
    case Kind::Row: {
      auto& row = dynamic_cast<const RowTypeBuilder&>(type);
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(),
          type.offset(),
          std::move(name),
          ScalarKind::Undefined,
          row.childrenCount()));
      for (auto i = 0; i < row.childrenCount(); ++i) {
        addNode(nodes, row.childAt(i), row.nameAt(i));
      }
      break;
    }
    case Kind::Map: {
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(), type.offset(), std::move(name)));
      auto& map = dynamic_cast<const MapTypeBuilder&>(type);
      addNode(nodes, map.keys());
      addNode(nodes, map.values());
      break;
    }
    case Kind::FlatMap: {
      auto& flatMap = dynamic_cast<const FlatMapTypeBuilder&>(type);

      size_t childrenSize = flatMap.childrenCount();
      nodes.push_back(std::make_unique<SchemaNode>(
          type.kind(),
          type.offset(),
          std::move(name),
          flatMap.keyScalarKind(),
          childrenSize));
      ALPHA_ASSERT(
          flatMap.inMaps_.size() == childrenSize,
          "Flat map in-maps collection size and children collection size should be the same.");
      for (size_t i = 0; i < childrenSize; ++i) {
        addNode(nodes, flatMap.inMapAt(i), flatMap.nameAt(i));
        addNode(nodes, flatMap.childAt(i));
      }

      break;
    }

    default:
      ALPHA_UNREACHABLE(fmt::format("Unknown type kind: {}.", type.kind()));
  }
}

std::vector<std::unique_ptr<const SchemaNode>> SchemaBuilder::getSchemaNodes()
    const {
  auto& root = getRoot();
  std::vector<std::unique_ptr<const SchemaNode>> nodes;
  nodes.reserve(currentOffset_);
  addNode(nodes, *root);
  return nodes;
}

std::shared_ptr<TypeBuilder> SchemaBuilder::createBuilderByTypeThreadSafe(
    Kind kind) {
  {
    std::scoped_lock<std::mutex> l(mutex_);
    switch (kind) {
      case Kind::Array:
        return createArrayTypeBuilder();
      case Kind::ArrayWithOffsets:
        return createArrayWithOffsetsTypeBuilder();
      case Kind::Map:
        return createMapTypeBuilder();
      default:
        ALPHA_UNREACHABLE(fmt::format("Unsupported type kind {}", kind));
    }
  }
}

std::shared_ptr<TypeBuilder> SchemaBuilder::createBuilderByTypeThreadSafe(
    Kind kind,
    ScalarKind scalarKind) {
  {
    std::scoped_lock<std::mutex> l(mutex_);
    switch (kind) {
      case Kind::FlatMap:
        return createFlatMapTypeBuilder(scalarKind);
      case Kind::Scalar:
        return createScalarTypeBuilder(scalarKind);
      default:
        ALPHA_UNREACHABLE(fmt::format("Unsupported type kind {}", kind));
    }
  }
}

std::shared_ptr<TypeBuilder> SchemaBuilder::createBuilderByTypeThreadSafe(
    Kind kind,
    size_t param) {
  {
    std::scoped_lock<std::mutex> l(mutex_);
    switch (kind) {
      case Kind::Row:
        return createRowTypeBuilder(param);
      default:
        ALPHA_UNREACHABLE(fmt::format("Unsupported type kind {}", kind));
    }
  }
}

void printType(
    std::ostream& out,
    const TypeBuilder& builder,
    uint32_t indentation,
    const std::optional<std::string>& name = std::nullopt) {
  out << std::string(indentation, ' ') << "[" << builder.offset() << "]"
      << (name.has_value() ? name.value() + ":" : "");

  if (auto scalar = dynamic_cast<const ScalarTypeBuilder*>(&builder)) {
    out << toString(scalar->scalarKind()) << "\n";
    return;
  }

  if (auto array = dynamic_cast<const ArrayTypeBuilder*>(&builder)) {
    out << "ARRAY\n";
    printType(out, array->elements(), indentation + 2, "elements");
    out << "\n";
    return;
  }

  if (auto arrayWithOffsets =
          dynamic_cast<const ArrayWithOffsetsTypeBuilder*>(&builder)) {
    out << "OFFSETARRAY\n";
    printType(out, arrayWithOffsets->offsets(), indentation + 2, "offsets");
    printType(out, arrayWithOffsets->elements(), indentation + 2, "elements");
    out << "\n";
    return;
  }

  if (auto row = dynamic_cast<const RowTypeBuilder*>(&builder)) {
    out << "ROW\n";
    for (auto i = 0; i < row->childrenCount(); ++i) {
      printType(out, row->childAt(i), indentation + 2, row->nameAt(i));
    }
    out << "\n";
    return;
  }

  if (auto map = dynamic_cast<const MapTypeBuilder*>(&builder)) {
    out << "MAP\n";
    printType(out, map->keys(), indentation + 2, "keys");
    printType(out, map->values(), indentation + 2, "values");
    out << "\n";
    return;
  }

  if (auto flatmap = dynamic_cast<const FlatMapTypeBuilder*>(&builder)) {
    out << "FLATMAP\n";
    for (auto i = 0; i < flatmap->childrenCount(); ++i) {
      printType(
          out,
          flatmap->childAt(i),
          indentation + 2,
          // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
          flatmap->nameAt(i));
    }
    out << "\n";
    return;
  }
}

std::ostream& operator<<(std::ostream& out, const SchemaBuilder& schema) {
  size_t index = 0;
  for (const auto& root : schema.roots_) {
    out << "Root " << index++ << ":\n";
    printType(out, *root, 2);
  }

  return out;
}
} // namespace facebook::alpha
