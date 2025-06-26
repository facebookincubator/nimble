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
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/SchemaTypes.h"

namespace facebook::nimble {

TypeBuilder::TypeBuilder(SchemaBuilder& schemaBuilder, Kind kind)
    : schemaBuilder_{schemaBuilder}, kind_{kind} {}

ScalarTypeBuilder& TypeBuilder::asScalar() {
  return dynamic_cast<ScalarTypeBuilder&>(*this);
}

ArrayTypeBuilder& TypeBuilder::asArray() {
  return dynamic_cast<ArrayTypeBuilder&>(*this);
}

MapTypeBuilder& TypeBuilder::asMap() {
  return dynamic_cast<MapTypeBuilder&>(*this);
}

SlidingWindowMapTypeBuilder& TypeBuilder::asSlidingWindowMap() {
  return dynamic_cast<SlidingWindowMapTypeBuilder&>(*this);
}

RowTypeBuilder& TypeBuilder::asRow() {
  return dynamic_cast<RowTypeBuilder&>(*this);
}

FlatMapTypeBuilder& TypeBuilder::asFlatMap() {
  return dynamic_cast<FlatMapTypeBuilder&>(*this);
}

ArrayWithOffsetsTypeBuilder& TypeBuilder::asArrayWithOffsets() {
  return dynamic_cast<ArrayWithOffsetsTypeBuilder&>(*this);
}

const ScalarTypeBuilder& TypeBuilder::asScalar() const {
  return dynamic_cast<const ScalarTypeBuilder&>(*this);
}

const ArrayTypeBuilder& TypeBuilder::asArray() const {
  return dynamic_cast<const ArrayTypeBuilder&>(*this);
}

const MapTypeBuilder& TypeBuilder::asMap() const {
  return dynamic_cast<const MapTypeBuilder&>(*this);
}

const SlidingWindowMapTypeBuilder& TypeBuilder::asSlidingWindowMap() const {
  return dynamic_cast<const SlidingWindowMapTypeBuilder&>(*this);
}

const RowTypeBuilder& TypeBuilder::asRow() const {
  return dynamic_cast<const RowTypeBuilder&>(*this);
}

const FlatMapTypeBuilder& TypeBuilder::asFlatMap() const {
  return dynamic_cast<const FlatMapTypeBuilder&>(*this);
}

const ArrayWithOffsetsTypeBuilder& TypeBuilder::asArrayWithOffsets() const {
  return dynamic_cast<const ArrayWithOffsetsTypeBuilder&>(*this);
}

Kind TypeBuilder::kind() const {
  return kind_;
}

ScalarTypeBuilder::ScalarTypeBuilder(
    SchemaBuilder& schemaBuilder,
    ScalarKind scalarKind)
    : TypeBuilder{schemaBuilder, Kind::Scalar},
      scalarDescriptor_{schemaBuilder_.allocateStreamOffset(), scalarKind} {}

const StreamDescriptorBuilder& ScalarTypeBuilder::scalarDescriptor() const {
  return scalarDescriptor_;
}

LengthsTypeBuilder::LengthsTypeBuilder(SchemaBuilder& schemaBuilder, Kind kind)
    : TypeBuilder(schemaBuilder, kind),
      lengthsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::UInt32} {}

const StreamDescriptorBuilder& LengthsTypeBuilder::lengthsDescriptor() const {
  return lengthsDescriptor_;
}

ArrayTypeBuilder::ArrayTypeBuilder(SchemaBuilder& schemaBuilder)
    : LengthsTypeBuilder(schemaBuilder, Kind::Array) {}

const TypeBuilder& ArrayTypeBuilder::elements() const {
  return *elements_;
}

void ArrayTypeBuilder::setChildren(std::shared_ptr<TypeBuilder> elements) {
  NIMBLE_ASSERT(!elements_, "ArrayTypeBuilder elements already initialized.");
  schemaBuilder_.registerChild(elements);
  elements_ = std::move(elements);
}

MapTypeBuilder::MapTypeBuilder(SchemaBuilder& schemaBuilder)
    : LengthsTypeBuilder{schemaBuilder, Kind::Map} {}

const TypeBuilder& MapTypeBuilder::keys() const {
  return *keys_;
}

const TypeBuilder& MapTypeBuilder::values() const {
  return *values_;
}

void MapTypeBuilder::setChildren(
    std::shared_ptr<TypeBuilder> keys,
    std::shared_ptr<TypeBuilder> values) {
  NIMBLE_ASSERT(!keys_, "MapTypeBuilder keys already initialized.");
  NIMBLE_ASSERT(!values_, "MapTypeBuilder values already initialized.");
  schemaBuilder_.registerChild(keys);
  schemaBuilder_.registerChild(values);
  keys_ = std::move(keys);
  values_ = std::move(values);
}

ArrayWithOffsetsTypeBuilder::ArrayWithOffsetsTypeBuilder(
    SchemaBuilder& schemaBuilder)
    : TypeBuilder(schemaBuilder, Kind::ArrayWithOffsets),
      offsetsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::UInt32},
      lengthsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::UInt32} {}

const StreamDescriptorBuilder& ArrayWithOffsetsTypeBuilder::offsetsDescriptor()
    const {
  return offsetsDescriptor_;
}

const StreamDescriptorBuilder& ArrayWithOffsetsTypeBuilder::lengthsDescriptor()
    const {
  return lengthsDescriptor_;
}

const TypeBuilder& ArrayWithOffsetsTypeBuilder::elements() const {
  return *elements_;
}

void ArrayWithOffsetsTypeBuilder::setChildren(
    std::shared_ptr<TypeBuilder> elements) {
  NIMBLE_ASSERT(
      !elements_, "ArrayWithOffsetsTypeBuilder elements already initialized.");
  schemaBuilder_.registerChild(elements);
  elements_ = std::move(elements);
}

RowTypeBuilder::RowTypeBuilder(
    SchemaBuilder& schemaBuilder,
    size_t childrenCount)
    : TypeBuilder{schemaBuilder, Kind::Row},
      nullsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::Bool} {
  names_.reserve(childrenCount);
  children_.reserve(childrenCount);
}

const StreamDescriptorBuilder& RowTypeBuilder::nullsDescriptor() const {
  return nullsDescriptor_;
}

size_t RowTypeBuilder::childrenCount() const {
  return children_.size();
}

const TypeBuilder& RowTypeBuilder::childAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return *children_[index];
}

const std::string& RowTypeBuilder::nameAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return names_[index];
}

void RowTypeBuilder::addChild(
    std::string name,
    std::shared_ptr<TypeBuilder> child) {
  NIMBLE_DASSERT(
      children_.size() < children_.capacity(),
      fmt::format(
          "Registering more row children than expected. Capacity: {}",
          children_.capacity()));
  schemaBuilder_.registerChild(child);
  names_.push_back(std::move(name));
  children_.push_back(std::move(child));
}

SlidingWindowMapTypeBuilder::SlidingWindowMapTypeBuilder(
    SchemaBuilder& schemaBuilder)
    : TypeBuilder{schemaBuilder, Kind::SlidingWindowMap},
      offsetsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::UInt32},
      lengthsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::UInt32} {}

const StreamDescriptorBuilder& SlidingWindowMapTypeBuilder::offsetsDescriptor()
    const {
  return offsetsDescriptor_;
}

const StreamDescriptorBuilder& SlidingWindowMapTypeBuilder::lengthsDescriptor()
    const {
  return lengthsDescriptor_;
}

const TypeBuilder& SlidingWindowMapTypeBuilder::keys() const {
  return *keys_;
}

const TypeBuilder& SlidingWindowMapTypeBuilder::values() const {
  return *values_;
}

void SlidingWindowMapTypeBuilder::setChildren(
    std::shared_ptr<TypeBuilder> keys,
    std::shared_ptr<TypeBuilder> values) {
  NIMBLE_ASSERT(
      !keys_, "SlidingWindowMapTypeBuilder keys already initialized.");
  NIMBLE_ASSERT(
      !values_, "SlidingWindowMapTypeBuilder values already initialized.");
  schemaBuilder_.registerChild(keys);
  schemaBuilder_.registerChild(values);
  keys_ = std::move(keys);
  values_ = std::move(values);
}

FlatMapTypeBuilder::FlatMapTypeBuilder(
    SchemaBuilder& schemaBuilder,
    ScalarKind keyScalarKind)
    : TypeBuilder{schemaBuilder, Kind::FlatMap},
      keyScalarKind_{keyScalarKind},
      nullsDescriptor_{
          schemaBuilder_.allocateStreamOffset(),
          ScalarKind::Bool} {}

const StreamDescriptorBuilder& FlatMapTypeBuilder::nullsDescriptor() const {
  return nullsDescriptor_;
}

const StreamDescriptorBuilder& FlatMapTypeBuilder::inMapDescriptorAt(
    size_t index) const {
  NIMBLE_ASSERT(
      index < inMapDescriptors_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.",
          index,
          inMapDescriptors_.size()));
  return *inMapDescriptors_[index];
}

const TypeBuilder& FlatMapTypeBuilder::childAt(size_t index) const {
  NIMBLE_ASSERT(
      index < children_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, children_.size()));
  return *children_[index];
}

ScalarKind FlatMapTypeBuilder::keyScalarKind() const {
  return keyScalarKind_;
}

size_t FlatMapTypeBuilder::childrenCount() const {
  return children_.size();
}

const std::string& FlatMapTypeBuilder::nameAt(size_t index) const {
  NIMBLE_ASSERT(
      index < names_.size(),
      fmt::format(
          "Index out of range. index: {}, size: {}.", index, names_.size()));
  return names_[index];
}

const StreamDescriptorBuilder& FlatMapTypeBuilder::addChild(
    std::string name,
    std::shared_ptr<TypeBuilder> child) {
  auto& inMapDescriptor =
      inMapDescriptors_.emplace_back(std::make_unique<StreamDescriptorBuilder>(
          schemaBuilder_.allocateStreamOffset(), ScalarKind::Bool));

  schemaBuilder_.registerChild(child);

  names_.push_back(std::move(name));
  children_.push_back(std::move(child));

  return *inMapDescriptor;
}

std::shared_ptr<ScalarTypeBuilder> SchemaBuilder::createScalarTypeBuilder(
    ScalarKind scalarKind) {
  struct MakeSharedEnabler : public ScalarTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, ScalarKind scalarKind)
        : ScalarTypeBuilder{schemaBuilder, scalarKind} {}
  };

  auto type = std::make_shared<MakeSharedEnabler>(*this, scalarKind);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<ArrayTypeBuilder> SchemaBuilder::createArrayTypeBuilder() {
  struct MakeSharedEnabler : public ArrayTypeBuilder {
    explicit MakeSharedEnabler(SchemaBuilder& schemaBuilder)
        : ArrayTypeBuilder{schemaBuilder} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<ArrayWithOffsetsTypeBuilder>
SchemaBuilder::createArrayWithOffsetsTypeBuilder() {
  struct MakeSharedEnabler : public ArrayWithOffsetsTypeBuilder {
    explicit MakeSharedEnabler(SchemaBuilder& schemaBuilder)
        : ArrayWithOffsetsTypeBuilder{schemaBuilder} {}
  };

  auto type = std::make_shared<MakeSharedEnabler>(*this);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<RowTypeBuilder> SchemaBuilder::createRowTypeBuilder(
    size_t childrenCount) {
  struct MakeSharedEnabler : public RowTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, size_t childrenCount)
        : RowTypeBuilder{schemaBuilder, childrenCount} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this, childrenCount);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<MapTypeBuilder> SchemaBuilder::createMapTypeBuilder() {
  struct MakeSharedEnabler : public MapTypeBuilder {
    explicit MakeSharedEnabler(SchemaBuilder& schemaBuilder)
        : MapTypeBuilder{schemaBuilder} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<SlidingWindowMapTypeBuilder>
SchemaBuilder::createSlidingWindowMapTypeBuilder() {
  struct MakeSharedEnabler : public SlidingWindowMapTypeBuilder {
    explicit MakeSharedEnabler(SchemaBuilder& schemaBuilder)
        : SlidingWindowMapTypeBuilder{schemaBuilder} {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this);

  // This new type builder is not attached to a parent, therefore it is a new
  // tree "root" (as of now), so we add it to the roots list.
  roots_.insert(type);
  return type;
}

std::shared_ptr<FlatMapTypeBuilder> SchemaBuilder::createFlatMapTypeBuilder(
    ScalarKind keyScalarKind) {
  struct MakeSharedEnabler : public FlatMapTypeBuilder {
    MakeSharedEnabler(SchemaBuilder& schemaBuilder, ScalarKind keyScalarKind)
        : FlatMapTypeBuilder(schemaBuilder, keyScalarKind) {}
  };
  auto type = std::make_shared<MakeSharedEnabler>(*this, keyScalarKind);

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
  // ordered vector, we need to guarantee that the schema tree has a single root
  // node, where we start traversing from.
  NIMBLE_ASSERT(
      roots_.size() == 1,
      fmt::format(
          "Unable to determine schema root. List of roots contain {} entries.",
          roots_.size()));
  return *roots_.cbegin();
}

void SchemaBuilder::registerChild(const std::shared_ptr<TypeBuilder>& type) {
  // If we try to attach a node to a parent, but this node doesn't exist in the
  // roots list, it means that either this node was already attached to a parent
  // before (and therefore was removed from the roots list), or the node was
  // created using a different schema builder instance (and therefore belongs to
  // a roots list in the other schema builder instance).
  NIMBLE_ASSERT(
      roots_.find(type) != roots_.end(),
      "Child type not found. This can happen if child is registered more than once, "
      "or if a different Schema Builder was used to create the child.");

  // Now that the node is attached to a parent, it is no longer a "root" of a
  // tree, and should be removed from the list of roots.
  roots_.erase(type);
}

offset_size SchemaBuilder::allocateStreamOffset() {
  return currentOffset_++;
}

void SchemaBuilder::addNode(
    std::vector<SchemaNode>& nodes,
    const TypeBuilder& type,
    std::optional<std::string> name) const {
  switch (type.kind()) {
    case Kind::Scalar: {
      const auto& scalar = type.asScalar();
      nodes.emplace_back(
          type.kind(),
          scalar.scalarDescriptor().offset(),
          scalar.scalarDescriptor().scalarKind(),
          std::move(name));
      break;
    }
    case Kind::Array: {
      const auto& array = type.asArray();
      nodes.emplace_back(
          type.kind(),
          array.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      addNode(nodes, array.elements());
      break;
    }
    case Kind::ArrayWithOffsets: {
      const auto& array = type.asArrayWithOffsets();
      nodes.emplace_back(
          type.kind(),
          array.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      nodes.emplace_back(
          Kind::Scalar,
          array.offsetsDescriptor().offset(),
          ScalarKind::UInt32,
          std::nullopt);
      addNode(nodes, array.elements());
      break;
    }
    case Kind::Row: {
      auto& row = type.asRow();
      nodes.emplace_back(
          type.kind(),
          row.nullsDescriptor().offset(),
          ScalarKind::Bool,
          std::move(name),
          row.childrenCount());
      for (auto i = 0; i < row.childrenCount(); ++i) {
        addNode(nodes, row.childAt(i), row.nameAt(i));
      }
      break;
    }
    case Kind::Map: {
      const auto& map = type.asMap();
      nodes.emplace_back(
          type.kind(),
          map.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      addNode(nodes, map.keys());
      addNode(nodes, map.values());
      break;
    }
    case Kind::SlidingWindowMap: {
      const auto& map = type.asSlidingWindowMap();
      nodes.emplace_back(
          type.kind(),
          map.offsetsDescriptor().offset(),
          ScalarKind::UInt32,
          std::move(name));
      nodes.emplace_back(
          Kind::Scalar,
          map.lengthsDescriptor().offset(),
          ScalarKind::UInt32,
          std::nullopt);
      addNode(nodes, map.keys());
      addNode(nodes, map.values());
      break;
    }
    case Kind::FlatMap: {
      auto& map = type.asFlatMap();

      size_t childrenSize = map.childrenCount();
      nodes.emplace_back(
          type.kind(),
          map.nullsDescriptor().offset(),
          map.keyScalarKind(),
          std::move(name),
          childrenSize);
      NIMBLE_ASSERT(
          map.inMapDescriptors_.size() == childrenSize,
          "Flat map in-maps collection size and children collection size should be the same.");
      for (size_t i = 0; i < childrenSize; ++i) {
        nodes.emplace_back(
            Kind::Scalar,
            map.inMapDescriptorAt(i).offset(),
            ScalarKind::Bool,
            map.nameAt(i));
        addNode(nodes, map.childAt(i));
      }

      break;
    }

    default:
      NIMBLE_UNREACHABLE(
          fmt::format("Unknown type kind: {}.", toString(type.kind())));
  }
}

std::vector<SchemaNode> SchemaBuilder::getSchemaNodes() const {
  auto& root = getRoot();
  std::vector<SchemaNode> nodes;
  nodes.reserve(currentOffset_);
  addNode(nodes, *root);
  return nodes;
}

void printType(
    std::ostream& out,
    const TypeBuilder& builder,
    uint32_t indentation,
    const std::optional<std::string>& name = std::nullopt) {
  out << std::string(indentation, ' ')
      << (name.has_value() ? name.value() + ":" : "");

  switch (builder.kind()) {
    case Kind::Scalar: {
      const auto& scalar = builder.asScalar();
      out << "[" << scalar.scalarDescriptor().offset() << "]"
          << toString(scalar.scalarDescriptor().scalarKind()) << "\n";
      break;
    }
    case Kind::Array: {
      const auto& array = builder.asArray();
      out << "[" << array.lengthsDescriptor().offset() << "]ARRAY\n";
      printType(out, array.elements(), indentation + 2, "elements");
      out << "\n";
      break;
    }
    case Kind::Map: {
      const auto& map = builder.asMap();
      out << "[" << map.lengthsDescriptor().offset() << "]MAP\n";
      printType(out, map.keys(), indentation + 2, "keys");
      printType(out, map.values(), indentation + 2, "values");
      out << "\n";
      break;
    }
    case Kind::SlidingWindowMap: {
      const auto& map = builder.asSlidingWindowMap();
      out << "[o:" << map.offsetsDescriptor().offset()
          << ",l:" << map.lengthsDescriptor().offset() << "]SLIDINGWINDOWMAP\n";
      printType(out, map.keys(), indentation + 2, "keys");
      printType(out, map.values(), indentation + 2, "values");
      out << "\n";
      break;
    }
    case Kind::Row: {
      const auto& row = builder.asRow();
      out << "[" << row.nullsDescriptor().offset() << "]ROW\n";
      for (auto i = 0; i < row.childrenCount(); ++i) {
        printType(out, row.childAt(i), indentation + 2, row.nameAt(i));
      }
      out << "\n";
      break;
    }
    case Kind::FlatMap: {
      const auto& map = builder.asFlatMap();
      out << "[" << map.nullsDescriptor().offset() << "]FLATMAP\n";
      for (auto i = 0; i < map.childrenCount(); ++i) {
        printType(
            out,
            map.childAt(i),
            indentation + 2,
            // @lint-ignore CLANGTIDY facebook-hte-LocalUncheckedArrayBounds
            map.nameAt(i));
      }
      out << "\n";
      break;
    }
    case Kind::ArrayWithOffsets: {
      const auto& array = builder.asArrayWithOffsets();
      out << "[" << array.offsetsDescriptor().offset() << ","
          << array.lengthsDescriptor().offset() << "]OFFSETARRAY\n";
      printType(out, array.elements(), indentation + 2, "elements");
      out << "\n";
      break;
    }
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
} // namespace facebook::nimble
