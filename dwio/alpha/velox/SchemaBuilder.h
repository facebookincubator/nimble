// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "folly/container/F14Set.h"

// SchemaBuilder is used to construct a tablet schema.
// The table schema translates logical type tree to its underlying Alpha streams
// stored in the file. Each stream has a unique offset assigned to it, which is
// later used when storing stream related data in the tablet footer.
// The class supports primitive types (scalar types) and complex types: rows,
// arrays, maps and flat maps.
//
// This class serves two purposes:
// 1. Allow constructing parent schema nodes, before creating their children,
// and later allow attaching children to parent nodes. This is needed because
// writers usually construct their inner state top to bottom (recursively), so
// this allows for a much easier integration with writers.
// 2. When writing an Alpha file, not all inner streams are known ahead of time.
// This is because FlatMaps may append new streams when new data arrives, and
// new keys are encountered. This class abstracts the allocation of tablet
// offsets (indices to streams) and makes sure they stay consistent across
// stripes, even while the schema keeps changing.
// NOTE: The offsets allocated by this class are used only during write time,
// and are different than the final serialized offsets. During footer
// serialization, "build time offsets" (offsets managed by this class) are
// translated to "schema ordered" offsets (rely on the final flattened schema
// tree layout).
namespace facebook::alpha {

class SchemaBuilder;

class TypeBuilderContext {
 public:
  virtual ~TypeBuilderContext() = default;
};

class TypeBuilder : public virtual Type {
 public:
  void setContext(std::unique_ptr<TypeBuilderContext>&& context) const {
    _context = std::move(context);
  }

  const std::unique_ptr<TypeBuilderContext>& context() const {
    return _context;
  }

 protected:
  TypeBuilder(SchemaBuilder& schemaBuilder, offset_size offset, Kind kind);
  virtual ~TypeBuilder() = default;

  SchemaBuilder& schemaBuilder_;

 private:
  mutable std::unique_ptr<TypeBuilderContext> _context;

  friend class SchemaBuilder;
};

class ScalarTypeBuilder : public virtual TypeBuilder,
                          public virtual ScalarType {
 private:
  ScalarTypeBuilder(
      SchemaBuilder& schemaBuilder,
      offset_size offset,
      ScalarKind scalarKind);

  friend class SchemaBuilder;
};

class ArrayTypeBuilder : public virtual TypeBuilder, public virtual ArrayType {
 public:
  const TypeBuilder& elements() const;

  void setChildren(std::shared_ptr<TypeBuilder> elements);

 private:
  ArrayTypeBuilder(SchemaBuilder& schemaBuilder, offset_size offset);

  friend class SchemaBuilder;
};

class ArrayWithOffsetsTypeBuilder : public virtual TypeBuilder,
                                    public virtual ArrayWithOffsetsType {
 public:
  const ScalarTypeBuilder& offsets() const;
  const TypeBuilder& elements() const;

  void setChildren(
      std::shared_ptr<ScalarTypeBuilder> offsets,
      std::shared_ptr<TypeBuilder> elements);

 private:
  ArrayWithOffsetsTypeBuilder(SchemaBuilder& schemaBuilder, offset_size offset);

  friend class SchemaBuilder;
};

class RowTypeBuilder : public virtual TypeBuilder, public virtual RowType {
 public:
  const TypeBuilder& childAt(size_t index) const;

  void addChild(std::string name, std::shared_ptr<TypeBuilder> child);

 private:
  RowTypeBuilder(
      SchemaBuilder& schemaBuilder,
      offset_size offset,
      size_t childrenCount);

  friend class SchemaBuilder;
};

class MapTypeBuilder : public virtual TypeBuilder, public virtual MapType {
 public:
  const TypeBuilder& keys() const;
  const TypeBuilder& values() const;

  void setChildren(
      std::shared_ptr<TypeBuilder> keys,
      std::shared_ptr<TypeBuilder> values);

 private:
  MapTypeBuilder(SchemaBuilder& schemaBuilder, offset_size offset);

  friend class SchemaBuilder;
};

class FlatMapTypeBuilder : public virtual TypeBuilder,
                           public virtual FlatMapType {
 public:
  const ScalarTypeBuilder& inMapAt(size_t index) const;
  const TypeBuilder& childAt(size_t index) const;

  void addChild(
      std::string name,
      std::shared_ptr<ScalarTypeBuilder> inMap,
      std::shared_ptr<TypeBuilder> child);

 private:
  FlatMapTypeBuilder(
      SchemaBuilder& schemaBuilder,
      offset_size offset,
      ScalarKind keyScalarKind);

  friend class SchemaBuilder;
};

class SchemaBuilder {
 public:
  // Create a builder representing a scalar type with kind |scalarKind|.
  std::shared_ptr<ScalarTypeBuilder> createScalarTypeBuilder(
      ScalarKind scalarKind);

  // Create an array builder
  std::shared_ptr<ArrayTypeBuilder> createArrayTypeBuilder();

  // Create an array with offsets builder
  std::shared_ptr<ArrayWithOffsetsTypeBuilder>
  createArrayWithOffsetsTypeBuilder();

  // Create a row (struct) builder, with fixed number of children
  // |childrenCount|.
  std::shared_ptr<RowTypeBuilder> createRowTypeBuilder(size_t childrenCount);

  // Create a map builder.
  std::shared_ptr<MapTypeBuilder> createMapTypeBuilder();

  // Create a flat map builder. |keyScalarKind| captures the type of the map
  // key.
  std::shared_ptr<FlatMapTypeBuilder> createFlatMapTypeBuilder(
      ScalarKind keyScalarKind);

  // Retrieves all the nodes CURRENTLY known to the schema builder.
  // If more nodes are added to the schema builder later on, following calls to
  // this method will return existing and new nodes. It is guaranteed that the
  // allocated offsets for each node will stay the same across invocations.
  // NOTE: The schema must be in a valid state when calling this method. Valid
  // state means that the schema is a valid tree, with a single root node. This
  // means that all created builders were attached to their parents by calling
  // addChild/setChildren.
  std::vector<std::unique_ptr<const SchemaNode>> getSchemaNodes() const;

  offset_size nodeCount() const;

  const std::shared_ptr<const TypeBuilder>& getRoot() const;

 private:
  void registerChild(const std::shared_ptr<TypeBuilder>& type);

  void addNode(
      std::vector<std::unique_ptr<const SchemaNode>>& nodes,
      const TypeBuilder& type,
      std::optional<std::string> name = std::nullopt) const;

  // Schema builder is building a tree of types. As a tree, it should have a
  // single root. We use |roots_| to track that the callers don't forget to
  // attach every created type to a parent (and thus constructing a valid tree).
  // Every time a new type is constructed (and still wasn't attached to a
  // parent), we add it to |roots_|, to indicate that a new root (unattached
  // node) exists. Every time a node is attached to a parent, we remove it from
  // |roots_|, as it is no longer a free standing node.
  // We also use |roots_| to identify two other misuses:
  // 1. If a node was created using a different schema builder instance and then
  // was attached to a parent of a different schema builder.
  // 2. Attaching a node more than once to a parent.
  folly::F14FastSet<std::shared_ptr<const TypeBuilder>> roots_;
  offset_size currentOffset_ = 0;

  friend class ScalarTypeBuilder;
  friend class ArrayTypeBuilder;
  friend class ArrayWithOffsetsTypeBuilder;
  friend class RowTypeBuilder;
  friend class MapTypeBuilder;
  friend class FlatMapTypeBuilder;
  friend std::ostream& operator<<(
      std::ostream& out,
      const SchemaBuilder& schema);
};

std::ostream& operator<<(std::ostream& out, const SchemaBuilder& schema);

} // namespace facebook::alpha
