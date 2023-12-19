// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "dwio/alpha/velox/SchemaTypes.h"

// Schema reader provides a strongly typed, tree like, reader friendly facade on
// top of a flat tablet schema.
// Schema stored in a tablet footer is a DFS representation of a type tree.
// Reconstructing a tree out of this flat representation require deep knowledge
// of how each complex type (rows, arrays, maps and flat maps) are laid out.
// Using this class, it is possible to reconstruct an easy to use tree
// representation on the logical type tree.
// The main usage of this class is to allow efficient retrieval of subsets of
// the schema tree (used in column and feature projection), as it can
// efficiently skip full sections of the schema tree (which cannot be done
// efficiently using a flat schema representation, that relies on schema node
// order for decoding).
namespace facebook::alpha {

class ScalarType;
class RowType;
class ArrayType;
class ArrayWithOffsetsType;
class MapType;
class FlatMapType;

class Type {
 public:
  Kind kind() const;
  offset_size offset() const;

  bool isScalar() const;
  bool isRow() const;
  bool isArray() const;
  bool isArrayWithOffsets() const;
  bool isMap() const;
  bool isFlatMap() const;

  const ScalarType& asScalar() const;
  const RowType& asRow() const;
  const ArrayType& asArray() const;
  const ArrayWithOffsetsType& asArrayWithOffsets() const;
  const MapType& asMap() const;
  const FlatMapType& asFlatMap() const;

 protected:
  Type(offset_size offset, Kind kind);

  virtual ~Type() = default;

 private:
  offset_size offset_;
  Kind kind_;
};

class ScalarType : public virtual Type {
 public:
  ScalarType(offset_size offset, ScalarKind scalarKind);

  ScalarKind scalarKind() const;

 private:
  ScalarKind scalarKind_;
};

class ArrayType : public virtual Type {
 public:
  ArrayType(offset_size offset, std::shared_ptr<const Type> elements);

  const std::shared_ptr<const Type>& elements() const;

 protected:
  std::shared_ptr<const Type> elements_;
};

class ArrayWithOffsetsType : public virtual Type {
 public:
  ArrayWithOffsetsType(
      offset_size offset,
      std::shared_ptr<const ScalarType> offsets,
      std::shared_ptr<const Type> elements);

  const std::shared_ptr<const ScalarType>& offsets() const;
  const std::shared_ptr<const Type>& elements() const;

 protected:
  std::shared_ptr<const ScalarType> offsets_;
  std::shared_ptr<const Type> elements_;
};

class RowType : public virtual Type {
 public:
  RowType(
      offset_size offset,
      std::vector<std::string> names,
      std::vector<std::shared_ptr<const Type>> children);

  size_t childrenCount() const;
  const std::shared_ptr<const Type>& childAt(size_t index) const;
  const std::string& nameAt(size_t index) const;

 protected:
  std::vector<std::string> names_;
  std::vector<std::shared_ptr<const Type>> children_;
};

class MapType : public virtual Type {
 public:
  MapType(
      offset_size offset,
      std::shared_ptr<const Type> keys,
      std::shared_ptr<const Type> values);

  const std::shared_ptr<const Type>& keys() const;
  const std::shared_ptr<const Type>& values() const;

 protected:
  std::shared_ptr<const Type> keys_;
  std::shared_ptr<const Type> values_;
};

class FlatMapType : public virtual Type {
 public:
  FlatMapType(
      offset_size offset,
      ScalarKind keyScalarKind,
      std::vector<std::string> names,
      std::vector<std::shared_ptr<const ScalarType>> inMaps,
      std::vector<std::shared_ptr<const Type>> children);

  ScalarKind keyScalarKind() const;
  size_t childrenCount() const;
  const std::shared_ptr<const ScalarType>& inMapAt(size_t index) const;
  const std::shared_ptr<const Type>& childAt(size_t index) const;
  const std::string& nameAt(size_t index) const;

 private:
  ScalarKind keyScalarKind_;

 protected:
  std::vector<std::string> names_;
  std::vector<std::shared_ptr<const ScalarType>> inMaps_;
  std::vector<std::shared_ptr<const Type>> children_;
};

class SchemaReader {
 public:
  // Construct type tree from an ordered list of schema nodes.
  // If the schema nodes are ordered incorrectly the behavior is undefined.
  static std::shared_ptr<const Type> getSchema(
      const std::vector<std::unique_ptr<const SchemaNode>>& nodes);

  struct NodeInfo {
    std::string_view name;
    const Type* parentType;
    size_t placeInSibling = 0;
  };

  static void traverseSchema(
      const std::shared_ptr<const Type>& root,
      std::function<void(
          uint32_t /* level */,
          const Type& /* streamType */,
          const NodeInfo& /* traverseInfo */)> visitor);
};

std::ostream& operator<<(
    std::ostream& out,
    const std::shared_ptr<const Type>& root);

} // namespace facebook::alpha
