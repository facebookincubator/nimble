// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <functional>
#include <memory>
#include <vector>

#include "dwio/nimble/velox/SchemaTypes.h"

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
namespace facebook::nimble {

class ScalarType;
class RowType;
class ArrayType;
class ArrayWithOffsetsType;
class MapType;
class FlatMapType;

class Type {
 public:
  Kind kind() const;

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
  explicit Type(Kind kind);

  virtual ~Type() = default;

 private:
  Type(const Type&) = delete;
  Type(Type&&) = delete;

  Kind kind_;
};

class ScalarType : public Type {
 public:
  explicit ScalarType(StreamDescriptor scalarDescriptor);

  const StreamDescriptor& scalarDescriptor() const;

 private:
  StreamDescriptor scalarDescriptor_;
};

class ArrayType : public Type {
 public:
  ArrayType(
      StreamDescriptor lengthsDescriptor,
      std::shared_ptr<const Type> elements);

  const StreamDescriptor& lengthsDescriptor() const;
  const std::shared_ptr<const Type>& elements() const;

 protected:
  StreamDescriptor lengthsDescriptor_;
  std::shared_ptr<const Type> elements_;
};

class MapType : public Type {
 public:
  MapType(
      StreamDescriptor lengthsDescriptor,
      std::shared_ptr<const Type> keys,
      std::shared_ptr<const Type> values);

  const StreamDescriptor& lengthsDescriptor() const;
  const std::shared_ptr<const Type>& keys() const;
  const std::shared_ptr<const Type>& values() const;

 protected:
  StreamDescriptor lengthsDescriptor_;
  std::shared_ptr<const Type> keys_;
  std::shared_ptr<const Type> values_;
};

class RowType : public Type {
 public:
  RowType(
      StreamDescriptor nullsDescriptor,
      std::vector<std::string> names,
      std::vector<std::shared_ptr<const Type>> children);

  const StreamDescriptor& nullsDescriptor() const;
  size_t childrenCount() const;
  const std::shared_ptr<const Type>& childAt(size_t index) const;
  const std::string& nameAt(size_t index) const;

 protected:
  StreamDescriptor nullsDescriptor_;
  std::vector<std::string> names_;
  std::vector<std::shared_ptr<const Type>> children_;
};

class FlatMapType : public Type {
 public:
  FlatMapType(
      StreamDescriptor nullsDescriptor,
      ScalarKind keyScalarKind,
      std::vector<std::string> names,
      std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors,
      std::vector<std::shared_ptr<const Type>> children);

  const StreamDescriptor& nullsDescriptor() const;
  const StreamDescriptor& inMapDescriptorAt(size_t index) const;
  ScalarKind keyScalarKind() const;
  size_t childrenCount() const;
  const std::shared_ptr<const Type>& childAt(size_t index) const;
  const std::string& nameAt(size_t index) const;

 private:
  StreamDescriptor nullsDescriptor_;
  ScalarKind keyScalarKind_;
  std::vector<std::string> names_;
  std::vector<std::unique_ptr<StreamDescriptor>> inMapDescriptors_;
  std::vector<std::shared_ptr<const Type>> children_;
};

class ArrayWithOffsetsType : public Type {
 public:
  ArrayWithOffsetsType(
      StreamDescriptor offsetsDescriptor,
      StreamDescriptor lengthsDescriptor,
      std::shared_ptr<const Type> elements);

  const StreamDescriptor& offsetsDescriptor() const;
  const StreamDescriptor& lengthsDescriptor() const;
  const std::shared_ptr<const Type>& elements() const;

 protected:
  StreamDescriptor offsetsDescriptor_;
  StreamDescriptor lengthsDescriptor_;
  std::shared_ptr<const Type> elements_;
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

} // namespace facebook::nimble
