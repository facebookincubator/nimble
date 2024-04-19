// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <gtest/gtest.h>

#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaReader.h"
#include "dwio/nimble/velox/SchemaTypes.h"

namespace facebook::nimble::test {

void verifySchemaNodes(
    const std::vector<std::unique_ptr<const nimble::SchemaNode>>& nodes,
    std::vector<nimble::SchemaNode> expected);

void compareSchema(
    const std::vector<std::unique_ptr<const nimble::SchemaNode>>& nodes,
    const std::shared_ptr<const nimble::Type>& root);

std::shared_ptr<nimble::RowTypeBuilder> row(
    nimble::SchemaBuilder& builder,
    std::vector<std::pair<std::string, std::shared_ptr<nimble::TypeBuilder>>>
        children);

std::shared_ptr<nimble::ArrayTypeBuilder> array(
    nimble::SchemaBuilder& builder,
    std::shared_ptr<nimble::TypeBuilder> elements);

std::shared_ptr<nimble::ArrayWithOffsetsTypeBuilder> arrayWithOffsets(
    nimble::SchemaBuilder& builder,
    std::shared_ptr<nimble::TypeBuilder> elements);

std::shared_ptr<nimble::MapTypeBuilder> map(
    nimble::SchemaBuilder& builder,
    std::shared_ptr<nimble::TypeBuilder> keys,
    std::shared_ptr<nimble::TypeBuilder> values);

class FlatMapChildAdder {
 public:
  void addChild(std::string name) {
    NIMBLE_CHECK(schemaBuilder_, "Flat map child adder is not intialized.");
    typeBuilder_->addChild(name, valueFactory_(*schemaBuilder_));
  }

 private:
  void initialize(
      nimble::SchemaBuilder& schemaBuilder,
      nimble::FlatMapTypeBuilder& typeBuilder,
      std::function<std::shared_ptr<nimble::TypeBuilder>(
          nimble::SchemaBuilder&)> valueFactory) {
    schemaBuilder_ = &schemaBuilder;
    typeBuilder_ = &typeBuilder;
    valueFactory_ = std::move(valueFactory);
  }

  nimble::SchemaBuilder* schemaBuilder_;
  nimble::FlatMapTypeBuilder* typeBuilder_;
  std::function<std::shared_ptr<nimble::TypeBuilder>(nimble::SchemaBuilder&)>
      valueFactory_;

  friend std::shared_ptr<nimble::FlatMapTypeBuilder> flatMap(
      nimble::SchemaBuilder& builder,
      nimble::ScalarKind keyScalarKind,
      std::function<std::shared_ptr<nimble::TypeBuilder>(
          nimble::SchemaBuilder&)> valueFactory,
      FlatMapChildAdder& childAdder);
};

std::shared_ptr<nimble::FlatMapTypeBuilder> flatMap(
    nimble::SchemaBuilder& builder,
    nimble::ScalarKind keyScalarKind,
    std::function<std::shared_ptr<nimble::TypeBuilder>(nimble::SchemaBuilder&)>
        valueFactory,
    FlatMapChildAdder& childAdder);

void schema(
    nimble::SchemaBuilder& builder,
    std::function<void(nimble::SchemaBuilder&)> factory);

#define SCHEMA(schemaBuilder, types) \
  facebook::nimble::test::schema(    \
      schemaBuilder, [&](nimble::SchemaBuilder& builder) { types; })

#define ROW(...) facebook::nimble::test::row(builder, __VA_ARGS__)
#define TINYINT() builder.createScalarTypeBuilder(nimble::ScalarKind::Int8)
#define SMALLINT() builder.createScalarTypeBuilder(nimble::ScalarKind::Int16)
#define INTEGER() builder.createScalarTypeBuilder(nimble::ScalarKind::Int32)
#define BIGINT() builder.createScalarTypeBuilder(nimble::ScalarKind::Int64)
#define REAL() builder.createScalarTypeBuilder(nimble::ScalarKind::Float)
#define DOUBLE() builder.createScalarTypeBuilder(nimble::ScalarKind::Double)
#define BOOLEAN() builder.createScalarTypeBuilder(nimble::ScalarKind::Bool)
#define STRING() builder.createScalarTypeBuilder(nimble::ScalarKind::String)
#define BINARY() builder.createScalarTypeBuilder(nimble::ScalarKind::Binary)
#define ARRAY(elements) facebook::nimble::test::array(builder, elements)
#define OFFSETARRAY(elements) \
  facebook::nimble::test::arrayWithOffsets(builder, elements)
#define MAP(keys, values) facebook::nimble::test::map(builder, keys, values)
#define FLATMAP(keyKind, values, adder)                       \
  facebook::nimble::test::flatMap(                            \
      builder,                                                \
      nimble::ScalarKind::keyKind,                            \
      [&](nimble::SchemaBuilder& builder) { return values; }, \
      adder)

} // namespace facebook::nimble::test
