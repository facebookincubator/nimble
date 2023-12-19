// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include <gtest/gtest.h>

#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaReader.h"
#include "dwio/alpha/velox/SchemaTypes.h"

namespace facebook::alpha::test {

void verifySchemaNodes(
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& nodes,
    std::vector<alpha::SchemaNode> expected);

void compareSchema(
    const std::vector<std::unique_ptr<const alpha::SchemaNode>>& nodes,
    const std::shared_ptr<const alpha::Type>& root);

std::shared_ptr<alpha::RowTypeBuilder> row(
    alpha::SchemaBuilder& builder,
    std::vector<std::pair<std::string, std::shared_ptr<alpha::TypeBuilder>>>
        children);

std::shared_ptr<alpha::ArrayTypeBuilder> array(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::TypeBuilder> elements);

std::shared_ptr<alpha::ArrayWithOffsetsTypeBuilder> arrayWithOffsets(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::ScalarTypeBuilder> offsets,
    std::shared_ptr<alpha::TypeBuilder> elements);

std::shared_ptr<alpha::MapTypeBuilder> map(
    alpha::SchemaBuilder& builder,
    std::shared_ptr<alpha::TypeBuilder> keys,
    std::shared_ptr<alpha::TypeBuilder> values);

class FlatMapChildAdder {
 public:
  void addChild(std::string name) {
    ALPHA_CHECK(schemaBuilder_, "Flat map child adder is not intialized.");
    typeBuilder_->addChild(
        name,
        schemaBuilder_->createScalarTypeBuilder(alpha::ScalarKind::Bool),
        valueFactory_(*schemaBuilder_));
  }

 private:
  void initialize(
      alpha::SchemaBuilder& schemaBuilder,
      alpha::FlatMapTypeBuilder& typeBuilder,
      std::function<std::shared_ptr<alpha::TypeBuilder>(alpha::SchemaBuilder&)>
          valueFactory) {
    schemaBuilder_ = &schemaBuilder;
    typeBuilder_ = &typeBuilder;
    valueFactory_ = std::move(valueFactory);
  }

  alpha::SchemaBuilder* schemaBuilder_;
  alpha::FlatMapTypeBuilder* typeBuilder_;
  std::function<std::shared_ptr<alpha::TypeBuilder>(alpha::SchemaBuilder&)>
      valueFactory_;

  friend std::shared_ptr<alpha::FlatMapTypeBuilder> flatMap(
      alpha::SchemaBuilder& builder,
      alpha::ScalarKind keyScalarKind,
      std::function<std::shared_ptr<alpha::TypeBuilder>(alpha::SchemaBuilder&)>
          valueFactory,
      FlatMapChildAdder& childAdder);
};

std::shared_ptr<alpha::FlatMapTypeBuilder> flatMap(
    alpha::SchemaBuilder& builder,
    alpha::ScalarKind keyScalarKind,
    std::function<std::shared_ptr<alpha::TypeBuilder>(alpha::SchemaBuilder&)>
        valueFactory,
    FlatMapChildAdder& childAdder);

void schema(
    alpha::SchemaBuilder& builder,
    std::function<void(alpha::SchemaBuilder&)> factory);

#define SCHEMA(schemaBuilder, types) \
  facebook::alpha::test::schema(     \
      schemaBuilder, [&](alpha::SchemaBuilder& builder) { types; })

#define ROW(...) facebook::alpha::test::row(builder, __VA_ARGS__)
#define TINYINT() builder.createScalarTypeBuilder(alpha::ScalarKind::Int8)
#define SMALLINT() builder.createScalarTypeBuilder(alpha::ScalarKind::Int16)
#define INTEGER() builder.createScalarTypeBuilder(alpha::ScalarKind::Int32)
#define UNSIGNEDINTEGER() \
  builder.createScalarTypeBuilder(alpha::ScalarKind::UInt32)
#define BIGINT() builder.createScalarTypeBuilder(alpha::ScalarKind::Int64)
#define REAL() builder.createScalarTypeBuilder(alpha::ScalarKind::Float)
#define DOUBLE() builder.createScalarTypeBuilder(alpha::ScalarKind::Double)
#define BOOLEAN() builder.createScalarTypeBuilder(alpha::ScalarKind::Bool)
#define STRING() builder.createScalarTypeBuilder(alpha::ScalarKind::String)
#define BINARY() builder.createScalarTypeBuilder(alpha::ScalarKind::Binary)
#define ARRAY(elements) facebook::alpha::test::array(builder, elements)
#define OFFSETARRAY(offsets, elements) \
  facebook::alpha::test::arrayWithOffsets(builder, offsets, elements)
#define MAP(keys, values) facebook::alpha::test::map(builder, keys, values)
#define FLATMAP(keyKind, values, adder)                      \
  facebook::alpha::test::flatMap(                            \
      builder,                                               \
      alpha::ScalarKind::keyKind,                            \
      [&](alpha::SchemaBuilder& builder) { return values; }, \
      adder)

} // namespace facebook::alpha::test
