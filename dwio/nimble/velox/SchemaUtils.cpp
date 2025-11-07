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
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/velox/SchemaBuilder.h"

namespace facebook::nimble {

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

velox::TypePtr convertToVeloxType(const Type& type) {
  switch (type.kind()) {
    case Kind::Scalar: {
      return convertToVeloxScalarType(
          type.asScalar().scalarDescriptor().scalarKind());
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
  return SchemaReader::getSchema(builder.getSchemaNodes());
}

} // namespace facebook::nimble
