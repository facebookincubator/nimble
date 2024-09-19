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
#pragma once

#include "dwio/nimble/encodings/ConstantEncoding.h"
#include "dwio/nimble/encodings/DictionaryEncoding.h"
#include "dwio/nimble/encodings/Encoding.h"
#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/encodings/FixedBitWidthEncoding.h"
#include "dwio/nimble/encodings/MainlyConstantEncoding.h"
#include "dwio/nimble/encodings/NullableEncoding.h"
#include "dwio/nimble/encodings/RleEncoding.h"
#include "dwio/nimble/encodings/SparseBoolEncoding.h"
#include "dwio/nimble/encodings/TrivialEncoding.h"
#include "dwio/nimble/encodings/VarintEncoding.h"

namespace facebook::nimble::test {

template <typename E>
class Encoder {
  using T = typename E::cppDataType;

 private:
  class TestCompressPolicy : public nimble::CompressionPolicy {
   public:
    explicit TestCompressPolicy(CompressionType compressionType)
        : compressionType_{compressionType} {}

    nimble::CompressionInformation compression() const override {
      if (compressionType_ == CompressionType::Uncompressed) {
        return {.compressionType = CompressionType::Uncompressed};
      }

      if (compressionType_ == CompressionType::Zstd) {
        nimble::CompressionInformation information{
            .compressionType = nimble::CompressionType::Zstd};
        information.parameters.zstd.compressionLevel = 3;
        return information;
      }

      nimble::CompressionInformation information{
          .compressionType = nimble::CompressionType::MetaInternal};
      information.parameters.metaInternal.compressionLevel = 9;
      information.parameters.metaInternal.decompressionLevel = 2;
      return information;
    }

    virtual bool shouldAccept(
        nimble::CompressionType /* compressionType */,
        uint64_t /* uncompressedSize */,
        uint64_t /* compressedSize */) const override {
      return true;
    }

   private:
    CompressionType compressionType_;
  };

  template <typename TInner>
  class TestTrivialEncodingSelectionPolicy
      : public nimble::EncodingSelectionPolicy<TInner> {
    using physicalType = typename nimble::TypeTraits<TInner>::physicalType;

   public:
    explicit TestTrivialEncodingSelectionPolicy(CompressionType compressionType)
        : compressionType_{compressionType} {}

    nimble::EncodingSelectionResult select(
        std::span<const physicalType> /* values */,
        const nimble::Statistics<physicalType>& /* statistics */) override {
      return {
          .encodingType = nimble::EncodingType::Trivial,
          .compressionPolicyFactory = [this]() {
            return std::make_unique<TestCompressPolicy>(compressionType_);
          }};
    }

    EncodingSelectionResult selectNullable(
        std::span<const physicalType> /* values */,
        std::span<const bool> /* nulls */,
        const Statistics<physicalType>& /* statistics */) override {
      return {
          .encodingType = EncodingType::Nullable,
      };
    }

    std::unique_ptr<nimble::EncodingSelectionPolicyBase> createImpl(
        nimble::EncodingType /* encodingType */,
        nimble::NestedEncodingIdentifier /* identifier */,
        nimble::DataType type) override {
      UNIQUE_PTR_FACTORY(
          type, TestTrivialEncodingSelectionPolicy, compressionType_);
    }

   private:
    CompressionType compressionType_;
  };

  template <typename Encoding>
  struct EncodingTypeTraits {};

  template <>
  struct EncodingTypeTraits<nimble::ConstantEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::Constant;
  };

  template <>
  struct EncodingTypeTraits<nimble::DictionaryEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::Dictionary;
  };

  template <>
  struct EncodingTypeTraits<nimble::FixedBitWidthEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::FixedBitWidth;
  };

  template <>
  struct EncodingTypeTraits<nimble::MainlyConstantEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::MainlyConstant;
  };

  template <>
  struct EncodingTypeTraits<nimble::RLEEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::RLE;
  };

  template <>
  struct EncodingTypeTraits<nimble::SparseBoolEncoding> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::SparseBool;
  };

  template <>
  struct EncodingTypeTraits<nimble::TrivialEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::Trivial;
  };

  template <>
  struct EncodingTypeTraits<nimble::VarintEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::Varint;
  };

  template <>
  struct EncodingTypeTraits<nimble::NullableEncoding<T>> {
    static constexpr inline nimble::EncodingType encodingType =
        nimble::EncodingType::Nullable;
  };

 public:
  static constexpr EncodingType encodingType() {
    return EncodingTypeTraits<E>::encodingType;
  }

  static std::string_view encode(
      nimble::Buffer& buffer,
      const nimble::Vector<T>& values,
      CompressionType compressionType = CompressionType::Uncompressed) {
    using physicalType = typename nimble::TypeTraits<T>::physicalType;

    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    nimble::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<E>::encodingType,
         .compressionPolicyFactory =
             [compressionType]() {
               return std::make_unique<TestCompressPolicy>(compressionType);
             }},
        nimble::Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(
            compressionType)};

    return E::encode(selection, physicalValues, buffer);
  }

  static std::string_view encodeNullable(
      nimble::Buffer& buffer,
      const nimble::Vector<T>& values,
      const nimble::Vector<bool>& nulls,
      nimble::CompressionType compressionType =
          nimble::CompressionType::Uncompressed) {
    using physicalType = typename nimble::TypeTraits<T>::physicalType;

    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    nimble::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<E>::encodingType,
         .compressionPolicyFactory =
             [compressionType]() {
               return std::make_unique<TestCompressPolicy>(compressionType);
             }},
        nimble::Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(
            compressionType)};

    return NullableEncoding<typename E::cppDataType>::encodeNullable(
        selection, physicalValues, nulls, buffer);
  }

  static std::unique_ptr<nimble::Encoding> createEncoding(
      nimble::Buffer& buffer,
      const nimble::Vector<T>& values,
      CompressionType compressionType = CompressionType::Uncompressed) {
    return std::make_unique<E>(
        buffer.getMemoryPool(), encode(buffer, values, compressionType));
  }

  static std::unique_ptr<nimble::Encoding> createNullableEncoding(
      nimble::Buffer& buffer,
      const nimble::Vector<T>& values,
      const nimble::Vector<bool>& nulls,
      CompressionType compressionType = CompressionType::Uncompressed) {
    return std::make_unique<E>(
        buffer.getMemoryPool(),
        encodeNullable(buffer, values, nulls, compressionType));
  }
};

class TestUtils {
 public:
  static uint64_t getRawDataSize(
      velox::memory::MemoryPool& memoryPool,
      std::string_view encodingStr);
};
} // namespace facebook::nimble::test
