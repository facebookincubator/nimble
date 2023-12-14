// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/encodings/ConstantEncoding.h"
#include "dwio/alpha/encodings/DeltaEncoding.h"
#include "dwio/alpha/encodings/DictionaryEncoding.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/encodings/FixedBitWidthEncoding.h"
#include "dwio/alpha/encodings/MainlyConstantEncoding.h"
#include "dwio/alpha/encodings/NullableEncoding.h"
#include "dwio/alpha/encodings/RleEncoding.h"
#include "dwio/alpha/encodings/SparseBoolEncoding.h"
#include "dwio/alpha/encodings/TrivialEncoding.h"
#include "dwio/alpha/encodings/VarintEncoding.h"

namespace facebook::alpha::test {

template <typename E>
class Encoder {
  using T = typename E::cppDataType;

 private:
  class TestCompressPolicy : public alpha::CompressionPolicy {
   public:
    explicit TestCompressPolicy(CompressionType compressionType)
        : compressionType_{compressionType} {}

    alpha::CompressionInformation compression() const override {
      if (compressionType_ == CompressionType::Uncompressed) {
        return {.compressionType = CompressionType::Uncompressed};
      }

      if (compressionType_ == CompressionType::Zstd) {
        alpha::CompressionInformation information{
            .compressionType = alpha::CompressionType::Zstd};
        information.parameters.zstd.compressionLevel = 3;
        return information;
      }

      alpha::CompressionInformation information{
          .compressionType = alpha::CompressionType::Zstrong};
      information.parameters.zstrong.compressionLevel = 9;
      information.parameters.zstrong.decompressionLevel = 2;
      return information;
    }

    virtual bool shouldAccept(
        alpha::CompressionType /* compressionType */,
        uint64_t /* uncompressedSize */,
        uint64_t /* compressedSize */) const override {
      return true;
    }

   private:
    CompressionType compressionType_;
  };

  template <typename TInner>
  class TestTrivialEncodingSelectionPolicy
      : public alpha::EncodingSelectionPolicy<TInner> {
    using physicalType = typename alpha::TypeTraits<TInner>::physicalType;

   public:
    explicit TestTrivialEncodingSelectionPolicy(CompressionType compressionType)
        : compressionType_{compressionType} {}

    alpha::EncodingSelectionResult select(
        std::span<const physicalType> /* values */,
        const alpha::Statistics<physicalType>& /* statistics */) override {
      return {
          .encodingType = alpha::EncodingType::Trivial,
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

    std::unique_ptr<alpha::EncodingSelectionPolicyBase> createImpl(
        alpha::EncodingType /* encodingType */,
        alpha::NestedEncodingIdentifier /* identifier */,
        alpha::DataType type) {
      UNIQUE_PTR_FACTORY(
          type, TestTrivialEncodingSelectionPolicy, compressionType_);
    }

   private:
    CompressionType compressionType_;
  };

  template <typename Encoding>
  struct EncodingTypeTraits {};

  template <>
  struct EncodingTypeTraits<alpha::ConstantEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::Constant;
  };

  template <>
  struct EncodingTypeTraits<alpha::DictionaryEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::Dictionary;
  };

  template <>
  struct EncodingTypeTraits<alpha::FixedBitWidthEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::FixedBitWidth;
  };

  template <>
  struct EncodingTypeTraits<alpha::MainlyConstantEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::MainlyConstant;
  };

  template <>
  struct EncodingTypeTraits<alpha::RLEEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::RLE;
  };

  template <>
  struct EncodingTypeTraits<alpha::SparseBoolEncoding> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::SparseBool;
  };

  template <>
  struct EncodingTypeTraits<alpha::TrivialEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::Trivial;
  };

  template <>
  struct EncodingTypeTraits<alpha::VarintEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::Varint;
  };

  template <>
  struct EncodingTypeTraits<alpha::NullableEncoding<T>> {
    static constexpr inline alpha::EncodingType encodingType =
        alpha::EncodingType::Nullable;
  };

 public:
  static constexpr EncodingType encodingType() {
    return EncodingTypeTraits<E>::encodingType;
  }

  static std::string_view encode(
      alpha::Buffer& buffer,
      const alpha::Vector<T>& values,
      CompressionType compressionType = CompressionType::Uncompressed) {
    using physicalType = typename alpha::TypeTraits<T>::physicalType;

    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    alpha::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<E>::encodingType,
         .compressionPolicyFactory =
             [compressionType]() {
               return std::make_unique<TestCompressPolicy>(compressionType);
             }},
        alpha::Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(
            compressionType)};

    return E::encode(selection, physicalValues, buffer);
  }

  static std::string_view encodeNullable(
      alpha::Buffer& buffer,
      const alpha::Vector<T>& values,
      const alpha::Vector<bool>& nulls,
      alpha::CompressionType compressionType =
          alpha::CompressionType::Uncompressed) {
    using physicalType = typename alpha::TypeTraits<T>::physicalType;

    auto physicalValues = std::span<const physicalType>(
        reinterpret_cast<const physicalType*>(values.data()), values.size());
    alpha::EncodingSelection<physicalType> selection{
        {.encodingType = EncodingTypeTraits<E>::encodingType,
         .compressionPolicyFactory =
             [compressionType]() {
               return std::make_unique<TestCompressPolicy>(compressionType);
             }},
        alpha::Statistics<physicalType>::create(physicalValues),
        std::make_unique<TestTrivialEncodingSelectionPolicy<T>>(
            compressionType)};

    return E::encodeNullable(selection, physicalValues, nulls, buffer);
  }

  static std::unique_ptr<alpha::Encoding> createEncoding(
      alpha::Buffer& buffer,
      const alpha::Vector<T>& values,
      CompressionType compressionType = CompressionType::Uncompressed) {
    return std::make_unique<E>(
        buffer.getMemoryPool(), encode(buffer, values, compressionType));
  }

  static std::unique_ptr<alpha::Encoding> createNullableEncoding(
      alpha::Buffer& buffer,
      const alpha::Vector<T>& values,
      const alpha::Vector<bool>& nulls,
      CompressionType compressionType = CompressionType::Uncompressed) {
    return std::make_unique<E>(
        buffer.getMemoryPool(),
        encodeNullable(buffer, values, nulls, compressionType));
  }
};
} // namespace facebook::alpha::test
