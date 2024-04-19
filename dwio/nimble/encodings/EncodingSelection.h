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

#include <span>
#include <unordered_map>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingFactoryNew.h"
#include "dwio/nimble/encodings/EncodingIdentifier.h"
#include "dwio/nimble/encodings/Statistics.h"

namespace facebook::nimble {

// Overview:
// Nimble has many encoding to chose from. Each encoding yields different
// results on different shapes of data. In addition, Nimble's encodings are
// nested (e.g. encodings may contain other encodings, to encode sub streams of
// data). To be able to successfully encode an Nimble stream, we must build an
// encoding tree, optimized for the given data. Nimble is using a top-down
// (greedy) selection approach, where, by inspecting the top level data, we pick
// an encoding, and use it to encode the data. If this is a nested encoding, we
// then break down the data, and apply the same algorithm on each nested
// encoding. Once we pick an encoding for a specific level, we commit to this
// selection and never go back and change it.
//
// Encoding selection allows plugging in different policy implementations for
// selecting an encoding tree.
// Encoding selection provides access to a rich set of statistics on the data,
// which allows selection policies to apply smart decisions on which encoding
// fits this data best.
//
// To implement an encoding selection policy, one must inherit from
// EncodingSelectionPolicy<T> and implement two methods:
// 1. select(): Given the data and its statistics, return a selected encoding
// (and compression policy)
// 2. createImpl(): Create a nested encoding selection policy to be used for a
// nested encoding
//
// During the encoding process, the encode() method of the selected encoding is
// given access to an EncodingSelection class. This class allows the encode()
// method to access/reuse the pre-calculated statistics, and also allows
// continuing the selection process on nested data streams, by invoking the
// encodeNested() method on the nested stream.
// Internally, encodeNested() is creating a child encoding selection policy,
// suitable for the nested stream type, and triggers the selection process and
// encoding for the nested stream.

class EncodingSelectionPolicyBase;

//
// Compression policy type definitions:
// A compression policy defines which compression algorithm to apply on the data
// (if any) and what parameters to use for this compression algorithm.
// In addition, once compression is applied to the data, the compression policy
// can decide if the compressed result is statisfactory or if it should be
// discarded.
struct ZstdCompressionParameters {
  int16_t compressionLevel = 3;
};

struct ZstrongCompressionParameters {
  int16_t compressionLevel = 0;
  int16_t decompressionLevel = 0;
  bool useVariableBitWidthCompressor = true;
};

union CompressionParameters {
  ZstdCompressionParameters zstd;
  ZstrongCompressionParameters zstrong;
};

struct CompressionInformation {
  CompressionType compressionType;
  CompressionParameters parameters;
};

class CompressionPolicy {
 public:
  virtual CompressionInformation compression() const = 0;
  virtual bool shouldAccept(
      CompressionType /* compressionType */,
      uint64_t /* uncompressedSize */,
      uint64_t /* compressedSize */) const = 0;

  virtual ~CompressionPolicy() = default;
};

// Default compression policy. Default behavior (if not compression policy is
// provided) is to not compress.
class NoCompressionPolicy : public CompressionPolicy {
 public:
  CompressionInformation compression() const override {
    return {.compressionType = CompressionType::Uncompressed};
  }

  virtual bool shouldAccept(
      CompressionType /* compressionType */,
      uint64_t /* uncompressedSize */,
      uint64_t /* compressedSize */) const override {
    return false;
  }
};

// Type representing a selected encoding.
// This is the result type returned from the select() method of an encoding
// selection policy. Also provides access to the compression policies for nested
// data streams.
struct EncodingSelectionResult {
  EncodingType encodingType;
  std::function<std::unique_ptr<CompressionPolicy>()> compressionPolicyFactory =
      []() { return std::make_unique<NoCompressionPolicy>(); };
};

// The EncodingSelection class is passed in to the encode() method of each
// encoding. It provides access to the current encoding selection details, and
// allows triggering nested necodings of nested data streams.
template <typename T>
class EncodingSelection {
 public:
  EncodingSelection(
      EncodingSelectionResult selectionResult,
      Statistics<T>&& statistics,
      std::unique_ptr<EncodingSelectionPolicyBase>&& selectionPolicy)
      : selectionResult_{std::move(selectionResult)},
        statistics_{std::move(statistics)},
        selectionPolicy_{std::move(selectionPolicy)} {}

  EncodingType encodingType() const noexcept {
    return selectionResult_.encodingType;
  }

  const Statistics<T>& statistics() const noexcept {
    return statistics_;
  }

  std::unique_ptr<CompressionPolicy> compressionPolicy() const noexcept {
    auto policy = selectionResult_.compressionPolicyFactory();
    return policy;
  }

  // This method encodes a nested data stream. This includes
  // triggering a new encoding selection operation for the nested data and
  // recursively encoding internal nested stream (if further nested encodings
  // are selected).
  template <typename NestedT>
  std::string_view encodeNested(
      NestedEncodingIdentifier identifier,
      std::span<const NestedT> values,
      Buffer& buffer);

 private:
  EncodingSelectionResult selectionResult_;
  Statistics<T> statistics_;
  std::unique_ptr<EncodingSelectionPolicyBase> selectionPolicy_;
};

// HACK: When triggering encoding of a nested data stream, this data stream
// often has different data type than the parent (for example, when encoding
// string data, using dictionary encoding, the dictionary indices nested stream
// will most likely be integers, thus different from the parent string data
// type). This means, that we need to create a selection process, using a child
// encoding selection policy, but on a different template type.
// Since we don't have access to the nested type at compile time, we use this
// intermediate (non-templated) class, to allow constructing the correct
// templated encoding selection policy class, at runtime.
class EncodingSelectionPolicyBase {
 public:
  template <typename NestedT>
  std::unique_ptr<EncodingSelectionPolicyBase> create(
      EncodingType encodingType,
      NestedEncodingIdentifier identifier) {
    return createImpl(encodingType, identifier, TypeTraits<NestedT>::dataType);
  }

  virtual ~EncodingSelectionPolicyBase() = default;

 protected:
  // This method allows creating a child (nested) encoding selection policy
  // instance, with a different data type than the parent instance.
  // This method allows transferring state between parent and nested encoding
  // selection policy instances.
  // A child encoding selection policy instance will be created for every nested
  // stream of the parent encoding. The identifier passed in allows to
  // differentiate between the nested streams.
  virtual std::unique_ptr<EncodingSelectionPolicyBase> createImpl(
      EncodingType encodingType,
      NestedEncodingIdentifier identifier,
      DataType type) = 0;
};

// This is the main base class for defining an encoding selection policy.
template <typename T>
class EncodingSelectionPolicy : public EncodingSelectionPolicyBase {
  using physicalType = typename TypeTraits<T>::physicalType;

 public:
  // This is the main encoding selection method.
  // Given the data, and its statistics, the encoding selection policy should
  // return the selected encoding (and the matching compression policy).
  virtual EncodingSelectionResult select(
      std::span<const physicalType> values,
      const Statistics<physicalType>& statistics) = 0;

  // Same as the |select()| method above, but for nullable values.
  virtual EncodingSelectionResult selectNullable(
      std::span<const physicalType> values,
      std::span<const bool> nulls,
      const Statistics<physicalType>& statistics) = 0;

  virtual ~EncodingSelectionPolicy() = default;
};

namespace {
template <typename T, typename S>
std::unique_ptr<T> unique_ptr_cast(std::unique_ptr<S> src) {
  return std::unique_ptr<T>(static_cast<T*>(src.release()));
}
} // namespace

template <typename T>
template <typename NestedT>
std::string_view EncodingSelection<T>::encodeNested(
    NestedEncodingIdentifier identifier,
    std::span<const NestedT> values,
    Buffer& buffer) {
  // Create the nested encoding selection policy instance, and cast it to the
  // strongly templated type.
  auto nestedPolicy = std::unique_ptr<EncodingSelectionPolicy<NestedT>>(
      static_cast<EncodingSelectionPolicy<NestedT>*>(
          selectionPolicy_->template create<NestedT>(encodingType(), identifier)
              .release()));
  auto statistics = Statistics<NestedT>::create(values);
  auto selectionResult = nestedPolicy->select(values, statistics);
  return EncodingFactory::encode<NestedT>(
      EncodingSelection<NestedT>{
          std::move(selectionResult),
          std::move(statistics),
          std::move(nestedPolicy)},
      values,
      buffer);
}

} // namespace facebook::nimble
