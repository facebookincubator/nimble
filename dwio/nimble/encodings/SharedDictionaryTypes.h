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
#pragma once

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include "dwio/nimble/common/DataTypeDispatch.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "dwio/nimble/encodings/views/EncodingViewFactory.h"
#include "velox/common/EnumDeclare.h"

namespace facebook::nimble {

/// Identifies where an alphabet is stored or resolved, and the namespace used
/// by SharedDictionaryConfig::dictionaryId. The scope controls both dictionary
/// lifetime and whether the reader loads an in-stripe auxiliary stream, the
/// file catalog, or an external catalog.
enum class SharedDictionaryScope : uint8_t {
  /// Alphabet is stored as an auxiliary stream in the current stripe, and its
  /// dictionary id is local to that stripe.
  Stripe = 0,
  /// Alphabet is stored once in the file shared dictionary catalog, and its
  /// dictionary id is local to that file. Value 1 is reserved so unknown
  /// historical values remain invalid.
  File = 2,
  /// Alphabet is resolved from an external catalog, and its dictionary id is
  /// interpreted by the configured resolver.
  External = 3,
};

VELOX_DECLARE_ENUM_NAME(SharedDictionaryScope);

inline std::string_view sharedDictionaryScopeString(
    SharedDictionaryScope scope) {
  return SharedDictionaryScopeName::toName(scope);
}

inline SharedDictionaryScope toSharedDictionaryScope(uint8_t value) {
  const auto scope = static_cast<SharedDictionaryScope>(value);
  switch (scope) {
    case SharedDictionaryScope::Stripe:
    case SharedDictionaryScope::File:
    case SharedDictionaryScope::External:
      return scope;
  }
  NIMBLE_UNSUPPORTED(
      "Unsupported shared dictionary scope {}.", static_cast<int>(value));
}

inline constexpr uint32_t kMaxSharedDictionaryEntryCount =
    std::numeric_limits<uint32_t>::max();

inline constexpr uint32_t kInvalidSharedDictionaryId =
    std::numeric_limits<uint32_t>::max();

/// Enables a shared dictionary for one logical scalar stream.
struct SharedDictionaryConfig {
  SharedDictionaryScope scope{SharedDictionaryScope::Stripe};

  /// Identifies the dictionary within its scope:
  /// - Stripe: auxiliary stream id in the current stripe that stores the
  ///   alphabet.
  /// - File: entry id in the file shared dictionary catalog.
  /// - External: id passed through to the external resolver.
  /// The sentinel default catches accidental use before the writer assigns an
  /// id in the selected scope.
  uint32_t dictionaryId{kInvalidSharedDictionaryId};

  /// Uses an externally determined alphabet from the configured resolver
  /// instead of growing it while encoding values. External-scope dictionaries
  /// always use a prebuilt alphabet; file-scope dictionaries use one when this
  /// is set. Forced alphabetEncoding is only allowed when the alphabet is
  /// prebuilt.
  bool usesPrebuiltAlphabet{false};

  /// Overrides the encoding used for the stored dictionary alphabet. Empty uses
  /// regular encoding selection.
  std::optional<EncodingType> alphabetEncoding;
};

/// Provides indexed access to one immutable shared dictionary alphabet.
class SharedDictionaryAlphabet {
 public:
  struct DecodedChunk {
    uint32_t begin{};
    uint32_t count{};
    const void* entries{};
    /// Keeps entries alive for this chunk.
    std::shared_ptr<const void> owner;
  };

  struct EncodedChunk {
    uint32_t begin{};
    /// Random-access view over the encoded alphabet chunk.
    std::shared_ptr<const EncodingView> view;
  };

  virtual ~SharedDictionaryAlphabet() = default;

  static DecodedChunk decodedChunk(
      uint32_t begin,
      uint32_t count,
      const void* entries,
      std::shared_ptr<const void> owner);

  static EncodedChunk encodedChunk(
      uint32_t begin,
      std::shared_ptr<const EncodingView> view);

  static std::shared_ptr<const SharedDictionaryAlphabet> createDecoded(
      DataType dataType,
      std::vector<DecodedChunk> chunks);

  static std::shared_ptr<const SharedDictionaryAlphabet> createEncoded(
      DataType dataType,
      std::vector<EncodedChunk> chunks);

  DataType dataType() const;

  uint32_t entryCount() const;

  template <typename T>
  typename TypeTraits<T>::physicalType physicalValueAt(uint32_t index) const {
    static_assert(
        !std::is_same_v<T, std::string>,
        "Use std::string_view for shared dictionary string alphabets.");
    NIMBLE_CHECK_EQ(
        dataType(),
        TypeTraits<T>::dataType,
        "Shared dictionary has unexpected type.");
    typename TypeTraits<T>::physicalType output;
    physicalValueAtImpl(index, &output);
    return output;
  }

  template <typename T>
  void materialize(
      std::span<const uint32_t> indices,
      typename TypeTraits<T>::physicalType* output) const {
    static_assert(
        !std::is_same_v<T, std::string>,
        "Use std::string_view for shared dictionary string alphabets.");
    NIMBLE_CHECK_EQ(
        dataType(),
        TypeTraits<T>::dataType,
        "Shared dictionary has unexpected type.");
    materializeImpl(indices, output);
  }

 protected:
  explicit SharedDictionaryAlphabet(DataType dataType);

  static uint32_t validateDecodedChunks(
      const std::vector<DecodedChunk>& chunks);

  static uint32_t validateEncodedChunks(
      DataType dataType,
      const std::vector<EncodedChunk>& chunks);

  void setEntryCount(uint32_t entryCount);

 private:
  virtual void physicalValueAtImpl(uint32_t index, void* output) const = 0;

  virtual void materializeImpl(std::span<const uint32_t> indices, void* output)
      const = 0;

  const DataType dataType_;
  uint32_t entryCount_{0};
};

/// Owns an immutable, decoded shared dictionary alphabet split into chunks.
class DecodedSharedDictionaryAlphabet final : public SharedDictionaryAlphabet {
 public:
  using Chunk = SharedDictionaryAlphabet::DecodedChunk;

  DecodedSharedDictionaryAlphabet(DataType dataType, std::vector<Chunk> chunks)
      : SharedDictionaryAlphabet{dataType}, chunks_{std::move(chunks)} {
    setEntryCount(validateDecodedChunks(chunks_));
  }

 private:
  template <typename T>
  void getPhysicalValueTyped(uint32_t index, void* output) const {
    const auto& chunk = chunkForIndex(index);
    *static_cast<typename TypeTraits<T>::physicalType*>(output) =
        static_cast<const typename TypeTraits<T>::physicalType*>(
            chunk.entries)[index - chunk.begin];
  }

  template <typename T>
  void materializeTyped(
      std::span<const uint32_t> indices,
      typename TypeTraits<T>::physicalType* output) const {
    if (chunks_.size() == 1) {
      const auto* entries =
          static_cast<const typename TypeTraits<T>::physicalType*>(
              chunks_[0].entries);
      for (size_t i = 0; i < indices.size(); ++i) {
        NIMBLE_CHECK_LT(
            indices[i],
            entryCount(),
            "Shared dictionary index exceeds alphabet size.");
        output[i] = entries[indices[i]];
      }
      return;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      const auto& chunk = chunkForIndex(indices[i]);
      output[i] = static_cast<const typename TypeTraits<T>::physicalType*>(
          chunk.entries)[indices[i] - chunk.begin];
    }
  }

  void physicalValueAtImpl(uint32_t index, void* output) const final {
    NIMBLE_RETURN_BY_DATA_TYPE_OR(
        dataType(),
        T,
        (getPhysicalValueTyped<T>(index, output), void()),
        NIMBLE_UNSUPPORTED(
            "{} is not supported by shared dictionary alphabets.", dataType()));
  }

  void materializeImpl(std::span<const uint32_t> indices, void* output)
      const final {
    NIMBLE_RETURN_BY_DATA_TYPE_OR(
        dataType(),
        T,
        (materializeTyped<T>(
             indices,
             static_cast<typename TypeTraits<T>::physicalType*>(output)),
         void()),
        NIMBLE_UNSUPPORTED(
            "{} is not supported by shared dictionary alphabets.", dataType()));
  }

  const Chunk& chunkForIndex(uint32_t index) const {
    NIMBLE_CHECK_LT(
        index, entryCount(), "Shared dictionary index exceeds alphabet size.");
    if (chunks_.size() == 1) {
      return chunks_.front();
    }
    const auto it = std::upper_bound(
        chunks_.begin(),
        chunks_.end(),
        index,
        [](uint32_t value, const Chunk& chunk) { return value < chunk.begin; });
    NIMBLE_CHECK(it != chunks_.begin());
    return *std::prev(it);
  }

  const std::vector<Chunk> chunks_;
};

/// Owns immutable, encoded shared dictionary alphabet chunks in memory.
class EncodedSharedDictionaryAlphabet final : public SharedDictionaryAlphabet {
 public:
  using Chunk = SharedDictionaryAlphabet::EncodedChunk;

  EncodedSharedDictionaryAlphabet(DataType dataType, std::vector<Chunk> chunks)
      : SharedDictionaryAlphabet{dataType}, chunks_{std::move(chunks)} {
    setEntryCount(validateEncodedChunks(dataType, chunks_));
  }

 private:
  void readPhysicalValue(uint32_t index, void* output) const {
    const auto chunkIndex = chunkIndexForIndex(index);
    const auto& chunk = chunks_[chunkIndex];
    chunk.view->readAt(index - chunk.begin, output);
  }

  template <typename T>
  void materializeTyped(
      std::span<const uint32_t> indices,
      typename TypeTraits<T>::physicalType* output) const {
    if (chunks_.size() == 1) {
      const auto& chunk = chunks_.front();
      for (size_t i = 0; i < indices.size(); ++i) {
        NIMBLE_CHECK_LT(
            indices[i],
            entryCount(),
            "Shared dictionary index exceeds alphabet size.");
        chunk.view->readAt(indices[i], output + i);
      }
      return;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
      const auto chunkIndex = chunkIndexForIndex(indices[i]);
      const auto& chunk = chunks_[chunkIndex];
      chunk.view->readAt(indices[i] - chunk.begin, output + i);
    }
  }

  void physicalValueAtImpl(uint32_t index, void* output) const final {
    readPhysicalValue(index, output);
  }

  void materializeImpl(std::span<const uint32_t> indices, void* output)
      const final {
    NIMBLE_RETURN_BY_DATA_TYPE_OR(
        dataType(),
        T,
        (materializeTyped<T>(
             indices,
             static_cast<typename TypeTraits<T>::physicalType*>(output)),
         void()),
        NIMBLE_UNSUPPORTED(
            "{} is not supported by shared dictionary alphabets.", dataType()));
  }

  size_t chunkIndexForIndex(uint32_t index) const {
    NIMBLE_CHECK_LT(
        index, entryCount(), "Shared dictionary index exceeds alphabet size.");
    if (chunks_.size() == 1) {
      return 0;
    }
    const auto it = std::upper_bound(
        chunks_.begin(),
        chunks_.end(),
        index,
        [](uint32_t value, const Chunk& chunk) { return value < chunk.begin; });
    NIMBLE_CHECK(it != chunks_.begin());
    return static_cast<size_t>(std::distance(chunks_.begin(), std::prev(it)));
  }

  const std::vector<Chunk> chunks_;
};

/// Resolves a dictionary ID within the current reader's decode context.
class SharedDictionaryResolver {
 public:
  virtual ~SharedDictionaryResolver() = default;

  virtual std::shared_ptr<const SharedDictionaryAlphabet> resolve(
      SharedDictionaryScope scope,
      uint32_t dictionaryId,
      DataType dataType) const = 0;
};

} // namespace facebook::nimble

template <>
struct fmt::formatter<facebook::nimble::SharedDictionaryScope>
    : fmt::formatter<std::string_view> {
  auto format(
      facebook::nimble::SharedDictionaryScope scope,
      format_context& ctx) const {
    return fmt::formatter<std::string_view>::format(
        facebook::nimble::SharedDictionaryScopeName::toName(scope), ctx);
  }
};
