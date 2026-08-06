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

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include <fmt/core.h>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/common/EncodingType.h"
#include "velox/common/EnumDeclare.h"

namespace facebook::nimble {

/// Identifies where an alphabet is stored or resolved, and the dictionary id
/// namespace to use. The scope controls both dictionary lifetime and whether
/// the reader loads an in-stripe auxiliary stream, the file catalog, or an
/// external catalog.
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

inline constexpr uint32_t kMaxSharedDictionarySize =
    std::numeric_limits<uint32_t>::max();

inline constexpr uint32_t kInvalidSharedDictionaryId =
    std::numeric_limits<uint32_t>::max();

SharedDictionaryScope readSharedDictionaryScope(
    std::string_view data,
    const char*& pos);

uint32_t readSharedDictionaryId(std::string_view data, const char*& pos);

/// SharedDictionary-specific input selected for one encoded value stream.
struct SharedDictionaryEncodingInput {
  SharedDictionaryScope scope{SharedDictionaryScope::Stripe};
  uint32_t dictionaryId{kInvalidSharedDictionaryId};
  std::span<const uint32_t> indices;
};

/// Provides indexed access to one immutable shared dictionary alphabet.
class SharedDictionaryAlphabet {
 public:
  virtual ~SharedDictionaryAlphabet() = default;

  DataType dataType() const;

  uint32_t entryCount() const;

  /// Returns the original Nimble encoding type for the alphabet when known.
  std::optional<EncodingType> encodingType() const;

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

  void setEntryCount(uint32_t entryCount);

 private:
  virtual void physicalValueAtImpl(uint32_t index, void* output) const = 0;

  virtual void materializeImpl(std::span<const uint32_t> indices, void* output)
      const = 0;

  virtual std::optional<EncodingType> encodingTypeImpl() const = 0;

  const DataType dataType_;
  uint32_t entryCount_{0};
};

/// Resolves a dictionary ID within the current reader or writer context.
///
/// File-scope writers use the resolved alphabet as logical dictionary values
/// and serialize it through Nimble encoding for the file catalog. They do not
/// accept or reuse resolver-provided encoded bytes. External-scope dictionaries
/// are resolver-owned and are not serialized into the Nimble file.
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
