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
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include "absl/container/flat_hash_map.h"
#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Vector.h"

#include "velox/common/memory/Memory.h"

namespace facebook::nimble {

inline constexpr uint64_t kMaxDictionaryEntryCount =
    std::numeric_limits<uint32_t>::max();

/// Maps dictionary values to their encoded shared dictionary indices.
template <typename T>
using DictionaryIndexType = absl::flat_hash_map<T, uint32_t>;

template <typename T>
class StreamingSharedDictionaryBuilder;

template <typename T>
class FixedSharedDictionaryBuilder;

template <typename T>
class ExternalSharedDictionaryBuilder;

/// Builds stable indices for one shared dictionary.
template <typename T>
class SharedDictionaryBuilder {
 public:
  /// Identifies how a writer-side shared dictionary builder owns or resolves
  /// its alphabet.
  enum class Kind : uint8_t {
    /// Builds an alphabet from input batches and commits accepted entries.
    Streaming = 0,
    /// Uses a prebuilt file-scope alphabet materialized by the writer.
    PrebuiltFile = 1,
    /// Uses an externally supplied value-to-index map without file alphabet
    /// data.
    External = 2,
  };

  virtual ~SharedDictionaryBuilder() = default;

  /// Mapping produced by prepare() before alphabet changes are committed.
  /// Writers use this to encode and estimate a candidate shared stream, then
  /// either commit it when shared dictionary wins or drop the mapping when
  /// direct encoding wins.
  class Mapping {
   public:
    explicit Mapping(velox::memory::MemoryPool* pool) : indices_{pool} {}

    /// Returns one dictionary index for each input value passed to prepare().
    std::span<const uint32_t> indices() const {
      return indices_;
    }

    /// Returns the number of new alphabet entries staged by prepare().
    uint32_t newEntryCount() const {
      return static_cast<uint32_t>(newEntries_.size());
    }

   private:
    friend class StreamingSharedDictionaryBuilder<T>;
    friend class FixedSharedDictionaryBuilder<T>;
    friend class ExternalSharedDictionaryBuilder<T>;

    // One dictionary index per input value.
    Vector<uint32_t> indices_;
    // Distinct values the streaming builder appends on commit().
    std::vector<T> newEntries_;
  };

  /// Maps values to dictionary indices and starts a pending mapping. New
  /// entries are staged in the returned mapping so that their indices are
  /// available for candidate encoding, but become durable only after commit().
  /// Returns nullopt when a fixed alphabet cannot map one of the values.
  std::optional<Mapping> prepare(std::span<const T> values) {
    checkState(State::Ready, "prepare");
    auto mapping = prepareImpl(values);
    if (mapping.has_value()) {
      state_ = State::Prepared;
    }
    return mapping;
  }

  virtual Kind kind() const = 0;

  /// Returns the builder kind as a stable debug string.
  std::string kindString() const {
    return kindString(kind());
  }

  /// Returns a stable debug string for a builder kind.
  static std::string kindString(Kind kind) {
    switch (kind) {
      case Kind::Streaming:
        return "Streaming";
      case Kind::PrebuiltFile:
        return "PrebuiltFile";
      case Kind::External:
        return "External";
    }
    return fmt::format("Unknown: {}", static_cast<int>(kind));
  }

  /// Makes staged alphabet growth durable after shared encoding is selected.
  void commit(const Mapping& mapping) {
    checkState(State::Prepared, "commit");
    commitImpl(mapping);
    state_ = State::Ready;
  }

  /// Clears stripe-owned alphabet state before values from the next stripe are
  /// prepared, dropping any pending mapping. Fixed file/external builders do
  /// not reset because their dictionaries are prebuilt.
  void reset() {
    checkState(State::Prepared, "reset");
    resetImpl();
    state_ = State::Ready;
  }

  /// Returns the alphabet entries when the writer owns and serializes them.
  virtual std::span<const T> alphabet() const = 0;

 protected:
  enum class State : uint8_t {
    Ready = 0,
    Prepared = 1,
  };

  explicit SharedDictionaryBuilder(velox::memory::MemoryPool* pool)
      : pool_{pool} {
    NIMBLE_CHECK_NOT_NULL(pool_, "Shared dictionary builder requires a pool.");
  }

  velox::memory::MemoryPool* pool() const {
    return pool_;
  }

  virtual std::optional<Mapping> prepareImpl(
      std::span<const T> values) const = 0;

  virtual void commitImpl(const Mapping& mapping) = 0;

  virtual void resetImpl() = 0;

 private:
  static std::string stateString(State state) {
    switch (state) {
      case State::Ready:
        return "Ready";
      case State::Prepared:
        return "Prepared";
    }
    return fmt::format("Unknown: {}", static_cast<int>(state));
  }

  void checkState(State expected, std::string_view operation) const {
    NIMBLE_CHECK(
        state_ == expected,
        "{} shared dictionary builder cannot {} while {}.",
        kindString(),
        operation,
        stateString(state_));
  }

  velox::memory::MemoryPool* const pool_;
  State state_{State::Ready};
};

/// Streaming dictionary builder for stripe/file-owned alphabets.
template <typename T>
class StreamingSharedDictionaryBuilder final
    : public SharedDictionaryBuilder<T> {
 public:
  using Mapping = typename SharedDictionaryBuilder<T>::Mapping;
  using Kind = typename SharedDictionaryBuilder<T>::Kind;

  explicit StreamingSharedDictionaryBuilder(velox::memory::MemoryPool* pool)
      : SharedDictionaryBuilder<T>{pool} {}

  Kind kind() const final {
    return Kind::Streaming;
  }

  std::span<const T> alphabet() const final {
    return {alphabet_.data(), alphabet_.size()};
  }

 protected:
  std::optional<Mapping> prepareImpl(std::span<const T> values) const final {
    Mapping mapping{this->pool()};
    mapping.indices_.reserve(values.size());
    mapping.newEntries_.reserve(values.size());

    DictionaryIndexType<T> pendingEntries;
    for (const auto& value : values) {
      const auto it = alphabetMapping_.find(value);
      if (it != alphabetMapping_.end()) {
        mapping.indices_.push_back(it->second);
        continue;
      }

      const auto pending = pendingEntries.find(value);
      if (pending != pendingEntries.end()) {
        mapping.indices_.push_back(pending->second);
        continue;
      }

      const auto index = alphabet().size() + mapping.newEntries_.size();
      NIMBLE_USER_CHECK_LT(
          index,
          kMaxDictionaryEntryCount,
          "Shared dictionary size exceeds maximum.");
      const auto dictionaryIndex = static_cast<uint32_t>(index);
      pendingEntries.emplace(value, dictionaryIndex);
      mapping.newEntries_.push_back(value);
      mapping.indices_.push_back(dictionaryIndex);
    }
    return mapping;
  }

  void commitImpl(const Mapping& mapping) final {
    alphabet_.reserve(alphabet_.size() + mapping.newEntries_.size());
    for (const auto& value : mapping.newEntries_) {
      NIMBLE_CHECK_LT(
          alphabet_.size(),
          kMaxDictionaryEntryCount,
          "Shared dictionary size exceeds maximum.");
      const auto index = static_cast<uint32_t>(alphabet_.size());
      const auto [_, inserted] = alphabetMapping_.emplace(value, index);
      NIMBLE_CHECK(inserted, "Shared dictionary mapping was committed twice.");
      alphabet_.push_back(value);
    }
  }

  void resetImpl() final {
    alphabet_.clear();
    alphabetMapping_.clear();
  }

 private:
  std::vector<T> alphabet_;
  DictionaryIndexType<T> alphabetMapping_;
};

/// Fixed dictionary builder backed by a prebuilt file-scope alphabet. The
/// caller keeps the alphabet alive and unchanged for the builder's lifetime;
/// the builder only stores a view so the prebuilt alphabet is not copied again.
template <typename T>
class FixedSharedDictionaryBuilder final : public SharedDictionaryBuilder<T> {
 public:
  using Mapping = typename SharedDictionaryBuilder<T>::Mapping;
  using Kind = typename SharedDictionaryBuilder<T>::Kind;

  FixedSharedDictionaryBuilder(
      velox::memory::MemoryPool* pool,
      std::span<const T> alphabet)
      : SharedDictionaryBuilder<T>{pool}, alphabet_{alphabet} {
    buildDictionaryMap();
  }

  Kind kind() const final {
    return Kind::PrebuiltFile;
  }

  std::span<const T> alphabet() const final {
    return alphabet_;
  }

 protected:
  std::optional<Mapping> prepareImpl(std::span<const T> values) const final {
    Mapping mapping{this->pool()};
    mapping.indices_.reserve(values.size());

    for (const auto& value : values) {
      const auto it = alphabetMapping_.find(value);
      if (it == alphabetMapping_.end()) {
        return std::nullopt;
      }
      mapping.indices_.push_back(it->second);
    }
    return mapping;
  }

  void commitImpl(const Mapping& mapping) final {
    NIMBLE_CHECK_EQ(
        mapping.newEntryCount(),
        0,
        "{} shared dictionary builder should not stage new entries.",
        this->kindString());
  }

  void resetImpl() final {
    NIMBLE_UNSUPPORTED(
        "{} shared dictionary builder does not support reset().",
        this->kindString());
  }

 private:
  void buildDictionaryMap() {
    NIMBLE_CHECK(
        alphabetMapping_.empty(),
        "Shared dictionary mapping should be empty before build.");
    NIMBLE_USER_CHECK_LE(
        alphabet_.size(),
        kMaxDictionaryEntryCount,
        "Prebuilt file shared dictionary size exceeds maximum.");
    alphabetMapping_.reserve(alphabet_.size());
    for (uint32_t i = 0; i < alphabet_.size(); ++i) {
      const auto [_, inserted] = alphabetMapping_.emplace(alphabet_[i], i);
      NIMBLE_USER_CHECK(
          inserted, "Prebuilt file shared dictionary has duplicate values.");
    }
  }

  const std::span<const T> alphabet_;
  DictionaryIndexType<T> alphabetMapping_;
};

/// Lookup-only builder for externally owned dictionaries. It maps values to
/// indices, but does not expose dictionary entries because external alphabets
/// are not serialized into the Nimble file.
template <typename T>
class ExternalSharedDictionaryBuilder final
    : public SharedDictionaryBuilder<T> {
 public:
  using Mapping = typename SharedDictionaryBuilder<T>::Mapping;
  using Kind = typename SharedDictionaryBuilder<T>::Kind;

  ExternalSharedDictionaryBuilder(
      DictionaryIndexType<T> dictionaryIndex,
      velox::memory::MemoryPool* pool)
      : SharedDictionaryBuilder<T>{pool},
        dictionaryIndex_{std::move(dictionaryIndex)} {}

  Kind kind() const final {
    return Kind::External;
  }

  std::span<const T> alphabet() const final {
    NIMBLE_UNSUPPORTED(
        "{} shared dictionary builder does not expose an alphabet.",
        this->kindString());
  }

 protected:
  std::optional<Mapping> prepareImpl(std::span<const T> values) const final {
    Mapping mapping{this->pool()};
    mapping.indices_.reserve(values.size());

    for (const auto& value : values) {
      const auto it = dictionaryIndex_.find(value);
      if (it == dictionaryIndex_.end()) {
        return std::nullopt;
      }
      mapping.indices_.push_back(it->second);
    }
    return mapping;
  }

  void commitImpl(const Mapping& mapping) final {
    NIMBLE_CHECK_EQ(
        mapping.newEntryCount(),
        0,
        "{} shared dictionary builder should not stage new entries.",
        this->kindString());
  }

  void resetImpl() final {
    NIMBLE_UNSUPPORTED(
        "{} shared dictionary builder does not support reset().",
        this->kindString());
  }

 private:
  const DictionaryIndexType<T> dictionaryIndex_;
};

} // namespace facebook::nimble
