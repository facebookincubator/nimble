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

#include "dwio/nimble/common/Types.h"
#include "folly/json/json.h"

namespace facebook::nimble {

///
/// Compression policy type definitions:
/// A compression policy defines which compression algorithm to apply on the
/// data (if any) and what parameters to use for this compression algorithm. In
/// addition, once compression is applied to the data, the compression policy
/// can decide if the compressed result is statisfactory or if it should be
/// discarded.
struct ZstdCompressionParameters {
  int16_t compressionLevel = 3;
};

/// An identifier for the meta internal compression policy.
class MetaInternalCompressionKey {
 public:
  MetaInternalCompressionKey() = default;

  MetaInternalCompressionKey(
      std::string ns,
      std::string tableName,
      std::string columnName)
      : ns_{std::move(ns)},
        tableName_{std::move(tableName)},
        columnName_{std::move(columnName)} {}

  const std::string& ns() const {
    return ns_;
  }

  const std::string& tableName() const {
    return tableName_;
  }

  const std::string& columnName() const {
    return columnName_;
  }

  std::string toString() const {
    folly::dynamic json = folly::dynamic::object("ns", ns_)(
        "tableName", tableName_)("columnName", columnName_);
    return folly::toJson(json);
  }

  static MetaInternalCompressionKey fromString(const std::string& str) {
    // will throw upon failure to parse or missing fields
    auto json = folly::parseJson(str);
    return MetaInternalCompressionKey{
        json["ns"].asString(),
        json["tableName"].asString(),
        json["columnName"].asString()};
  }

 private:
  std::string ns_;
  std::string tableName_;
  std::string columnName_;
};

struct MetaInternalCompressionParameters {
  int16_t compressionLevel = 0;
  int16_t decompressionLevel = 0;
  bool useVariableBitWidthCompressor = true;
  MetaInternalCompressionKey compressionKey;
};

struct CompressionParameters {
  ZstdCompressionParameters zstd{};
  MetaInternalCompressionParameters metaInternal{};
};

struct CompressionInformation {
  CompressionType compressionType{};
  CompressionParameters parameters{};
  uint64_t minCompressionSize = 0;
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

/// Default compression policy. Default behavior (if not compression policy is
/// provided) is to not compress.
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

} // namespace facebook::nimble
