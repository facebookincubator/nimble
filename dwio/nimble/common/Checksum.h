// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "dwio/nimble/common/Types.h"

#include <memory>
#include <string>

namespace facebook::nimble {

class Checksum {
 public:
  virtual ~Checksum() = default;
  virtual void update(std::string_view data) = 0;
  virtual uint64_t getChecksum(bool reset = false) = 0;
  virtual ChecksumType getType() const = 0;
};

class ChecksumFactory {
 public:
  static std::unique_ptr<Checksum> create(ChecksumType type);
};

} // namespace facebook::nimble
