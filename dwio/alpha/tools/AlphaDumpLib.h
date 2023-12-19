// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#pragma once
#include <optional>
#include <ostream>

#include "dwio/alpha/encodings/Encoding.h"
#include "velox/common/file/File.h"

namespace facebook::alpha::tools {

class AlphaDumpLib {
 public:
  AlphaDumpLib(std::ostream& ostream, const std::string& file);

  void emitInfo();
  void emitSchema(bool collapseFlatMap = true);
  void emitStripes(bool noHeader);
  void emitStreams(
      bool noHeader,
      bool flatmapKeys,
      std::optional<uint32_t> stripeId);
  void
  emitHistogram(bool topLevel, bool noHeader, std::optional<uint32_t> stripeId);
  void emitContent(uint32_t streamId, std::optional<uint32_t> stripeId);
  void emitBinary(
      std::function<std::unique_ptr<std::ostream>()> outputFactory,
      uint32_t streamId,
      uint32_t stripeId);
  void emitLayout(bool noHeader, bool compressed);

 private:
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::ReadFile> file_;
  std::ostream& ostream_;
};
} // namespace facebook::alpha::tools
