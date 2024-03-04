// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "dwio/alpha/common/MetricsLogger.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/velox/BufferGrowthPolicy.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/FlushPolicy.h"
#include "folly/container/F14Set.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/type/Type.h"

// Options used by Velox writer that affect the output file format

namespace facebook::alpha {

namespace detail {
std::unordered_map<std::string, std::string> defaultMetadata();
}

// NOTE: the object could be large when encodingOverrides are
// supplied. It's strongly advised to move instead of copying
// it.
struct VeloxWriterOptions {
  // Provides policy that controls stripe sizes and memory footprint.
  std::function<std::unique_ptr<FlushPolicy>()> flushPolicyFactory = []() {
    // Buffering 256MB data before encoding stripes.
    return std::make_unique<RawStripeSizeFlushPolicy>(256 << 20);
  };

  // Controls the speed/size tradeoff. Higher numbers choose faster
  // encodings, lower numbers choose high compression ratios. See FindBest
  // in the .cpp for more details.
  int32_t usecFactor = 100;

  // Columns that should be encoded as flat maps
  folly::F14FastSet<std::string> flatMapColumns;

  // Columns that should be encoded as dictionary arrays
  // NOTE: For each column, ALL the arrays inside this column will be encoded
  // using dictionary arrays. In the future we'll have finer control on
  // individual arrays within a column.
  folly::F14FastSet<std::string> dictionaryArrayColumns;

  // TODO: doesn't belong to writer layer and should be moved to converter
  // layer instead.
  // If > 0, conversion will stop converting stripes after at least
  // rowLimit rows have been processed.
  uint64_t rowLimit = 0;

  // The metric logger would come populated with access descriptor information,
  // application generated query id or specific sampling configs.
  std::shared_ptr<MetricsLogger> metricsLogger;

  // Optional feature reordering config.
  // The key for this config is a (top-level) flat map column ordinal and
  // the value is an ordered collection of feature ids. When provided, the
  // writer will make sure that flat map features are grouped together and
  // ordered based on this config.
  std::optional<std::vector<std::tuple<size_t, std::vector<int64_t>>>>
      featureReordering;

  // Property bag for storing user metadata in the file.
  std::unordered_map<std::string, std::string> metadata =
      detail::defaultMetadata();

  std::optional<EncodingLayoutTree> encodingLayoutTree;

  CompressionOptions compressionOptions;

  bool lowMemoryMode = false;
  bool leafOnlyMode = false;
  bool useEncodingSelectionPolicy = true;
  bool parallelEncoding = false;
  bool parallelWriting = false;

  // Optional writer parallel executor. When supplied, encoding/writing will be
  // parallelized by this executor, if the column type supports parallel
  // writing/decoding.
  std::shared_ptr<folly::Executor> parallelExecutor;

  // The factory function that produces the root encoding selection policy.
  // Encoding selection policy is the way to balance the tradeoffs of
  // different performance factors (at both read and write times). Heuristics
  // based, ML based or specialized policies can be specified.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = ManualEncodingSelectionPolicyFactory{}](
          DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  std::function<std::unique_ptr<InputBufferGrowthPolicy>()>
      inputGrowthPolicyFactory =
          []() -> std::unique_ptr<InputBufferGrowthPolicy> {
    return DefaultInputBufferGrowthPolicy::withDefaultRanges();
  };

  std::function<std::unique_ptr<velox::memory::MemoryReclaimer>()>
      reclaimerFactory = []() { return nullptr; };

  const velox::common::SpillConfig* spillConfig{nullptr};

  bool enableChunking = false;
};

} // namespace facebook::alpha
