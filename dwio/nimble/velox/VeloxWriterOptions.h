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

#include "dwio/nimble/common/MetricsLogger.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/FlushPolicy.h"
#include "folly/container/F14Set.h"
#include "velox/common/base/SpillConfig.h"
#include "velox/type/Type.h"

// Options used by Velox writer that affect the output file format

namespace facebook::nimble {

namespace detail {
std::unordered_map<std::string, std::string> defaultMetadata();
}

// NOTE: the object could be large when encodingOverrides are
// supplied. It's strongly advised to move instead of copying
// it.
struct VeloxWriterOptions {
  // Property bag for storing user metadata in the file.
  std::unordered_map<std::string, std::string> metadata =
      detail::defaultMetadata();

  // Columns that should be encoded as flat maps
  folly::F14FastSet<std::string> flatMapColumns;

  // Columns that should be encoded as dictionary arrays
  // NOTE: For each column, ALL the arrays inside this column will be encoded
  // using dictionary arrays. In the future we'll have finer control on
  // individual arrays within a column.
  folly::F14FastSet<std::string> dictionaryArrayColumns;

  // Columns that should be encoded as dictionary map
  // NOTE: For each column, ALL the maps inside this column will be encoded
  // using dictionary maps.
  folly::F14FastSet<std::string> deduplicatedMapColumns;

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

  // Optional captured encoding layout tree.
  // Encoding layout tree is overlayed on the writer tree and the captured
  // encodings are attempted to be used first, before resolving to perform an
  // encoding selection.
  // Captured encodings can be used to speed up writes (as no encoding selection
  // is needed at runtime) and cal also provide better selected encodings, based
  // on history data.
  std::optional<EncodingLayoutTree> encodingLayoutTree;

  // Compression settings to be used when encoding and compressing data streams
  CompressionOptions compressionOptions;

  // In low-memory mode, the writer is trying to perform smaller (and more
  // precise) buffer allocations. This means that overall, the writer will
  // consume less memory, but will come with an additional cost, of more
  // reallocations and extra data copy.
  // TODO: This options should be removed and integrated into the
  // inputGrowthPolicyFactory option (e.g. allow the caller to set an
  // ExactGrowthPolicy, as defined here: dwio/nimble/velox/BufferGrowthPolicy.h)
  bool lowMemoryMode = false;

  // If present, metadata sections above this threshold size will be compressed.
  std::optional<uint32_t> metadataCompressionThreshold;

  // When flushing data streams into chunks, streams with raw data size smaller
  // than this threshold will not be flushed.
  // Note: this threshold is ignored when it is time to flush a stripe.
  uint64_t minStreamChunkRawSize = 1024;

  // The factory function that produces the root encoding selection policy.
  // Encoding selection policy is the way to balance the tradeoffs of
  // different performance factors (at both read and write times). Heuristics
  // based, ML based or specialized policies can be specified.
  EncodingSelectionPolicyFactory encodingSelectionPolicyFactory =
      [encodingFactory = ManualEncodingSelectionPolicyFactory{}](
          DataType dataType) -> std::unique_ptr<EncodingSelectionPolicyBase> {
    return encodingFactory.createPolicy(dataType);
  };

  // Provides policy that controls stripe sizes and memory footprint.
  std::function<std::unique_ptr<FlushPolicy>()> flushPolicyFactory = []() {
    // Buffering 256MB data before encoding stripes.
    return std::make_unique<RawStripeSizeFlushPolicy>(256 << 20);
  };

  // When the writer needs to buffer data, and internal buffers don't have
  // enough capacity, the writer is using this policy to claculate the the new
  // capacity for the vuffers.
  std::function<std::unique_ptr<InputBufferGrowthPolicy>()>
      inputGrowthPolicyFactory =
          []() -> std::unique_ptr<InputBufferGrowthPolicy> {
    return DefaultInputBufferGrowthPolicy::withDefaultRanges();
  };

  std::function<std::unique_ptr<velox::memory::MemoryReclaimer>()>
      reclaimerFactory = []() { return nullptr; };

  const velox::common::SpillConfig* spillConfig{nullptr};

  // If provided, internal encoding operations will happen in parallel using
  // this executor.
  std::shared_ptr<folly::Executor> encodingExecutor;

  bool enableChunking = false;

  // This callback will be visited on access to getDecodedVector in order to
  // monitor usage of decoded vectors vs. data that is passed-through in the
  // writer. Default function is no-op since its used for tests only.
  std::function<void(void)> vectorDecoderVisitor = []() {};
};

} // namespace facebook::nimble
