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

#include "dwio/nimble/encodings/EncodingFactory.h"
#include "dwio/nimble/velox/selective/ReaderBase.h"
#include "dwio/nimble/velox/selective/RowSizeTracker.h"
#include "velox/dwio/common/FormatData.h"

namespace facebook::nimble {

class ChunkedDecoder;

class NimbleData : public velox::dwio::common::FormatData {
 public:
  NimbleData(
      const std::shared_ptr<const Type>& nimbleType,
      StripeStreams& streams,
      velox::memory::MemoryPool& memoryPool,
      ChunkedDecoder* inMapDecoder,
      std::function<std::unique_ptr<Encoding>(
          velox::memory::MemoryPool&,
          std::string_view,
          std::function<void*(uint32_t)>)> encodingFactory);

  /// Read internal node nulls. For leaf nodes, we only copy `incomingNulls' if
  /// it exists.
  void readNulls(
      velox::vector_size_t numValues,
      const uint64_t* incomingNulls,
      velox::BufferPtr& nulls,
      bool nullsOnly) override;

  uint64_t skipNulls(uint64_t numValues, bool nullsOnly) override;

  uint64_t skip(uint64_t numValues) final {
    return skipNulls(numValues, false);
  }

  bool hasNulls() const final {
    return nullsDecoder_ != nullptr;
  }

  velox::dwio::common::PositionProvider seekToRowGroup(
      int64_t /*index*/) final {
    NIMBLE_UNSUPPORTED();
  }

  void filterRowGroups(
      const velox::common::ScanSpec& /*scanSpec*/,
      uint64_t /*rowsPerRowGroup*/,
      const velox::dwio::common::StatsContext& /*writerContext*/,
      FilterRowGroupsResult& /*result*/) final {
    NIMBLE_UNSUPPORTED();
  }

  const Type& nimbleType() const {
    return *nimbleType_;
  }

  ChunkedDecoder makeScalarDecoder();

  // Decoders for Timestamps
  ChunkedDecoder makeMicrosDecoder();
  ChunkedDecoder makeNanosDecoder();

  const velox::BufferPtr& getPreloadedValues();

  const uint64_t* inMap() const {
    return inMapDecoder_ ? inMap_->as<uint64_t>() : nullptr;
  }

  const velox::BufferPtr& inMapBuffer() {
    return inMap_;
  }

  const ChunkedDecoder* nullsDecoder() const {
    return nullsDecoder_.get();
  }

  const ChunkedDecoder* inMapDecoder() const {
    return inMapDecoder_;
  }

  std::unique_ptr<ChunkedDecoder> makeLengthDecoder();

 private:
  std::unique_ptr<ChunkedDecoder> makeDecoder(
      const StreamDescriptor& descriptor,
      bool decodeValuesWithNulls);

  const std::shared_ptr<const Type> nimbleType_;
  StripeStreams* const streams_;
  velox::memory::MemoryPool* const pool_;
  ChunkedDecoder* const inMapDecoder_;
  std::unique_ptr<ChunkedDecoder> nullsDecoder_;
  velox::BufferPtr inMap_;
  std::function<std::unique_ptr<Encoding>(
      velox::memory::MemoryPool&,
      std::string_view,
      std::function<void*(uint32_t)>)>
      encodingFactory_;
};

class NimbleParams : public velox::dwio::common::FormatParams {
 public:
  NimbleParams(
      velox::memory::MemoryPool& pool,
      velox::dwio::common::ColumnReaderStatistics& stats,
      const std::shared_ptr<const Type>& nimbleType,
      StripeStreams& streams,
      RowSizeTracker* rowSizeTracker,
      std::function<std::unique_ptr<Encoding>(
          velox::memory::MemoryPool&,
          std::string_view,
          std::function<void*(uint32_t)>)> encodingFactory,
      bool preserveFlatMapsInMemory = false)
      : FormatParams(pool, stats),
        nimbleType_(nimbleType),
        streams_(&streams),
        rowSizeTracker_(rowSizeTracker),
        preserveFlatMapsInMemory_(preserveFlatMapsInMemory),
        encodingFactory_(encodingFactory) {}

  std::unique_ptr<velox::dwio::common::FormatData> toFormatData(
      const std::shared_ptr<const velox::dwio::common::TypeWithId>& /*type*/,
      const velox::common::ScanSpec& /*scanSpec*/) override;

  NimbleParams makeChildParams(const std::shared_ptr<const Type>& type) {
    return NimbleParams(
        pool(),
        runtimeStatistics(),
        type,
        *streams_,
        rowSizeTracker_,
        encodingFactory_,
        preserveFlatMapsInMemory_);
  }

  const std::shared_ptr<const Type>& nimbleType() const {
    return nimbleType_;
  }

  StripeStreams& streams() {
    return *streams_;
  }

  void setInMapDecoder(ChunkedDecoder* decoder) {
    inMapDecoder_ = decoder;
  }

  bool preserveFlatMapsInMemory() const {
    return preserveFlatMapsInMemory_;
  }

  RowSizeTracker* rowSizeTracker() const {
    return rowSizeTracker_;
  }

 private:
  const std::shared_ptr<const Type> nimbleType_;
  StripeStreams* const streams_{nullptr};
  RowSizeTracker* const rowSizeTracker_{nullptr};
  const bool preserveFlatMapsInMemory_{false};
  ChunkedDecoder* inMapDecoder_{nullptr};
  std::function<std::unique_ptr<Encoding>(
      velox::memory::MemoryPool&,
      std::string_view,
      std::function<void*(uint32_t)> stringBufferFactory)>
      encodingFactory_;
};

} // namespace facebook::nimble
