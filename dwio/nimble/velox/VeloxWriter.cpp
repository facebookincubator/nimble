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
#include "dwio/nimble/velox/VeloxWriter.h"

#include <memory>
#include <unordered_map>

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/selection/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/Constants.h"
#include "dwio/nimble/velox/BufferGrowthPolicy.h"
#include "dwio/nimble/velox/ChunkedStreamWriter.h"
#include "dwio/nimble/velox/EncodingLayoutTree.h"
#include "dwio/nimble/velox/FieldWriter.h"
#include "dwio/nimble/velox/FlushPolicy.h"
#include "dwio/nimble/velox/LayoutPlanner.h"
#include "dwio/nimble/velox/MetadataGenerated.h"
#include "dwio/nimble/velox/RawSizeUtils.h"
#include "dwio/nimble/velox/SchemaBuilder.h"
#include "dwio/nimble/velox/SchemaSerialization.h"
#include "dwio/nimble/velox/SchemaTypes.h"
#include "dwio/nimble/velox/StatsGenerated.h"
#include "dwio/nimble/velox/StreamChunker.h"
#include "dwio/nimble/velox/stats/VectorizedStatistics.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/common/time/Timer.h"
#include "velox/type/Type.h"

namespace facebook::nimble {

using velox::dwio::common::TypeWithId;

namespace detail {

class WriterContext : public FieldWriterContext {
 public:
  WriterContext(
      velox::memory::MemoryPool& memoryPool,
      VeloxWriterOptions options)
      : FieldWriterContext{memoryPool, options.reclaimerFactory(), options.vectorDecoderVisitor},
        options_{std::move(options)},
        logger_{
            this->options_.metricsLogger == nullptr
                ? std::make_shared<MetricsLogger>()
                : this->options_.metricsLogger} {
    inputBufferGrowthPolicy_ = this->options_.lowMemoryMode
        ? std::make_unique<ExactGrowthPolicy>()
        : this->options_.inputGrowthPolicyFactory();
    stringBufferGrowthPolicy_ = this->options_.lowMemoryMode
        ? std::make_unique<ExactGrowthPolicy>()
        : this->options_.stringBufferGrowthPolicyFactory();
    ignoreTopLevelNulls_ = options_.ignoreTopLevelNulls;
    disableSharedStringBuffers_ = options_.disableSharedStringBuffers;
    if (this->options_.encodingExecutor &&
        this->options_.maxEncodeParallelism > 0) {
      setParallelEncoding(
          this->options_.encodingExecutor.get(),
          this->options_.maxEncodeParallelism,
          this->options_.minStreamsPerEncodeUnit);
    }
  }

  const VeloxWriterOptions& options() const {
    return options_;
  }

  velox::CpuWallTiming& encodingTiming() {
    return encodingTiming_;
  }

  const velox::CpuWallTiming& encodingTiming() const {
    return encodingTiming_;
  }

  velox::CpuWallTiming& writeTiming() {
    return writeTiming_;
  }

  const velox::CpuWallTiming& writeTiming() const {
    return writeTiming_;
  }

  velox::CpuWallTiming& ingestionTiming() {
    return ingestionTiming_;
  }

  const velox::CpuWallTiming& ingestionTiming() const {
    return ingestionTiming_;
  }

  velox::CpuWallTiming& encodingSelectionTiming() {
    return encodingSelectionTiming_;
  }

  const velox::CpuWallTiming& encodingSelectionTiming() const {
    return encodingSelectionTiming_;
  }

  std::shared_ptr<MetricsLogger>& logger() {
    return logger_;
  }

  const std::shared_ptr<MetricsLogger>& logger() const {
    return logger_;
  }

  uint64_t memoryUsed() const {
    return memoryUsed_;
  }

  void setMemoryUsed(uint64_t value) {
    memoryUsed_ = value;
  }

  void updateMemoryUsed(uint64_t value) {
    memoryUsed_ += value;
    NIMBLE_CHECK_GE(memoryUsed_, 0);
  }

  uint64_t bytesWritten() const {
    return bytesWritten_;
  }

  void setBytesWritten(uint64_t writtenBytes) {
    bytesWritten_ = writtenBytes;
  }

  uint64_t rowsInFile() const {
    return rowsInFile_;
  }

  void updateRowsInFile(uint64_t numRows) {
    rowsInFile_ += numRows;
  }

  uint32_t rowsInStripe() const {
    return rowsInStripe_;
  }

  void updateRowsInStripe(uint32_t numRows) {
    rowsInStripe_ += numRows;
  }

  uint64_t stripeEncodedPhysicalSize() const {
    return stripeEncodedPhysicalSize_;
  }

  void updateStripeEncodedPhysicalSize(uint64_t updateBytes) {
    stripeEncodedPhysicalSize_ += updateBytes;
  }

  uint64_t stripeEncodedLogicalSize() const {
    return stripeEncodedLogicalSize_;
  }

  void updateStripeEncodedLogicalSize(uint64_t bytes) {
    stripeEncodedLogicalSize_ += bytes;
  }

  uint64_t fileRawSize() const {
    return fileRawBytes_;
  }

  void updateFileRawSize(uint64_t bytes) {
    fileRawBytes_ += bytes;
  }

  std::vector<uint64_t>& rowsPerStripe() {
    return rowsPerStripe_;
  }

  const std::vector<uint64_t>& rowsPerStripe() const {
    return rowsPerStripe_;
  }

  void nextStripe() {
    rowsPerStripe_.push_back(rowsInStripe_);
    memoryUsed_ = 0;
    rowsInStripe_ = 0;
    stripeEncodedPhysicalSize_ = 0;
    stripeEncodedLogicalSize_ = 0;
    ++stripeIndex_;
  }

  size_t getStripeIndex() const {
    return stripeIndex_;
  }

 private:
  const VeloxWriterOptions options_;
  velox::CpuWallTiming encodingTiming_;
  velox::CpuWallTiming writeTiming_;
  velox::CpuWallTiming ingestionTiming_;
  velox::CpuWallTiming encodingSelectionTiming_;
  std::shared_ptr<MetricsLogger> logger_;
  uint64_t memoryUsed_{0};
  uint64_t bytesWritten_{0};
  uint64_t rowsInFile_{0};
  uint32_t rowsInStripe_{0};
  uint64_t stripeEncodedPhysicalSize_{0};
  uint64_t stripeEncodedLogicalSize_{0};
  uint64_t fileRawBytes_{0};
  std::vector<uint64_t> rowsPerStripe_;
  size_t stripeIndex_{0};
};

} // namespace detail

namespace {

velox::RuntimeMetric toRuntimeMetric(const std::vector<uint64_t>& values) {
  velox::RuntimeMetric metric;
  for (auto value : values) {
    metric.addValue(value);
  }
  return metric;
}

constexpr uint32_t kInitialSchemaSectionSize = 1 << 20; // 1MB

// When writing null streams, we write the nulls as data, and the stream itself
// is non-nullable. This adapter class is how we expose the nulls as values.
class NullsAsDataStreamData : public StreamData {
 public:
  explicit NullsAsDataStreamData(StreamData& streamData)
      : StreamData(streamData.descriptor()), streamData_{&streamData} {
    streamData_->materialize();
  }

  inline uint32_t rowCount() const override {
    return streamData_->rowCount();
  }

  inline std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(streamData_->nonNulls().data()),
        streamData_->nonNulls().size()};
  }

  inline std::span<const bool> nonNulls() const override {
    return {};
  }

  inline bool hasNulls() const override {
    return false;
  }

  inline bool empty() const override {
    return streamData_->empty();
  }

  inline uint64_t memoryUsed() const override {
    return streamData_->memoryUsed();
  }

  inline void reset() override {
    streamData_->reset();
  }

 private:
  StreamData* const streamData_;
};

class WriterStreamContext : public StreamContext {
 public:
  bool isNullStream() const {
    return isNullStream_;
  }

  void setIsNullStream(bool value) {
    isNullStream_ = value;
  }

  bool isInMapStream() const {
    return isInMapStream_;
  }

  void setIsInMapStream(bool value) {
    isInMapStream_ = value;
  }

  const EncodingLayout* encoding() const {
    return encoding_;
  }

  void setEncoding(const EncodingLayout* value) {
    encoding_ = value;
  }

 private:
  bool isNullStream_{false};
  bool isInMapStream_{false};
  const EncodingLayout* encoding_{nullptr};
};

class FlatmapEncodingLayoutContext : public TypeBuilderContext {
 public:
  explicit FlatmapEncodingLayoutContext(
      folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
          keyEncodings)
      : keyEncodings{std::move(keyEncodings)} {}

  const folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
      keyEncodings;
};

template <typename T>
std::string_view encode(
    std::optional<EncodingLayout> encodingLayout,
    detail::WriterContext& context,
    Buffer& buffer,
    const StreamData& streamData) {
  NIMBLE_DCHECK_EQ(
      streamData.data().size() % sizeof(T),
      0,
      "Unexpected size {}",
      streamData.data().size());
  std::span<const T> data{
      reinterpret_cast<const T*>(streamData.data().data()),
      streamData.data().size() / sizeof(T)};

  std::unique_ptr<EncodingSelectionPolicy<T>> policy;
  if (encodingLayout.has_value()) {
    policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        std::move(encodingLayout.value()),
        context.options().compressionOptions,
        context.options().encodingSelectionPolicyCreator);

  } else {
    policy = std::unique_ptr<EncodingSelectionPolicy<T>>(
        static_cast<EncodingSelectionPolicy<T>*>(
            context.options()
                .encodingSelectionPolicyCreator(TypeTraits<T>::dataType)
                .release()));
  }

  const auto encodingOptions = context.options().buildEncodingOptions();

  if (streamData.hasNulls()) {
    std::span<const bool> notNulls = streamData.nonNulls();
    return EncodingFactory::encodeNullable(
        std::move(policy), data, notNulls, buffer, encodingOptions);
  } else {
    return EncodingFactory::encode(
        std::move(policy), data, buffer, encodingOptions);
  }
}

template <typename T>
std::string_view encodeStreamTyped(
    detail::WriterContext& context,
    Buffer& buffer,
    const StreamData& streamData) {
  const auto* streamContext =
      streamData.descriptor().context<WriterStreamContext>();

  std::optional<EncodingLayout> encodingLayout;
  if (streamContext && streamContext->encoding()) {
    encodingLayout.emplace(*streamContext->encoding());
  }

  try {
    return encode<T>(std::move(encodingLayout), context, buffer, streamData);
  } catch (const NimbleUserError& e) {
    if (e.errorCode() != error_code::IncompatibleEncoding ||
        !encodingLayout.has_value()) {
      throw;
    }

    // Incompatible captured encoding. Try again without a captured encoding.
    return encode<T>(std::nullopt, context, buffer, streamData);
  }
}

std::string_view encodeStreamData(
    detail::WriterContext& context,
    Buffer& buffer,
    const StreamData& streamData) {
  auto scalarKind = streamData.descriptor().scalarKind();
  switch (scalarKind) {
    case ScalarKind::Bool:
      return encodeStreamTyped<bool>(context, buffer, streamData);
    case ScalarKind::Int8:
      return encodeStreamTyped<int8_t>(context, buffer, streamData);
    case ScalarKind::Int16:
      return encodeStreamTyped<int16_t>(context, buffer, streamData);
    case ScalarKind::UInt16:
      return encodeStreamTyped<uint16_t>(context, buffer, streamData);
    case ScalarKind::Int32:
      return encodeStreamTyped<int32_t>(context, buffer, streamData);
    case ScalarKind::UInt32:
      return encodeStreamTyped<uint32_t>(context, buffer, streamData);
    case ScalarKind::Int64:
      return encodeStreamTyped<int64_t>(context, buffer, streamData);
    case ScalarKind::Float:
      return encodeStreamTyped<float>(context, buffer, streamData);
    case ScalarKind::Double:
      return encodeStreamTyped<double>(context, buffer, streamData);
    case ScalarKind::String:
    case ScalarKind::Binary:
      return encodeStreamTyped<std::string_view>(context, buffer, streamData);
    default:
      NIMBLE_UNREACHABLE("Unsupported scalar kind {}", toString(scalarKind));
  }
}

template <typename Set>
void findNodeIds(
    const velox::dwio::common::TypeWithId& typeWithId,
    Set& output,
    const std::function<bool(const velox::dwio::common::TypeWithId&)>&
        predicate) {
  if (predicate(typeWithId)) {
    output.insert(typeWithId.id());
  }

  for (const auto& child : typeWithId.getChildren()) {
    findNodeIds(*child, output, predicate);
  }
}

WriterStreamContext& getStreamContext(
    const StreamDescriptorBuilder& descriptor) {
  auto* context = descriptor.context<WriterStreamContext>();
  if (context != nullptr) {
    return *context;
  }
  descriptor.setContext(std::make_unique<WriterStreamContext>());
  return *descriptor.context<WriterStreamContext>();
}

// NOTE: This is a temporary method. We currently use TypeWithId to assing
// node ids to each node in the schema tree. Using TypeWithId is not ideal, as
// it is not very intuitive to users to figure out node ids. In the future,
// we'll design a new way to identify nodes in the tree (probably based on
// multi-level ordinals). But until then, we keep using a "simple" (yet
// restrictive) external configuration and perform internal conversion to node
// ids. Once the new language is ready, we'll switch to using it instead and
// this translation logic will be removed.

// Resolves a dotted-path key (e.g. "user.name") against a TypeWithId tree by
// walking RowType children. Returns nullptr if any segment fails to match a
// row child. The empty path resolves to `root`. Paths that traverse a
// non-Row parent return nullptr; callers currently emit flat top-level /
// nested-struct keys only. Returning nullptr is the intentional "unresolved
// path" result; folly is avoided per the velox coding guideline, so the
// nullable-return check is suppressed instead of using FOLLY_NULLABLE.
//
// NOLINTNEXTLINE(facebook-hte-NullableReturn)
const velox::dwio::common::TypeWithId* resolveDottedPath(
    const velox::dwio::common::TypeWithId& root,
    std::string_view path) {
  if (path.empty()) {
    return &root;
  }
  const velox::dwio::common::TypeWithId* current = &root;
  size_t start = 0;
  while (start <= path.size()) {
    auto dot = path.find('.', start);
    auto end = (dot == std::string_view::npos) ? path.size() : dot;
    auto segment = path.substr(start, end - start);
    if (current->type()->kind() != velox::TypeKind::ROW) {
      return nullptr;
    }
    std::shared_ptr<const velox::dwio::common::TypeWithId> child;
    try {
      child = current->childByName(std::string(segment));
    } catch (const velox::VeloxUserError&) {
      return nullptr;
    }
    if (child == nullptr) {
      return nullptr;
    }
    current = child.get();
    if (dot == std::string_view::npos) {
      break;
    }
    start = dot + 1;
  }
  return current;
}

std::unique_ptr<FieldWriter> createRootFieldWriter(
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type,
    detail::WriterContext& context) {
  if (!context.options().flatMapColumns.empty()) {
    context.reserveFlatMapNodes(context.options().flatMapColumns.size());
    for (const auto& [columnName, keys] : context.options().flatMapColumns) {
      auto nodeId = type->childByName(columnName)->id();
      context.addFlatMapNodeId(nodeId, keys);
    }
  }

  if (!context.options().dictionaryArrayColumns.empty()) {
    context.clearAndReserveDictionaryArrayNodeIds(
        context.options().dictionaryArrayColumns.size());
    for (const auto& column : context.options().dictionaryArrayColumns) {
      findNodeIds(
          *type->childByName(column),
          context.dictionaryArrayNodeIds(),
          [](const velox::dwio::common::TypeWithId& type) {
            return type.type()->kind() == velox::TypeKind::ARRAY;
          });
    }
  }

  if (!context.options().deduplicatedMapColumns.empty()) {
    context.clearAndReserveDeduplicatedMapNodeIds(
        context.options().deduplicatedMapColumns.size());
    for (const auto& column : context.options().deduplicatedMapColumns) {
      findNodeIds(
          *type->childByName(column),
          context.deduplicatedMapNodeIds(),
          [](const TypeWithId& type) {
            return type.type()->kind() == velox::TypeKind::MAP;
          });
    }
  }

  if (context.options().enableStatsCollection) {
    context.initStatsCollectors(type);
  }

  // Translate dotted-path column-name keys to TypeWithId::id keys so the
  // typeAddedHandler can look them up in O(1) as each TypeBuilder is
  // constructed. Paths that fail to resolve are silently dropped (see
  // VeloxWriterOptions::attributesByColumn doc).
  std::unordered_map<uint32_t, std::vector<std::pair<std::string, std::string>>>
      attributesByNodeId;
  if (!context.options().attributesByColumn.empty()) {
    attributesByNodeId.reserve(context.options().attributesByColumn.size());
    for (const auto& [path, attributes] :
         context.options().attributesByColumn) {
      const auto* resolved = resolveDottedPath(*type, path);
      if (resolved != nullptr) {
        attributesByNodeId.emplace(resolved->id(), attributes);
      }
    }
  }

  return FieldWriter::create(
      context,
      type,
      [&, nodeAttributes = std::move(attributesByNodeId)](
          TypeBuilder& type, uint32_t nodeId) {
        if (type.kind() == Kind::Row) {
          getStreamContext(type.asRow().nullsDescriptor())
              .setIsNullStream(true);
        } else if (type.kind() == Kind::FlatMap) {
          getStreamContext(type.asFlatMap().nullsDescriptor())
              .setIsNullStream(true);
        }
        auto it = nodeAttributes.find(nodeId);
        if (it != nodeAttributes.end()) {
          type.setAttributes(it->second);
        }
      });
}

void initializeEncodingLayouts(
    const TypeBuilder& typeBuilder,
    const EncodingLayoutTree& encodingLayoutTree) {
  {
#define SET_STREAM_CONTEXT(builder, descriptor, identifier)             \
  if (auto* encodingLayout = encodingLayoutTree.encodingLayout(         \
          EncodingLayoutTree::StreamIdentifiers::identifier)) {         \
    getStreamContext(builder.descriptor()).setEncoding(encodingLayout); \
  }

    if (typeBuilder.kind() == Kind::FlatMap) {
      if (encodingLayoutTree.schemaKind() == Kind::Map) {
        // Schema evolution - If a map is converted to flatmap, we should not
        // fail, but also not try to replay captured encodings.
        return;
      }
      NIMBLE_CHECK_EQ(
          encodingLayoutTree.schemaKind(),
          Kind::FlatMap,
          "Incompatible encoding layout node. Expecting flatmap node.");
      folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
          keyEncodings;
      keyEncodings.reserve(encodingLayoutTree.childrenCount());
      for (auto i = 0; i < encodingLayoutTree.childrenCount(); ++i) {
        auto& child = encodingLayoutTree.child(i);
        keyEncodings.emplace(child.name(), child);
      }
      const auto& mapBuilder = typeBuilder.asFlatMap();
      mapBuilder.setContext(
          std::make_unique<FlatmapEncodingLayoutContext>(
              std::move(keyEncodings)));

      SET_STREAM_CONTEXT(mapBuilder, nullsDescriptor, FlatMap::NullsStream);
      return;
    }

    switch (typeBuilder.kind()) {
      case Kind::Scalar: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::Scalar,
            "Incompatible encoding layout node. Expecting scalar node.");
        SET_STREAM_CONTEXT(
            typeBuilder.asScalar(), scalarDescriptor, Scalar::ScalarStream);
        break;
      }
      case Kind::TimestampMicroNano: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::TimestampMicroNano,
            "Incompatible encoding layout node. Expecting TimestampMicroNano node but got {}.",
            toString(encodingLayoutTree.schemaKind()));
        auto& timestampMicroNanoBuilder = typeBuilder.asTimestampMicroNano();
        SET_STREAM_CONTEXT(
            timestampMicroNanoBuilder,
            microsDescriptor,
            TimestampMicroNano::MicrosStream);
        SET_STREAM_CONTEXT(
            timestampMicroNanoBuilder,
            nanosDescriptor,
            TimestampMicroNano::NanosStream);
        break;
      }
      case Kind::Row: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::Row,
            "Incompatible encoding layout node. Expecting row node.");
        auto& rowBuilder = typeBuilder.asRow();
        SET_STREAM_CONTEXT(rowBuilder, nullsDescriptor, Row::NullsStream);
        for (auto i = 0; i < rowBuilder.childrenCount() &&
             i < encodingLayoutTree.childrenCount();
             ++i) {
          initializeEncodingLayouts(
              rowBuilder.childAt(i), encodingLayoutTree.child(i));
        }
        break;
      }
      case Kind::Array: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::Array,
            "Incompatible encoding layout node. Expecting array node.");
        auto& arrayBuilder = typeBuilder.asArray();
        SET_STREAM_CONTEXT(
            arrayBuilder, lengthsDescriptor, Array::LengthsStream);
        if (encodingLayoutTree.childrenCount() > 0) {
          NIMBLE_CHECK(
              encodingLayoutTree.childrenCount() == 1,
              "Invalid encoding layout tree. Array node should have exactly one child.");
          initializeEncodingLayouts(
              arrayBuilder.elements(), encodingLayoutTree.child(0));
        }
        break;
      }
      case Kind::Map: {
        if (encodingLayoutTree.schemaKind() == Kind::FlatMap) {
          // Schema evolution - If a flatmap is converted to map, we should
          // not fail, but also not try to replay captured encodings.
          return;
        }
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::Map,
            "Incompatible encoding layout node. Expecting map node.");
        auto& mapBuilder = typeBuilder.asMap();

        SET_STREAM_CONTEXT(mapBuilder, lengthsDescriptor, Map::LengthsStream);
        if (encodingLayoutTree.childrenCount() > 0) {
          NIMBLE_CHECK_EQ(
              encodingLayoutTree.childrenCount(),
              2,
              "Invalid encoding layout tree. Map node should have exactly two children.");
          initializeEncodingLayouts(
              mapBuilder.keys(), encodingLayoutTree.child(0));
          initializeEncodingLayouts(
              mapBuilder.values(), encodingLayoutTree.child(1));
        }

        break;
      }
      case Kind::SlidingWindowMap: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::SlidingWindowMap,
            "Incompatible encoding layout node. Expecting SlidingWindowMap node.");
        auto& mapBuilder = typeBuilder.asSlidingWindowMap();
        SET_STREAM_CONTEXT(
            mapBuilder, offsetsDescriptor, SlidingWindowMap::OffsetsStream);
        SET_STREAM_CONTEXT(
            mapBuilder, lengthsDescriptor, SlidingWindowMap::LengthsStream);
        if (encodingLayoutTree.childrenCount() > 0) {
          NIMBLE_CHECK(
              encodingLayoutTree.childrenCount() == 2,
              "Invalid encoding layout tree. SlidingWindowMap node should have exactly two children.");
          initializeEncodingLayouts(
              mapBuilder.keys(), encodingLayoutTree.child(0));
          initializeEncodingLayouts(
              mapBuilder.values(), encodingLayoutTree.child(1));
        }

        break;
      }
      case Kind::ArrayWithOffsets: {
        NIMBLE_CHECK_EQ(
            encodingLayoutTree.schemaKind(),
            Kind::ArrayWithOffsets,
            "Incompatible encoding layout node. Expecting offset array node.");
        auto& arrayBuilder = typeBuilder.asArrayWithOffsets();
        SET_STREAM_CONTEXT(
            arrayBuilder, offsetsDescriptor, ArrayWithOffsets::OffsetsStream);
        SET_STREAM_CONTEXT(
            arrayBuilder, lengthsDescriptor, ArrayWithOffsets::LengthsStream);
        if (encodingLayoutTree.childrenCount() > 0) {
          NIMBLE_CHECK(
              encodingLayoutTree.childrenCount() == 2,
              "Invalid encoding layout tree. ArrayWithOffset node should have exactly two children.");
          initializeEncodingLayouts(
              arrayBuilder.elements(), encodingLayoutTree.child(0));
        }
        break;
      }
      case Kind::FlatMap: {
        NIMBLE_UNREACHABLE("Flatmap handled already");
      }
    }
#undef SET_STREAM_CONTEXT
  }
}
} // namespace

VeloxWriter::VeloxWriter(
    const velox::TypePtr& type,
    std::unique_ptr<velox::WriteFile> file,
    velox::memory::MemoryPool& pool,
    VeloxWriterOptions options)
    : schema_{velox::dwio::common::TypeWithId::create(type)},
      writerMemoryPool_{MemoryPoolHolder::create(
          pool,
          [&](auto& pool) {
            return pool.addAggregateChild(
                fmt::format("nimble_writer_{}", folly::Random::rand64()),
                options.reclaimerFactory());
          })},
      encodingMemoryPool_{MemoryPoolHolder::create(
          *writerMemoryPool_,
          [&](auto& pool) {
            return pool.addLeafChild(
                "encoding", true, options.reclaimerFactory());
          })},
      context_{std::make_unique<detail::WriterContext>(
          *writerMemoryPool_,
          std::move(options))},
      file_{std::move(file)},
      clusterIndexWriter_{index::ClusterIndexWriter::create(
          context_->options().clusterIndexConfig,
          type,
          &(*context_->bufferMemoryPool()))},
      hashIndexWriter_{index::HashIndexWriter::create(
          context_->options().hashIndexConfigs,
          type,
          &(*context_->bufferMemoryPool()))},
      sortedIndexWriter_{index::SortedIndexWriter::create(
          context_->options().sortedIndexConfigs,
          type,
          &(*context_->bufferMemoryPool()))},
      tabletWriter_{TabletWriter::create(
          file_.get(),
          *encodingMemoryPool_,
          {.layoutPlanner = std::make_unique<DefaultLayoutPlanner>(
               [&schemaBuilder = context_->schemaBuilder()]() {
                 return schemaBuilder.root();
               },
               context_->options().featureReordering),
           .metadataCompressionThreshold =
               context_->options().metadataCompressionThreshold.value_or(
                   kMetadataCompressionThreshold),
           .streamDeduplicationEnabled =
               context_->options().enableStreamDeduplication,
           .enableChunkIndex = context_->options().enableChunkIndex,
           .chunkIndexMinAvgChunks = context_->options().chunkIndexMinAvgChunks,
           .stripeGroupFlushCallback = clusterIndexWriter_ != nullptr
               ? TabletWriter::StripeGroupFlushCallback(
                     [this](
                         const WriteDataFn& writeDataFn,
                         const CreateMetadataSectionFn& createMetadataFn) {
                       clusterIndexWriter_->flush(
                           writeDataFn, createMetadataFn);
                     })
               : nullptr,
           .closeCallback = [this](
                                const WriteDataFn& writeDataFn,
                                const CreateMetadataSectionFn& createMetadataFn,
                                const WriteOptionalSectionFn& writeMetadataFn) {
             writeIndexes(writeDataFn, createMetadataFn, writeMetadataFn);
           }})} {
  NIMBLE_CHECK_NOT_NULL(file_);

  // Register handler for dynamically discovered FlatMap keys before creating
  // the writer tree, so that predefined keys also trigger the handler.
  context_->setFlatmapFieldAddedEventHandler([this](
                                                 const TypeBuilder& flatmap,
                                                 std::string_view fieldKey,
                                                 const TypeBuilder& fieldType) {
    // Mark the newly added child's in-map stream descriptor.
    auto& flatmapBuilder = flatmap.asFlatMap();
    getStreamContext(
        flatmapBuilder.inMapDescriptorAt(flatmapBuilder.childrenCount() - 1))
        .setIsInMapStream(true);

    // Handle encoding layout if configured.
    if (context_->options().encodingLayoutTree.has_value()) {
      auto* ctx = flatmap.context<FlatmapEncodingLayoutContext>();
      if (ctx != nullptr) {
        auto it = ctx->keyEncodings.find(fieldKey);
        if (it != ctx->keyEncodings.end()) {
          initializeEncodingLayouts(fieldType, it->second);
        }
      }
    }
  });

  rootWriter_ = createRootFieldWriter(schema_, *context_);

  if (context_->options().encodingLayoutTree.has_value()) {
    initializeEncodingLayouts(
        *rootWriter_->typeBuilder(),
        context_->options().encodingLayoutTree.value());
  }
}

VeloxWriter::~VeloxWriter() {}

bool VeloxWriter::write(const velox::VectorPtr& input) {
  if (lastException_) {
    std::rethrow_exception(lastException_);
  }

  NIMBLE_CHECK_NOT_NULL(file_, "Writer is already closed");
  try {
    const auto numRows = input->size();
    // When enableStatsConsistencyCheck is true, compute raw size using
    // RawSizeUtils to verify consistency with column statistics.
    // Otherwise, skip this computation as column statistics will provide
    // the raw size.
    // Skip entirely when stats collection is disabled — there is no
    // writeColumnStats() call to consume this value.
    if (context_->options().enableStatsCollection &&
        context_->options().enableStatsConsistencyCheck) {
      // Calculate raw size using schema information to correctly handle
      // passthrough flatmaps.
      RawSizeContext context;
      const auto rawSize = nimble::getRawSizeFromVector(
          input,
          velox::common::Ranges::of(0, numRows),
          context,
          schema_.get(),
          context_->flatMapNodeIds(),
          context_->ignoreTopLevelNulls());
      NIMBLE_CHECK_GE(rawSize, 0, "Invalid raw size");
      context_->updateFileRawSize(rawSize);
    }

    {
      velox::CpuWallTimer ingestionTimer{context_->ingestionTiming()};
      rootWriter_->write(input, OrderedRanges::of(0, numRows));
    }
    addIndexKey(input);

    uint64_t memoryUsed{0};
    for (const auto& [_, stream] : context_->streams()) {
      memoryUsed += stream->memoryUsed();
    }

    context_->setMemoryUsed(memoryUsed);
    context_->updateRowsInFile(numRows);
    context_->updateRowsInStripe(numRows);
    context_->setBytesWritten(file_->size());

    return evaluateFlushPolicy();
  } catch (const std::exception& e) {
    lastException_ = std::current_exception();
    context_->logger()->logException(LogOperation::Write, e.what());
    throw;
  } catch (...) {
    lastException_ = std::current_exception();
    context_->logger()->logException(
        LogOperation::Write,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

void VeloxWriter::writeMetadata() {
  if (context_->options().metadata.empty()) {
    return;
  }
  auto& metadata = context_->options().metadata;
  auto it = metadata.cbegin();
  flatbuffers::FlatBufferBuilder builder(kInitialSchemaSectionSize);
  auto entries =
      builder.CreateVector<flatbuffers::Offset<serialization::MetadataEntry>>(
          metadata.size(), [&builder, &it](size_t /* i */) {
            auto entry = serialization::CreateMetadataEntry(
                builder,
                builder.CreateString(it->first),
                builder.CreateString(it->second));
            ++it;
            return entry;
          });

  builder.Finish(serialization::CreateMetadata(builder, entries));
  tabletWriter_->writeOptionalSection(
      std::string(kMetadataSection),
      {reinterpret_cast<const char*>(builder.GetBufferPointer()),
       builder.GetSize()});
}

void VeloxWriter::writeColumnStats() {
  // When enableStatsConsistencyCheck is true, verify that fileRawSize
  // (accumulated via RawSizeUtils) matches the root column statistics.
  if (context_->options().enableStatsConsistencyCheck) {
    NIMBLE_CHECK_EQ(
        context_->fileRawSize(),
        context_->columnStats().front()->getLogicalSize(),
        "Mismatched raw sizes!");
  }

  if (context_->options().enableVectorizedStats) {
    VectorizedFileStats fileStats{
        context_->columnStats(), encodingMemoryPool_.get()};
    Buffer buffer{*encodingMemoryPool_};
    tabletWriter_->writeOptionalSection(
        std::string(kVectorizedStatsSection), fileStats.serialize(buffer));
  } else {
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(
        serialization::CreateStats(builder, context_->fileRawSize()));
    tabletWriter_->writeOptionalSection(
        std::string(kStatsSection),
        {reinterpret_cast<const char*>(builder.GetBufferPointer()),
         builder.GetSize()});
  }
}

void VeloxWriter::writeSchema() {
  SchemaSerializer serializer;
  tabletWriter_->writeOptionalSection(
      std::string(kSchemaSection),
      serializer.serialize(context_->schemaBuilder()));
}

bool VeloxWriter::hasClusterIndex() const {
  return clusterIndexWriter_ != nullptr;
}

bool VeloxWriter::hasHashIndex() const {
  return hashIndexWriter_ != nullptr;
}

bool VeloxWriter::hasSortedIndex() const {
  return sortedIndexWriter_ != nullptr;
}

void VeloxWriter::addIndexKey(
    const velox::VectorPtr& input,
    velox::dwio::common::ExecutorBarrier* barrier) {
  addClusterIndexKey(input, barrier);
  addHashIndexKey(input, barrier);
  addSortedIndexKey(input, barrier);
}

void VeloxWriter::addClusterIndexKey(
    const velox::VectorPtr& input,
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasClusterIndex()) {
    return;
  }
  if (barrier != nullptr) {
    barrier->add([&]() { clusterIndexWriter_->write(input); });
  } else {
    clusterIndexWriter_->write(input);
  }
}

void VeloxWriter::addHashIndexKey(
    const velox::VectorPtr& input,
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasHashIndex()) {
    return;
  }
  if (barrier != nullptr) {
    barrier->add([&]() { hashIndexWriter_->write(input); });
  } else {
    hashIndexWriter_->write(input);
  }
}

void VeloxWriter::addSortedIndexKey(
    const velox::VectorPtr& input,
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasSortedIndex()) {
    return;
  }
  if (barrier != nullptr) {
    barrier->add([&]() { sortedIndexWriter_->write(input); });
  } else {
    sortedIndexWriter_->write(input);
  }
}

void VeloxWriter::writeIndexes(
    const WriteDataFn& writeDataFn,
    const CreateMetadataSectionFn& createMetadataFn,
    const WriteOptionalSectionFn& writeMetadataFn) {
  if (hasHashIndex()) {
    hashIndexWriter_->close(writeDataFn, createMetadataFn, writeMetadataFn);
  }
  if (hasSortedIndex()) {
    sortedIndexWriter_->close(writeDataFn, createMetadataFn, writeMetadataFn);
  }
  if (hasClusterIndex()) {
    clusterIndexWriter_->close(writeDataFn, createMetadataFn, writeMetadataFn);
  }
}

bool VeloxWriter::shouldFlush(FlushPolicy* policy) const {
  return policy->shouldFlush(
      StripeProgress{
          .stripeRawSize = context_->memoryUsed(),
          .stripeEncodedSize = context_->stripeEncodedPhysicalSize(),
          .stripeEncodedLogicalSize = context_->stripeEncodedLogicalSize()});
}

bool VeloxWriter::shouldChunk(FlushPolicy* policy) const {
  return policy->shouldChunk(
      StripeProgress{
          .stripeRawSize = context_->memoryUsed(),
          .stripeEncodedSize = context_->stripeEncodedPhysicalSize(),
          .stripeEncodedLogicalSize = context_->stripeEncodedLogicalSize()});
}

void VeloxWriter::close() {
  if (lastException_) {
    std::rethrow_exception(lastException_);
  }

  if (file_ != nullptr) {
    try {
      writeStripe();
      rootWriter_->close();
      if (context_->options().enableStatsCollection) {
        context_->finalizeStatsCollectors();
      }

      writeMetadata();
      if (context_->options().enableStatsCollection) {
        writeColumnStats();
      }
      writeSchema();

      tabletWriter_->close();
      file_->close();
      context_->setBytesWritten(file_->size());

      const auto stats = this->stats();
      // TODO: compute and populate input size.
      FileCloseMetrics metrics{
          .rowCount = context_->rowsInFile(),
          .stripeCount = context_->getStripeIndex(),
          .fileSize = context_->bytesWritten(),
          .encodingCpuNs = stats.encodingCpuTimeNs,
          .encodingWallNs = stats.encodingWallTimeNs};
      context_->logger()->logFileClose(metrics);
      file_ = nullptr;
    } catch (const std::exception& e) {
      lastException_ = std::current_exception();
      context_->logger()->logException(LogOperation::Close, e.what());
      file_ = nullptr;
      throw;
    } catch (...) {
      lastException_ = std::current_exception();
      context_->logger()->logException(
          LogOperation::Close,
          folly::to<std::string>(
              folly::exceptionStr(std::current_exception())));
      file_ = nullptr;
      throw;
    }
  }
}

void VeloxWriter::flush() {
  if (lastException_) {
    std::rethrow_exception(lastException_);
  }

  try {
    writeStripe();
  } catch (const std::exception& e) {
    lastException_ = std::current_exception();
    context_->logger()->logException(LogOperation::Flush, e.what());
    throw;
  } catch (...) {
    lastException_ = std::current_exception();
    context_->logger()->logException(
        LogOperation::Flush,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

void VeloxWriter::ensureEncodingBuffer() {
  if (encodingBuffer_ == nullptr) {
    encodingBuffer_ = std::make_unique<Buffer>(*encodingMemoryPool_);
  }
}

void VeloxWriter::clearEncodingBuffer() {
  encodingBuffer_.reset();
}

void VeloxWriter::ensureWriteStreams() {
  ensureEncodingBuffer();
  const auto schemaNodeCount = context_->schemaBuilder().nodeCount();
  encodedStreams_.resize(schemaNodeCount);
}

void VeloxWriter::resetFieldWriter() {
  rootWriter_->reset();
}

void VeloxWriter::writeStreams() {
  std::atomic_uint64_t chunkSize{0};
  std::atomic_uint64_t encodingCpuNanos{0};
  uint64_t encodingWallNanos{0};
  {
    LoggingScope scope{*context_->logger()};
    velox::NanosecondTimer wallTimer{&encodingWallNanos};

    ensureWriteStreams();

    if (context_->options().encodingExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options().encodingExecutor};
      for (auto& [nodeId, streamData] : context_->streams()) {
        barrier.add([&,
                     statsCollector = context_->getStatsCollector(nodeId),
                     _streamData = streamData.get()]() {
          uint64_t startCpuNs = velox::process::threadCpuNanos();
          uint64_t streamSize{0};
          processStream(*_streamData, streamSize, chunkSize);
          if (statsCollector) {
            statsCollector->addPhysicalSize(streamSize);
          }
          encodingCpuNanos.fetch_add(
              velox::process::threadCpuNanos() - startCpuNs,
              std::memory_order_relaxed);
        });
      }

      barrier.waitAll();
    } else {
      const auto& streams = context_->streams();
      for (auto& [nodeId, streamData] : streams) {
        auto statsCollector = context_->getStatsCollector(nodeId);
        uint64_t startCpuNs = velox::process::threadCpuNanos();
        uint64_t streamSize{0};
        processStream(*streamData, streamSize, chunkSize);
        if (statsCollector) {
          statsCollector->addPhysicalSize(streamSize);
        }
        encodingCpuNanos.fetch_add(
            velox::process::threadCpuNanos() - startCpuNs,
            std::memory_order_relaxed);
      }
    }
    resetFieldWriter();
  }

  velox::CpuWallTiming encodingTiming;
  encodingTiming.cpuNanos = encodingCpuNanos.load(std::memory_order_relaxed);
  encodingTiming.wallNanos = encodingWallNanos;
  context_->encodingTiming().add(encodingTiming);
  VLOG(1) << "writeChunk cpu: " << velox::succinctNanos(encodingTiming.cpuNanos)
          << ", wall: " << velox::succinctNanos(encodingWallNanos)
          << ", chunk size: " << velox::succinctBytes(chunkSize);
}

void VeloxWriter::encodeStream(
    StreamData& streamData,
    uint64_t& streamSize,
    std::atomic_uint64_t& chunkSize) {
  const auto offset = streamData.descriptor().offset();
  NIMBLE_DCHECK_LT(
      offset, encodedStreams_.size(), "Stream offset out of range.");
  auto& encodedStream = encodedStreams_[offset];
  // NOTE: we always expect the stream to be empty as encodeStream is only
  // used in non-chunked mode.
  NIMBLE_CHECK(encodedStream.chunks.empty());
  auto& chunk = encodedStream.chunks.emplace_back();
  const auto chunkBytes = encodeChunk(streamData, chunk);
  streamSize += chunkBytes;
  chunkSize += chunkBytes;
  streamData.reset();
}

void VeloxWriter::processStream(
    StreamData& streamData,
    uint64_t& streamSize,
    std::atomic_uint64_t& chunkSize) {
  const auto offset = streamData.descriptor().offset();
  const auto* context = streamData.descriptor().context<WriterStreamContext>();
  NIMBLE_CHECK(encodedStreams_[offset].chunks.empty());
  if ((context != nullptr) && context->isNullStream()) {
    // For null streams we promote the null values to be written as
    // boolean data.
    if (streamData.hasNullValues()) {
      NullsAsDataStreamData nullsStreamData{streamData};
      encodeStream(nullsStreamData, streamSize, chunkSize);
    }
  } else if (
      (context != nullptr) && context->isInMapStream() &&
      context_->options().skipConstantFlatMapInMapStreams) {
    // When enabled, skip encoding in-map streams that are all-true (every row
    // has the key) or all-false (no row has the key). The reader distinguishes
    // these by checking value stream presence: all-true keys have value
    // streams, all-false keys do not.
    //
    // NOTE: readers that don't infer missing in-map streams require
    // skipConstantFlatMapInMapStreams to remain false.
    streamData.materialize();
    if (!isConstantBoolStream(streamData.data())) {
      encodeStream(streamData, streamSize, chunkSize);
    }
  } else {
    streamData.materialize();
    if (!streamData.data().empty()) {
      encodeStream(streamData, streamSize, chunkSize);
    }
  }
}

bool VeloxWriter::encodeStreamChunk(
    StreamData& streamData,
    uint64_t minChunkSize,
    uint64_t maxChunkSize,
    bool ensureFullChunks,
    Stream& encodedStream,
    uint64_t& streamBytes,
    std::atomic_uint64_t& chunkBytes,
    std::atomic_uint64_t& logicalBytes) {
  bool writtenChunk{false};
  logicalBytes += streamData.memoryUsed();
  auto& streamChunks = encodedStream.chunks;
  auto chunker = getStreamChunker(
      streamData,
      StreamChunkerOptions{
          .minChunkSize = minChunkSize,
          .maxChunkSize = maxChunkSize,
          .ensureFullChunks = ensureFullChunks,
          .isFirstChunk = streamChunks.empty()});
  uint64_t encodedChunkBytes{0};
  while (auto chunkView = chunker->next()) {
    auto& streamChunk = streamChunks.emplace_back();
    encodedChunkBytes += encodeChunk(*chunkView, streamChunk);
    writtenChunk = true;
  }
  streamBytes += encodedChunkBytes;
  chunkBytes += encodedChunkBytes;
  // Compact erases processed stream data to reclaim memory.
  chunker->compact();
  logicalBytes -= streamData.memoryUsed();
  return writtenChunk;
}

uint32_t VeloxWriter::encodeChunk(const StreamData& chunkView, Chunk& chunk) {
  std::string_view encoded =
      encodeStreamData(*context_, *encodingBuffer_, chunkView);
  NIMBLE_DCHECK(!encoded.empty());
  if (encoded.empty()) {
    return 0;
  }
  uint32_t chunkBytes{0};
  chunk.rowCount = chunkView.rowCount();
  ChunkedStreamWriter chunkWriter{
      *encodingBuffer_, context_->options().chunkCompression};
  for (auto& buffer : chunkWriter.encode(encoded)) {
    chunkBytes += buffer.size();
    chunk.content.push_back(std::move(buffer));
  }
  return chunkBytes;
}

bool VeloxWriter::writeChunks(
    std::span<const uint32_t> streamIndices,
    bool ensureFullChunks,
    bool lastChunk) {
  std::atomic_uint64_t chunkBytes{0};
  std::atomic_uint64_t logicalBytes{0};
  std::atomic_bool writtenChunk{false};
  std::atomic_uint64_t encodingCpuNanos{0};
  uint64_t encodingWallNanos{0};
  {
    LoggingScope scope{*context_->logger()};
    velox::NanosecondTimer wallTimer{&encodingWallNanos};
    const auto& options = context_->options();
    const auto minChunkSize = lastChunk ? 0 : options.minStreamChunkRawSize;
    const auto schemaNodeCount = context_->schemaBuilder().nodeCount();
    const auto maxChunkSize = schemaNodeCount > options.largeSchemaThreshold
        ? options.wideSchemaMaxStreamChunkRawSize
        : options.maxStreamChunkRawSize;
    ensureWriteStreams();

    const auto& streams = context_->streams();
    if (context_->options().encodingExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options().encodingExecutor};
      for (auto streamIndex : streamIndices) {
        auto& [nodeId, streamData] = streams[streamIndex];
        const auto offset = streamData->descriptor().offset();
        auto& encodeStream = encodedStreams_[offset];
        barrier.add([&,
                     streamData = streamData.get(),
                     statsCollector = context_->getStatsCollector(nodeId)] {
          uint64_t startCpuNs = velox::process::threadCpuNanos();
          uint64_t streamSize = 0;
          if (encodeStreamChunk(
                  *streamData,
                  minChunkSize,
                  maxChunkSize,
                  ensureFullChunks,
                  encodeStream,
                  streamSize,
                  chunkBytes,
                  logicalBytes)) {
            writtenChunk = true;
          }
          if (statsCollector) {
            statsCollector->addPhysicalSize(streamSize);
          }
          encodingCpuNanos.fetch_add(
              velox::process::threadCpuNanos() - startCpuNs,
              std::memory_order_relaxed);
        });
      }

      barrier.waitAll();
    } else {
      for (auto streamIndex : streamIndices) {
        auto& [nodeId, streamData] = streams[streamIndex];
        const auto offset = streamData->descriptor().offset();
        auto statsCollector = context_->getStatsCollector(nodeId);
        uint64_t startCpuNs = velox::process::threadCpuNanos();
        uint64_t streamSize = 0;
        if (encodeStreamChunk(
                *streamData,
                minChunkSize,
                maxChunkSize,
                ensureFullChunks,
                encodedStreams_[offset],
                streamSize,
                chunkBytes,
                logicalBytes)) {
          writtenChunk = true;
        }
        if (statsCollector) {
          statsCollector->addPhysicalSize(streamSize);
        }
        encodingCpuNanos.fetch_add(
            velox::process::threadCpuNanos() - startCpuNs,
            std::memory_order_relaxed);
      }
    }

    if (lastChunk) {
      resetFieldWriter();
    }

    context_->updateStripeEncodedPhysicalSize(chunkBytes);
    context_->updateStripeEncodedLogicalSize(logicalBytes);
    context_->updateMemoryUsed(-logicalBytes);
  }

  velox::CpuWallTiming encodingTiming;
  encodingTiming.cpuNanos = encodingCpuNanos.load(std::memory_order_relaxed);
  encodingTiming.wallNanos = encodingWallNanos;
  context_->encodingTiming().add(encodingTiming);
  if (writtenChunk) {
    context_->recordChunkSize(chunkBytes);
  }
  VLOG(1) << "writeChunk cpu: " << velox::succinctNanos(encodingTiming.cpuNanos)
          << ", wall: " << velox::succinctNanos(encodingWallNanos)
          << ", chunk size: " << velox::succinctBytes(chunkBytes);
  return writtenChunk;
}

bool VeloxWriter::flushChunks(
    const std::vector<uint32_t>& indices,
    bool ensureFullChunks,
    FlushPolicy* flushPolicy) {
  const size_t indicesCount = indices.size();
  const auto batchSize = context_->options().chunkedStreamBatchSize;
  for (size_t index = 0; index < indicesCount; index += batchSize) {
    const size_t currentBatchSize = std::min(batchSize, indicesCount - index);
    std::span<const uint32_t> batchIndices(
        indices.begin() + index, currentBatchSize);
    // Stop attempting chunking once streams are too small to chunk or
    // memory pressure is relieved.
    if (!writeChunks(batchIndices, ensureFullChunks) ||
        !shouldChunk(flushPolicy)) {
      return false;
    }
  }
  return true;
}

bool VeloxWriter::writeStripe() {
  if (context_->rowsInStripe() == 0) {
    return false;
  }

  if (context_->options().enableChunking) {
    // Chunk all streams.
    std::vector<uint32_t> streamIndices(context_->streams().size());
    std::iota(streamIndices.begin(), streamIndices.end(), 0);
    writeChunks(streamIndices, /*ensureFullChunks=*/false, /*lastChunk=*/true);
  } else {
    writeStreams();
  }

  uint64_t stripeSize{0};
  {
    LoggingScope scope{*context_->logger()};

    size_t nonEmptyCount{0};
    for (auto i = 0; i < encodedStreams_.size(); ++i) {
      auto& source = encodedStreams_[i];
      if (!source.chunks.empty()) {
        source.offset = i;
        if (nonEmptyCount != i) {
          encodedStreams_[nonEmptyCount] = std::move(source);
        }
        ++nonEmptyCount;
      }
    }
    encodedStreams_.resize(nonEmptyCount);

    const uint64_t startSize = tabletWriter_->size();
    {
      velox::CpuWallTimer writeTimer{context_->writeTiming()};
      tabletWriter_->writeStripe(
          context_->rowsInStripe(), std::move(encodedStreams_));
    }
    stripeSize = tabletWriter_->size() - startSize;
    clearEncodingBuffer();
    // TODO: once chunked string fields are supported, move string buffer
    // reset to writeStreams()
    context_->resetStringBuffer();
  }

  NIMBLE_CHECK_LT(
      stripeSize,
      std::numeric_limits<uint32_t>::max(),
      "unexpected stripe size");

  VLOG(1) << "on disk stripe size: " << velox::succinctBytes(stripeSize);

  StripeFlushMetrics metrics{
      .inputSize = context_->stripeEncodedPhysicalSize(),
      .rowCount = context_->rowsInStripe(),
      .stripeSize = stripeSize,
      .trackedMemory = context_->memoryUsed(),
  };
  context_->logger()->logStripeFlush(metrics);
  context_->nextStripe();
  return true;
}

bool VeloxWriter::evaluateFlushPolicy() {
  // NOTE that flush policy factory is stateful, so we need to get a new
  // policy every time we check.
  auto flushPolicy = context_->options().flushPolicyFactory();
  if (context_->options().enableChunking && shouldChunk(flushPolicy.get())) {
    // Relieve memory pressure by chunking streams above max size.
    const auto& streams = context_->streams();
    std::vector<uint32_t> streamIndices;
    const auto streamCount = streams.size();
    streamIndices.reserve(streamCount);

    // Determine size threshold for soft chunking based on schema width.
    const auto& options = context_->options();
    const auto maxChunkSize = streamCount > options.largeSchemaThreshold
        ? options.wideSchemaMaxStreamChunkRawSize
        : options.maxStreamChunkRawSize;
    for (auto streamIndex = 0; streamIndex < streams.size(); ++streamIndex) {
      if (streams[streamIndex].second->memoryUsed() >= maxChunkSize) {
        streamIndices.push_back(streamIndex);
      }
    }

    // Soft chunking.
    const bool continueChunking = flushChunks(
        streamIndices, /*ensureFullChunks=*/true, flushPolicy.get());
    // Hard chunking when chunking streams above maxChunkSize fails to
    // relieve memory pressure.
    if (continueChunking) {
      // Relieve memory pressure by chunking small streams.
      // Sort streams for chunking based on raw memory usage.
      // TODO(T240072104): Improve performance by bucketing the streams
      // by size (by most significant bit) instead of sorting them.
      // Only sort streams above minChunkSize.
      streamIndices.resize(streams.size());
      std::iota(streamIndices.begin(), streamIndices.end(), 0);
      std::sort(
          streamIndices.begin(),
          streamIndices.end(),
          [&](const uint32_t& a, const uint32_t& b) {
            return streams[a].second->memoryUsed() >
                streams[b].second->memoryUsed();
          });
      flushChunks(streamIndices, /*ensureFullChunks=*/false, flushPolicy.get());
    }
  }

  if (shouldFlush(flushPolicy.get())) {
    return writeStripe();
  }
  return false;
}

VeloxWriter::Stats VeloxWriter::stats() const {
  return Stats{
      .writtenBytes = context_->bytesWritten(),
      .stripeCount = folly::to<uint32_t>(context_->getStripeIndex()),
      .inputBytes = context_->fileRawSize(),
      .writeCpuTimeNs = context_->writeTiming().cpuNanos,
      .writeWallTimeNs = context_->writeTiming().wallNanos,
      .ingestionCpuTimeNs = context_->ingestionTiming().cpuNanos,
      .encodingCpuTimeNs = context_->encodingTiming().cpuNanos,
      .encodingWallTimeNs = context_->encodingTiming().wallNanos,
      .encodingSelectionCpuTimeNs =
          context_->encodingSelectionTiming().cpuNanos,
      .rowsPerStripe = toRuntimeMetric(context_->rowsPerStripe()),
      .chunkSizeBytes = context_->chunkSizeStats(),
      .duplicateStreamCount = tabletWriter_->stats().duplicateStreamCount,
      .duplicateStreamBytes = tabletWriter_->stats().duplicateStreamBytes,
      .columnStats = context_->columnStats(),
  };
}
} // namespace facebook::nimble
