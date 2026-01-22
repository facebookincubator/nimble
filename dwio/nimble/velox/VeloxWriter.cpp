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

#include "dwio/nimble/common/Exceptions.h"
#include "dwio/nimble/common/Types.h"
#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
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
#include "velox/common/time/CpuWallTimer.h"
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
  }

  const VeloxWriterOptions& options() const {
    return options_;
  }

  velox::CpuWallTiming& totalFlushTiming() {
    return totalFlushTiming_;
  }

  const velox::CpuWallTiming& totalFlushTiming() const {
    return totalFlushTiming_;
  }

  const velox::CpuWallTiming& stripeFlushTiming() const {
    return stripeFlushTiming_;
  }

  void addStripeFlushTiming(const velox::CpuWallTiming& timing) {
    stripeFlushTiming_.add(timing);
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
    totalFlushTiming_.add(stripeFlushTiming_);
    stripeFlushTiming_.clear();
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
  velox::CpuWallTiming totalFlushTiming_;
  velox::CpuWallTiming stripeFlushTiming_;
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
  bool isNullStream = false;
  const EncodingLayout* encoding;
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
        context.options().encodingSelectionPolicyFactory);

  } else {
    policy = std::unique_ptr<EncodingSelectionPolicy<T>>(
        static_cast<EncodingSelectionPolicy<T>*>(
            context.options()
                .encodingSelectionPolicyFactory(TypeTraits<T>::dataType)
                .release()));
  }

  if (streamData.hasNulls()) {
    std::span<const bool> notNulls = streamData.nonNulls();
    return EncodingFactory::encodeNullable(
        std::move(policy), data, notNulls, buffer);
  } else {
    return EncodingFactory::encode(std::move(policy), data, buffer);
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
  if (streamContext && streamContext->encoding) {
    encodingLayout.emplace(*streamContext->encoding);
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
  if (context) {
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
std::unique_ptr<FieldWriter> createRootFieldWriter(
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type,
    detail::WriterContext& context) {
  if (!context.options().flatMapColumns.empty()) {
    context.clearAndReserveFlatMapNodeIds(
        context.options().flatMapColumns.size());
    for (const auto& column : context.options().flatMapColumns) {
      context.addFlatMapNodeId(type->childByName(column)->id());
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

  return FieldWriter::create(context, type, [&](const TypeBuilder& type) {
    switch (type.kind()) {
      case Kind::Row: {
        getStreamContext(type.asRow().nullsDescriptor()).isNullStream = true;
        break;
      }
      case Kind::FlatMap: {
        getStreamContext(type.asFlatMap().nullsDescriptor()).isNullStream =
            true;
        break;
      }
      default:
        break;
    }
  });
}

std::optional<TabletIndexConfig> createTabletIndexConfig(
    const std::optional<IndexConfig>& config,
    index::IndexWriter* indexWriter) {
  if (!config.has_value() || indexWriter == nullptr) {
    return std::nullopt;
  }
  return TabletIndexConfig{
      .columns = config->columns,
      .sortOrders = indexWriter->sortOrders(),
      .enforceKeyOrder = config->enforceKeyOrder,
  };
}

void initializeEncodingLayouts(
    const TypeBuilder& typeBuilder,
    const EncodingLayoutTree& encodingLayoutTree) {
  {
#define _SET_STREAM_CONTEXT(builder, descriptor, identifier)      \
  if (auto* encodingLayout = encodingLayoutTree.encodingLayout(   \
          EncodingLayoutTree::StreamIdentifiers::identifier)) {   \
    auto& streamContext = getStreamContext(builder.descriptor()); \
    streamContext.encoding = encodingLayout;                      \
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

      _SET_STREAM_CONTEXT(mapBuilder, nullsDescriptor, FlatMap::NullsStream);
    } else {
      switch (typeBuilder.kind()) {
        case Kind::Scalar: {
          NIMBLE_CHECK_EQ(
              encodingLayoutTree.schemaKind(),
              Kind::Scalar,
              "Incompatible encoding layout node. Expecting scalar node.");
          _SET_STREAM_CONTEXT(
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
          _SET_STREAM_CONTEXT(
              timestampMicroNanoBuilder,
              microsDescriptor,
              TimestampMicroNano::MicrosStream);
          _SET_STREAM_CONTEXT(
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
          _SET_STREAM_CONTEXT(rowBuilder, nullsDescriptor, Row::NullsStream);
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
          _SET_STREAM_CONTEXT(
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

          _SET_STREAM_CONTEXT(
              mapBuilder, lengthsDescriptor, Map::LengthsStream);
          if (encodingLayoutTree.childrenCount() > 0) {
            NIMBLE_CHECK(
                encodingLayoutTree.childrenCount() == 2,
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
          _SET_STREAM_CONTEXT(
              mapBuilder, offsetsDescriptor, SlidingWindowMap::OffsetsStream);
          _SET_STREAM_CONTEXT(
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
          _SET_STREAM_CONTEXT(
              arrayBuilder, offsetsDescriptor, ArrayWithOffsets::OffsetsStream);
          _SET_STREAM_CONTEXT(
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
    }
#undef _SET_STREAM_CONTEXT
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
      rootWriter_{createRootFieldWriter(schema_, *context_)},
      indexWriter_{IndexWriter::create(
          context_->options().indexConfig,
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
           .indexConfig = createTabletIndexConfig(
               context_->options().indexConfig,
               indexWriter_.get())})} {
  NIMBLE_CHECK_NOT_NULL(file_);

  if (context_->options().encodingLayoutTree.has_value()) {
    context_->setFlatmapFieldAddedEventHandler(
        [&](const TypeBuilder& flatmap,
            std::string_view fieldKey,
            const TypeBuilder& fieldType) {
          auto* context = flatmap.context<FlatmapEncodingLayoutContext>();
          if (context) {
            auto it = context->keyEncodings.find(fieldKey);
            if (it != context->keyEncodings.end()) {
              initializeEncodingLayouts(fieldType, it->second);
            }
          }
        });
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
    // Calculate raw size.
    const auto rawSize = nimble::getRawSizeFromVector(
        input, velox::common::Ranges::of(0, numRows));
    NIMBLE_CHECK_GE(rawSize, 0, "Invalid raw size");
    context_->updateFileRawSize(rawSize);

    if (context_->options().writeExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options().writeExecutor};
      rootWriter_->write(input, OrderedRanges::of(0, numRows), &barrier);
      addIndexKey(input, &barrier);
      barrier.waitAll();
    } else {
      rootWriter_->write(input, OrderedRanges::of(0, numRows));
      addIndexKey(input);
    }

    uint64_t memoryUsed{0};
    for (const auto& stream : context_->streams()) {
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
  nimble::aggregateStats(
      *context_->schemaBuilder().root(), context_->columnStats());
  flatbuffers::FlatBufferBuilder builder;
  builder.Finish(serialization::CreateStats(builder, context_->fileRawSize()));
  tabletWriter_->writeOptionalSection(
      std::string(kStatsSection),
      {reinterpret_cast<const char*>(builder.GetBufferPointer()),
       builder.GetSize()});
}

void VeloxWriter::writeSchema() {
  SchemaSerializer serializer;
  tabletWriter_->writeOptionalSection(
      std::string(kSchemaSection),
      serializer.serialize(context_->schemaBuilder()));
}

bool VeloxWriter::hasIndex() const {
  return indexWriter_ != nullptr;
}

void VeloxWriter::addIndexKey(
    const velox::VectorPtr& input,
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasIndex()) {
    return;
  }
  ensureEncodingBuffer();
  if (barrier != nullptr) {
    barrier->add([&]() { indexWriter_->write(input, *encodingBuffer_); });
  } else {
    indexWriter_->write(input, *encodingBuffer_);
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
      if (hasIndex()) {
        indexWriter_->close();
      }

      writeMetadata();
      writeColumnStats();
      writeSchema();

      tabletWriter_->close();
      file_->close();
      context_->setBytesWritten(file_->size());

      auto runStats = getRunStats();
      // TODO: compute and populate input size.
      FileCloseMetrics metrics{
          .rowCount = context_->rowsInFile(),
          .stripeCount = context_->getStripeIndex(),
          .fileSize = context_->bytesWritten(),
          .totalFlushCpuUsec = runStats.flushCpuTimeUsec,
          .totalFlushWallTimeUsec = runStats.flushWallTimeUsec};
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
  velox::CpuWallTiming flushTiming;
  {
    LoggingScope scope{*context_->logger()};
    velox::CpuWallTimer veloxTimer{flushTiming};

    ensureWriteStreams();

    if (context_->options().encodingExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options().encodingExecutor};
      for (auto& streamData : context_->streams()) {
        auto& streamSize =
            context_->columnStats(streamData->descriptor().offset())
                .physicalSize;
        barrier.add([&, _streamData = streamData.get()]() {
          processStream(*_streamData, streamSize, chunkSize);
        });
      }

      // Handle keyStream if index is enabled
      maybeProcessKeyStream(&barrier);
      barrier.waitAll();
    } else {
      const auto& streams = context_->streams();
      for (auto& streamData : streams) {
        auto& streamSize =
            context_->columnStats(streamData->descriptor().offset())
                .physicalSize;
        processStream(*streamData, streamSize, chunkSize);
      }

      // Handle keyStream if index is enabled
      maybeProcessKeyStream();
    }
    resetFieldWriter();
  }

  context_->addStripeFlushTiming(flushTiming);
  VLOG(1) << "writeChunk time: " << velox::succinctNanos(flushTiming.wallNanos)
          << ", chunk size: " << velox::succinctBytes(chunkSize);
}

void VeloxWriter::encodeKeyStreamChunk(bool lastChunk, bool ensureFullChunks) {
  NIMBLE_CHECK(hasIndex());
  if (!indexWriter_->hasKeys()) {
    return;
  }
  // IndexWriter accumulates chunks internally, accessed via finishStripe()
  indexWriter_->encodeChunk(ensureFullChunks, lastChunk, *encodingBuffer_);
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
  if ((context != nullptr) && context->isNullStream) {
    // For null streams we promote the null values to be written as
    // boolean data.
    // We still apply the same null logic, where if all values are
    // non-nulls, we omit the entire stream.
    if (streamData.hasNulls()) {
      NullsAsDataStreamData nullsStreamData{streamData};
      encodeStream(nullsStreamData, streamSize, chunkSize);
    }
  } else {
    streamData.materialize();
    if (!streamData.data().empty()) {
      encodeStream(streamData, streamSize, chunkSize);
    }
  }
}

void VeloxWriter::maybeProcessKeyStream(
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasIndex()) {
    return;
  }
  NIMBLE_CHECK(indexWriter_->hasKeys());
  auto encode = [this]() { indexWriter_->encodeStream(*encodingBuffer_); };
  if (barrier != nullptr) {
    barrier->add(std::move(encode));
  } else {
    encode();
  }
}

void VeloxWriter::maybeEncodeKeyStreamChunk(
    bool lastChunk,
    bool ensureFullChunks,
    velox::dwio::common::ExecutorBarrier* barrier) {
  if (!hasIndex()) {
    return;
  }
  auto encode = [&]() { encodeKeyStreamChunk(lastChunk, ensureFullChunks); };
  if (barrier != nullptr) {
    barrier->add(std::move(encode));
  } else {
    encode();
  }
}

std::optional<KeyStream> VeloxWriter::finishKeyStream() {
  if (!hasIndex()) {
    return std::nullopt;
  }
  return indexWriter_->finishStripe(*encodingBuffer_);
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
  ChunkedStreamWriter chunkWriter{*encodingBuffer_};
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
  velox::CpuWallTiming flushTiming;
  {
    LoggingScope scope{*context_->logger()};
    velox::CpuWallTimer veloxTimer{flushTiming};
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
        auto& streamData = streams[streamIndex];
        const auto offset = streamData->descriptor().offset();
        auto& streamSize = context_->columnStats(offset).physicalSize;
        auto& encodeStream = encodedStreams_[offset];
        barrier.add([&] {
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
        });
      }

      maybeEncodeKeyStreamChunk(lastChunk, ensureFullChunks, &barrier);

      barrier.waitAll();
    } else {
      for (auto streamIndex : streamIndices) {
        auto* streamData = streams[streamIndex].get();
        const auto offset = streamData->descriptor().offset();
        if (encodeStreamChunk(
                *streamData,
                minChunkSize,
                maxChunkSize,
                ensureFullChunks,
                encodedStreams_[offset],
                context_->columnStats()[offset].physicalSize,
                chunkBytes,
                logicalBytes)) {
          writtenChunk = true;
        }
      }

      maybeEncodeKeyStreamChunk(lastChunk, ensureFullChunks);
    }

    if (lastChunk) {
      resetFieldWriter();
    }

    context_->updateStripeEncodedPhysicalSize(chunkBytes);
    context_->updateStripeEncodedLogicalSize(logicalBytes);
    context_->updateMemoryUsed(-logicalBytes);
  }

  context_->addStripeFlushTiming(flushTiming);
  VLOG(1) << "writeChunk time: " << velox::succinctNanos(flushTiming.wallNanos)
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
  velox::CpuWallTiming flushTiming;
  {
    LoggingScope scope{*context_->logger()};
    velox::CpuWallTimer veloxTimer{flushTiming};

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

    std::optional<KeyStream> keyStream = finishKeyStream();

    const uint64_t startSize = tabletWriter_->size();
    tabletWriter_->writeStripe(
        context_->rowsInStripe(),
        std::move(encodedStreams_),
        std::move(keyStream));
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

  // Consider getting this from flush timing.
  context_->addStripeFlushTiming(flushTiming);
  VLOG(1) << "writeStripe time: " << velox::succinctNanos(flushTiming.wallNanos)
          << ", on disk stripe size: " << velox::succinctBytes(stripeSize);

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
      if (streams[streamIndex]->memoryUsed() >= maxChunkSize) {
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
            return streams[a]->memoryUsed() > streams[b]->memoryUsed();
          });
      flushChunks(streamIndices, /*ensureFullChunks=*/false, flushPolicy.get());
    }
  }

  if (shouldFlush(flushPolicy.get())) {
    return writeStripe();
  }
  return false;
}

VeloxWriter::RunStats VeloxWriter::getRunStats() const {
  return RunStats{
      .bytesWritten = context_->bytesWritten(),
      .stripeCount = folly::to<uint32_t>(context_->getStripeIndex()),
      .rawSize = context_->fileRawSize(),
      .rowsPerStripe = context_->rowsPerStripe(),
      .flushCpuTimeUsec = context_->totalFlushTiming().cpuNanos / 1'000,
      .flushWallTimeUsec = context_->totalFlushTiming().wallNanos / 1'000,
      .encodingSelectionCpuTimeUsec =
          context_->encodingSelectionTiming().cpuNanos / 1'000,
      .inputBufferReallocCount = context_->inputBufferGrowthStats().count,
      .inputBufferReallocItemCount =
          context_->inputBufferGrowthStats().itemCount,
      .columnStats = context_->columnStats(),
  };
}
} // namespace facebook::nimble
