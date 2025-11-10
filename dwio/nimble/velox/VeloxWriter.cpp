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
#include "velox/dwio/common/ExecutorBarrier.h"
#include "velox/type/Type.h"

namespace facebook::nimble {

namespace detail {

class WriterContext : public FieldWriterContext {
 public:
  const VeloxWriterOptions options;
  velox::CpuWallTiming totalFlushTiming;
  velox::CpuWallTiming stripeFlushTiming;
  velox::CpuWallTiming encodingSelectionTiming;
  // Right now each writer is considered its own session if not passed from
  // writer option.
  std::shared_ptr<MetricsLogger> logger;

  uint64_t memoryUsed{0};
  uint64_t bytesWritten{0};
  uint64_t rowsInFile{0};
  uint32_t rowsInStripe{0};
  // Physical size of the encoded stripe data.
  uint64_t stripeEncodedPhysicalSize{0};
  // Logical size of the encoded stripe data.
  uint64_t stripeEncodedLogicalSize{0};
  uint64_t fileRawSize{0};
  std::vector<uint64_t> rowsPerStripe;

  WriterContext(
      velox::memory::MemoryPool& memoryPool,
      VeloxWriterOptions options)
      : FieldWriterContext{memoryPool, options.reclaimerFactory(), options.vectorDecoderVisitor},
        options{std::move(options)},
        logger{this->options.metricsLogger} {
    inputBufferGrowthPolicy = this->options.lowMemoryMode
        ? std::make_unique<ExactGrowthPolicy>()
        : this->options.inputGrowthPolicyFactory();
    if (!logger) {
      logger = std::make_shared<MetricsLogger>();
    }
    ignoreTopLevelNulls = this->options.ignoreTopLevelNulls;
  }

  void nextStripe() {
    totalFlushTiming.add(stripeFlushTiming);
    stripeFlushTiming.clear();
    rowsPerStripe.push_back(rowsInStripe);
    memoryUsed = 0;
    rowsInStripe = 0;
    stripeEncodedPhysicalSize = 0;
    stripeEncodedLogicalSize = 0;
    ++stripeIndex_;
  }

  size_t getStripeIndex() const {
    return stripeIndex_;
  }

 private:
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
      : StreamData(streamData.descriptor()), streamData_{streamData} {
    streamData_.materialize();
  }

  inline virtual std::string_view data() const override {
    return {
        reinterpret_cast<const char*>(streamData_.nonNulls().data()),
        streamData_.nonNulls().size()};
  }

  inline virtual std::span<const bool> nonNulls() const override {
    return {};
  }

  inline virtual bool hasNulls() const override {
    return false;
  }

  inline virtual bool empty() const override {
    return streamData_.empty();
  }

  inline virtual uint64_t memoryUsed() const override {
    return streamData_.memoryUsed();
  }

  inline virtual void reset() override {
    streamData_.reset();
  }

 private:
  StreamData& streamData_;
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
        context.options.compressionOptions,
        context.options.encodingSelectionPolicyFactory);

  } else {
    policy = std::unique_ptr<EncodingSelectionPolicy<T>>(
        static_cast<EncodingSelectionPolicy<T>*>(
            context.options
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

    // Incompatible captured encoding.Try again without a captured encoding.
    return encode<T>(std::nullopt, context, buffer, streamData);
  }
}

std::string_view encodeStream(
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
std::unique_ptr<FieldWriter> createRootField(
    detail::WriterContext& context,
    const std::shared_ptr<const velox::dwio::common::TypeWithId>& type) {
  if (!context.options.flatMapColumns.empty()) {
    context.flatMapNodeIds.clear();
    context.flatMapNodeIds.reserve(context.options.flatMapColumns.size());
    for (const auto& column : context.options.flatMapColumns) {
      context.flatMapNodeIds.insert(type->childByName(column)->id());
    }
  }

  if (!context.options.dictionaryArrayColumns.empty()) {
    context.dictionaryArrayNodeIds.clear();
    context.dictionaryArrayNodeIds.reserve(
        context.options.dictionaryArrayColumns.size());
    for (const auto& column : context.options.dictionaryArrayColumns) {
      findNodeIds(
          *type->childByName(column),
          context.dictionaryArrayNodeIds,
          [](const velox::dwio::common::TypeWithId& type) {
            return type.type()->kind() == velox::TypeKind::ARRAY;
          });
    }
  }

  if (!context.options.deduplicatedMapColumns.empty()) {
    context.deduplicatedMapNodeIds.clear();
    context.deduplicatedMapNodeIds.reserve(
        context.options.deduplicatedMapColumns.size());
    for (const auto& column : context.options.deduplicatedMapColumns) {
      findNodeIds(
          *type->childByName(column),
          context.deduplicatedMapNodeIds,
          [](const velox::dwio::common::TypeWithId& type) {
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
      NIMBLE_CHECK(
          encodingLayoutTree.schemaKind() == Kind::FlatMap,
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
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Scalar,
              "Incompatible encoding layout node. Expecting scalar node.");
          _SET_STREAM_CONTEXT(
              typeBuilder.asScalar(), scalarDescriptor, Scalar::ScalarStream);
          break;
        }
        case Kind::Row: {
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Row,
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
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Array,
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
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Map,
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
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::SlidingWindowMap,
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
          NIMBLE_CHECK(
              encodingLayoutTree.schemaKind() == Kind::ArrayWithOffsets,
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
    velox::memory::MemoryPool& memoryPool,
    const velox::TypePtr& schema,
    std::unique_ptr<velox::WriteFile> file,
    VeloxWriterOptions options)
    : schema_{velox::dwio::common::TypeWithId::create(schema)},
      file_{std::move(file)},
      writerMemoryPool_{MemoryPoolHolder::create(
          memoryPool,
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
      writer_{
          *encodingMemoryPool_,
          file_.get(),
          {.layoutPlanner = std::make_unique<DefaultLayoutPlanner>(
               [&sb = context_->schemaBuilder]() { return sb.getRoot(); },
               context_->options.featureReordering),
           .metadataCompressionThreshold =
               context_->options.metadataCompressionThreshold.value_or(
                   kMetadataCompressionThreshold)}},
      root_{createRootField(*context_, schema_)},
      spillConfig_{context_->options.spillConfig} {
  NIMBLE_CHECK(file_, "File is null");

  if (context_->options.encodingLayoutTree.has_value()) {
    context_->flatmapFieldAddedEventHandler =
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
        };
    initializeEncodingLayouts(
        *root_->typeBuilder(), context_->options.encodingLayoutTree.value());
  }
}

VeloxWriter::~VeloxWriter() {}

bool VeloxWriter::write(const velox::VectorPtr& vector) {
  if (lastException_) {
    std::rethrow_exception(lastException_);
  }

  NIMBLE_CHECK(file_, "Writer is already closed");
  try {
    auto size = vector->size();

    // Calculate raw size.
    auto rawSize = nimble::getRawSizeFromVector(
        vector, velox::common::Ranges::of(0, size));
    DWIO_ENSURE_GE(rawSize, 0, "Invalid raw size");
    context_->fileRawSize += rawSize;

    if (context_->options.writeExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options.writeExecutor};
      root_->write(vector, OrderedRanges::of(0, size), &barrier);
      barrier.waitAll();
    } else {
      root_->write(vector, OrderedRanges::of(0, size));
    }

    uint64_t memoryUsed = 0;
    for (const auto& stream : context_->streams()) {
      memoryUsed += stream->memoryUsed();
    }

    context_->memoryUsed = memoryUsed;
    context_->rowsInFile += size;
    context_->rowsInStripe += size;
    context_->bytesWritten = file_->size();

    return evalauateFlushPolicy();
  } catch (const std::exception& e) {
    lastException_ = std::current_exception();
    context_->logger->logException(LogOperation::Write, e.what());
    throw;
  } catch (...) {
    lastException_ = std::current_exception();
    context_->logger->logException(
        LogOperation::Write,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

void VeloxWriter::close() {
  if (lastException_) {
    std::rethrow_exception(lastException_);
  }

  if (file_) {
    try {
      writeStripe();
      root_->close();

      if (!context_->options.metadata.empty()) {
        auto& metadata = context_->options.metadata;
        auto it = metadata.cbegin();
        flatbuffers::FlatBufferBuilder builder(kInitialSchemaSectionSize);
        auto entries = builder.CreateVector<
            flatbuffers::Offset<serialization::MetadataEntry>>(
            metadata.size(), [&builder, &it](size_t /* i */) {
              auto entry = serialization::CreateMetadataEntry(
                  builder,
                  builder.CreateString(it->first),
                  builder.CreateString(it->second));
              ++it;
              return entry;
            });

        builder.Finish(serialization::CreateMetadata(builder, entries));
        writer_.writeOptionalSection(
            std::string(kMetadataSection),
            {reinterpret_cast<const char*>(builder.GetBufferPointer()),
             builder.GetSize()});
      }

      {
        nimble::aggregateStats(
            *context_->schemaBuilder.getRoot(), context_->columnStats);
        // TODO(T228118622): Write column stats to file.
        flatbuffers::FlatBufferBuilder builder;
        builder.Finish(
            serialization::CreateStats(builder, context_->fileRawSize));
        writer_.writeOptionalSection(
            std::string(kStatsSection),
            {reinterpret_cast<const char*>(builder.GetBufferPointer()),
             builder.GetSize()});
      }

      {
        SchemaSerializer serializer;
        writer_.writeOptionalSection(
            std::string(kSchemaSection),
            serializer.serialize(context_->schemaBuilder));
      }

      writer_.close();
      file_->close();
      context_->bytesWritten = file_->size();

      auto runStats = getRunStats();
      // TODO: compute and populate input size.
      FileCloseMetrics metrics{
          .rowCount = context_->rowsInFile,
          .stripeCount = context_->getStripeIndex(),
          .fileSize = context_->bytesWritten,
          .totalFlushCpuUsec = runStats.flushCpuTimeUsec,
          .totalFlushWallTimeUsec = runStats.flushWallTimeUsec};
      context_->logger->logFileClose(metrics);
      file_ = nullptr;
    } catch (const std::exception& e) {
      lastException_ = std::current_exception();
      context_->logger->logException(LogOperation::Close, e.what());
      file_ = nullptr;
      throw;
    } catch (...) {
      lastException_ = std::current_exception();
      context_->logger->logException(
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
    context_->logger->logException(LogOperation::Flush, e.what());
    throw;
  } catch (...) {
    lastException_ = std::current_exception();
    context_->logger->logException(
        LogOperation::Flush,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

void VeloxWriter::writeChunk(bool lastChunk) {
  uint64_t previousFlushWallTime = context_->stripeFlushTiming.wallNanos;
  std::atomic<uint64_t> chunkSize = 0;
  {
    LoggingScope scope{*context_->logger};
    velox::CpuWallTimer veloxTimer{context_->stripeFlushTiming};

    if (!encodingBuffer_) {
      encodingBuffer_ = std::make_unique<Buffer>(*encodingMemoryPool_);
    }
    streams_.resize(context_->schemaBuilder.nodeCount());

    auto encode = [&](StreamData& streamData, uint64_t& streamSize) {
      const auto offset = streamData.descriptor().offset();
      auto encoded = encodeStream(*context_, *encodingBuffer_, streamData);
      if (!encoded.empty()) {
        ChunkedStreamWriter chunkWriter{*encodingBuffer_};
        NIMBLE_DCHECK_LT(
            offset, streams_.size(), "Stream offset out of range.");
        auto& stream = streams_[offset];
        for (auto& buffer : chunkWriter.encode(encoded)) {
          streamSize += buffer.size();
          chunkSize += buffer.size();
          stream.content.push_back(std::move(buffer));
        }
      }
      streamData.reset();
    };

    auto processStream = [&](StreamData& streamData,
                             std::function<void(StreamData&, bool)> encoder) {
      const auto offset = streamData.descriptor().offset();
      const auto* context =
          streamData.descriptor().context<WriterStreamContext>();

      const auto minStreamSize =
          lastChunk ? 0 : context_->options.minStreamChunkRawSize;

      if (context && context->isNullStream) {
        // For null streams we promote the null values to be written as
        // boolean data.
        // We still apply the same null logic, where if all values are
        // non-nulls, we omit the entire stream.
        if ((streamData.hasNulls() &&
             streamData.nonNulls().size() > minStreamSize) ||
            (lastChunk && !streamData.empty() &&
             !streams_[offset].content.empty())) {
          encoder(streamData, true);
        }
      } else {
        if (streamData.data().size() > minStreamSize ||
            (lastChunk && streamData.nonNulls().size() > 0 &&
             !streams_[offset].content.empty())) {
          encoder(streamData, false);
        }
      }
    };

    if (context_->options.encodingExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options.encodingExecutor};
      for (auto& streamData : context_->streams()) {
        auto& streamSize =
            context_->columnStats[streamData->descriptor().offset()]
                .physicalSize;
        processStream(
            *streamData, [&](StreamData& innerStreamData, bool isNullStream) {
              barrier.add(
                  [&innerStreamData, isNullStream, &encode, &streamSize]() {
                    if (isNullStream) {
                      NullsAsDataStreamData nullsStreamData{innerStreamData};
                      encode(nullsStreamData, streamSize);
                    } else {
                      encode(innerStreamData, streamSize);
                    }
                  });
            });
      }
      barrier.waitAll();
    } else {
      for (auto& streamData : context_->streams()) {
        auto& streamSize =
            context_->columnStats[streamData->descriptor().offset()]
                .physicalSize;
        processStream(
            *streamData,
            [&encode, &streamSize](
                StreamData& innerStreamData, bool isNullStream) {
              if (isNullStream) {
                NullsAsDataStreamData nullsStreamData{innerStreamData};
                encode(nullsStreamData, streamSize);
              } else {
                encode(innerStreamData, streamSize);
              }
            });
      }
    }

    if (lastChunk) {
      root_->reset();
    }
  }

  // Consider getting this from flush timing.
  auto flushWallTimeMs =
      (context_->stripeFlushTiming.wallNanos - previousFlushWallTime) /
      1'000'000;
  VLOG(1) << "writeChunk milliseconds: " << flushWallTimeMs
          << ", chunk bytes: " << chunkSize;
}

bool VeloxWriter::writeChunks(
    std::span<const uint32_t> streamIndices,
    bool ensureFullChunks,
    bool lastChunk) {
  uint64_t previousFlushWallTime = context_->stripeFlushTiming.wallNanos;
  std::atomic<uint64_t> chunkSize = 0;
  std::atomic<uint64_t> logicalSizeBeforeEncoding = 0;
  std::atomic<bool> wroteChunk = false;
  {
    LoggingScope scope{*context_->logger};
    velox::CpuWallTimer veloxTimer{context_->stripeFlushTiming};

    if (!encodingBuffer_) {
      encodingBuffer_ = std::make_unique<Buffer>(*encodingMemoryPool_);
    }
    streams_.resize(context_->schemaBuilder.nodeCount());

    auto processStream = [&](StreamData& streamData, uint64_t& streamSize) {
      logicalSizeBeforeEncoding += streamData.memoryUsed();
      const auto& offset = streamData.descriptor().offset();
      auto& streamContent = streams_[offset].content;
      auto chunker = getStreamChunker(
          streamData,
          StreamChunkerOptions{
              .minChunkSize =
                  lastChunk ? 0 : context_->options.minStreamChunkRawSize,
              .maxChunkSize = context_->options.maxStreamChunkRawSize,
              .ensureFullChunks = ensureFullChunks,
              .isFirstChunk = streamContent.empty()});
      while (auto streamDataView = chunker->next()) {
        std::string_view encoded =
            encodeStream(*context_, *encodingBuffer_, *streamDataView);
        if (!encoded.empty()) {
          ChunkedStreamWriter chunkWriter{*encodingBuffer_};
          for (auto& buffer : chunkWriter.encode(encoded)) {
            streamSize += buffer.size();
            chunkSize += buffer.size();
            streamContent.push_back(std::move(buffer));
          }
        }
        wroteChunk = true;
      }
      // Compact erases processed stream data to reclaim memory.
      chunker->compact();
      logicalSizeBeforeEncoding -= streamData.memoryUsed();
    };

    const auto& streams = context_->streams();
    if (context_->options.encodingExecutor) {
      velox::dwio::common::ExecutorBarrier barrier{
          context_->options.encodingExecutor};
      for (auto streamIndex : streamIndices) {
        auto& streamData = streams[streamIndex];
        const auto offset = streamData->descriptor().offset();
        auto& streamSize = context_->columnStats[offset].physicalSize;
        barrier.add([&] { processStream(*streamData, streamSize); });
      }
      barrier.waitAll();
    } else {
      for (auto streamIndex : streamIndices) {
        auto& streamData = streams[streamIndex];
        const auto offset = streamData->descriptor().offset();
        auto& streamSize = context_->columnStats[offset].physicalSize;
        processStream(*streamData, streamSize);
      }
    }

    if (lastChunk) {
      root_->reset();
    }

    context_->stripeEncodedPhysicalSize += chunkSize;
    context_->stripeEncodedLogicalSize += logicalSizeBeforeEncoding;
    context_->memoryUsed -= logicalSizeBeforeEncoding;
  }

  // Consider getting this from flush timing.
  auto flushWallTimeMs =
      (context_->stripeFlushTiming.wallNanos - previousFlushWallTime) /
      1'000'000;
  VLOG(1) << "writeChunk milliseconds: " << flushWallTimeMs
          << ", chunk bytes: " << chunkSize;
  return wroteChunk;
}

bool VeloxWriter::writeStripe() {
  if (context_->rowsInStripe == 0) {
    return false;
  }

  if (context_->options.enableChunking) {
    // Chunk all streams.
    std::vector<uint32_t> streamIndices(context_->streams().size());
    std::iota(streamIndices.begin(), streamIndices.end(), 0);
    writeChunks(streamIndices, /*ensureFullChunks=*/false, /*lastChunk=*/true);
  } else {
    writeChunk(true);
  }

  uint64_t previousFlushWallTime = context_->stripeFlushTiming.wallNanos;
  uint64_t stripeSize = 0;
  {
    LoggingScope scope{*context_->logger};
    velox::CpuWallTimer veloxTimer{context_->stripeFlushTiming};

    size_t nonEmptyCount = 0;
    for (auto i = 0; i < streams_.size(); ++i) {
      auto& source = streams_[i];
      if (!source.content.empty()) {
        source.offset = i;
        if (nonEmptyCount != i) {
          streams_[nonEmptyCount] = std::move(source);
        }
        ++nonEmptyCount;
      }
    }
    streams_.resize(nonEmptyCount);

    uint64_t startSize = writer_.size();
    writer_.writeStripe(context_->rowsInStripe, std::move(streams_));
    stripeSize = writer_.size() - startSize;
    encodingBuffer_.reset();
    // TODO: once chunked string fields are supported, move string buffer
    // reset to writeChunk()
    context_->resetStringBuffer();
  }

  NIMBLE_CHECK_LT(
      stripeSize,
      std::numeric_limits<uint32_t>::max(),
      "unexpected stripe size {}",
      stripeSize);

  // Consider getting this from flush timing.
  auto flushWallTimeMs =
      (context_->stripeFlushTiming.wallNanos - previousFlushWallTime) /
      1'000'000;

  VLOG(1) << "writeStripe milliseconds: " << flushWallTimeMs
          << ", on disk stripe bytes: " << stripeSize;

  StripeFlushMetrics metrics{
      .inputSize = context_->stripeEncodedPhysicalSize,
      .rowCount = context_->rowsInStripe,
      .stripeSize = stripeSize,
      .trackedMemory = context_->memoryUsed,
  };
  context_->logger->logStripeFlush(metrics);

  context_->nextStripe();
  return true;
}

bool VeloxWriter::evalauateFlushPolicy() {
  auto flushPolicy = context_->options.flushPolicyFactory();
  NIMBLE_DCHECK_NOT_NULL(flushPolicy, "Flush policy must not be null");

  auto shouldFlush = [&]() {
    return flushPolicy->shouldFlush(
        StripeProgress{
            .stripeRawSize = context_->memoryUsed,
            .stripeEncodedSize = context_->stripeEncodedPhysicalSize,
            .stripeEncodedLogicalSize = context_->stripeEncodedLogicalSize});
  };

  auto shouldChunk = [&]() {
    return flushPolicy->shouldChunk(
        StripeProgress{
            .stripeRawSize = context_->memoryUsed,
            .stripeEncodedSize = context_->stripeEncodedPhysicalSize,
            .stripeEncodedLogicalSize = context_->stripeEncodedLogicalSize,
        });
  };

  if (context_->options.enableChunking && shouldChunk()) {
    auto batchChunkStreams = [&](const std::vector<uint32_t>& indices,
                                 bool ensureFullChunks) {
      const size_t indicesCount = indices.size();
      const auto batchSize = context_->options.chunkedStreamBatchSize;
      for (size_t index = 0; index < indicesCount; index += batchSize) {
        size_t currentBatchSize = std::min(batchSize, indicesCount - index);
        std::span<const uint32_t> batchIndices(
            indices.begin() + index, currentBatchSize);
        // Stop attempting chunking once streams are too small to chunk or
        // memory pressure is relieved.
        if (!writeChunks(batchIndices, ensureFullChunks) || !shouldChunk()) {
          return false;
        }
      }
      return true;
    };

    // Relieve memory pressure by chunking streams above max size.
    const auto& streams = context_->streams();
    std::vector<uint32_t> streamIndices;
    streamIndices.reserve(streams.size());
    for (auto streamIndex = 0; streamIndex < streams.size(); ++streamIndex) {
      if (streams[streamIndex]->memoryUsed() >=
          context_->options.maxStreamChunkRawSize) {
        streamIndices.push_back(streamIndex);
      }
    }
    const bool continueChunking =
        batchChunkStreams(streamIndices, /*ensureFullChunks=*/true);
    if (continueChunking) {
      // Relieve memory pressure by chunking small streams.
      // Sort streams for chunking based on raw memory usage.
      // TODO(T240072104): Improve performance by bucketing the streams
      // by size (by most significant bit) instead of sorting them.
      streamIndices.resize(streams.size());
      std::iota(streamIndices.begin(), streamIndices.end(), 0);
      std::sort(
          streamIndices.begin(),
          streamIndices.end(),
          [&](const uint32_t& a, const uint32_t& b) {
            return streams[a]->memoryUsed() > streams[b]->memoryUsed();
          });
      batchChunkStreams(streamIndices, /*ensureFullChunks=*/false);
    }
  }

  if (shouldFlush()) {
    return writeStripe();
  }
  return false;
}

VeloxWriter::RunStats VeloxWriter::getRunStats() const {
  return RunStats{
      .bytesWritten = context_->bytesWritten,
      .stripeCount = folly::to<uint32_t>(context_->getStripeIndex()),
      .rawSize = context_->fileRawSize,
      .rowsPerStripe = context_->rowsPerStripe,
      .flushCpuTimeUsec = context_->totalFlushTiming.cpuNanos / 1000,
      .flushWallTimeUsec = context_->totalFlushTiming.wallNanos / 1000,
      .encodingSelectionCpuTimeUsec =
          context_->encodingSelectionTiming.cpuNanos / 1000,
      .inputBufferReallocCount = context_->inputBufferGrowthStats.count,
      .inputBufferReallocItemCount = context_->inputBufferGrowthStats.itemCount,
      .columnStats = context_->columnStats,
  };
}
} // namespace facebook::nimble
