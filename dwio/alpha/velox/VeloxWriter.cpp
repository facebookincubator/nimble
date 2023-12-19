// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#include "dwio/alpha/velox/VeloxWriter.h"

#include <ios>
#include <memory>

#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/StopWatch.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/Encoding.h"
#include "dwio/alpha/encodings/EncodingSelectionPolicy.h"
#include "dwio/alpha/encodings/SentinelEncoding.h"
#include "dwio/alpha/tablet/Tablet.h"
#include "dwio/alpha/velox/BufferGrowthPolicy.h"
#include "dwio/alpha/velox/EncodingLayoutTree.h"
#include "dwio/alpha/velox/FieldWriter.h"
#include "dwio/alpha/velox/FlatMapLayoutPlanner.h"
#include "dwio/alpha/velox/FlushPolicy.h"
#include "dwio/alpha/velox/MetadataGenerated.h"
#include "dwio/alpha/velox/SchemaBuilder.h"
#include "dwio/alpha/velox/SchemaSerialization.h"
#include "dwio/alpha/velox/SchemaTypes.h"
#include "dwio/alpha/velox/TabletSections.h"
#include "folly/ScopeGuard.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "folly/experimental/coro/BlockingWait.h"
#include "folly/experimental/coro/Collect.h"
#include "koski/common/SliceUtil.h"
#include "velox/common/time/CpuWallTimer.h"
#include "velox/type/Type.h"

namespace facebook::alpha {

namespace detail {

class WriterContext : public FieldWriterContext {
 public:
  const VeloxWriterOptions options;
  std::unique_ptr<FlushPolicy> flushPolicy;
  velox::CpuWallTiming totalFlushTiming;
  velox::CpuWallTiming stripeFlushTiming;
  velox::CpuWallTiming encodingSelectionTiming;
  // Right now each writer is considered its own session if not passed from
  // writer option.
  std::shared_ptr<MetricsLogger> logger;

  uint64_t memoryUsed{0};
  uint64_t bytesWritten{0};
  uint64_t rowsInFile{0};
  uint64_t rowsInStripe{0};
  uint64_t stripeSize{0};
  std::vector<uint64_t> rowsPerStripe;

  WriterContext(MemoryPool& memoryPool, VeloxWriterOptions options_)
      : FieldWriterContext{memoryPool},
        options{std::move(options_)},
        logger{this->options.metricsLogger} {
    flushPolicy = this->options.flushPolicyFactory();
    inputBufferGrowthPolicy = this->options.lowMemoryMode
        ? std::make_unique<ExactGrowthPolicy>()
        : this->options.inputGrowthPolicyFactory();

    parallelEncoding = options.parallelEncoding;
    parallelWriting = options.parallelWriting;
    parallelExecutor = options.parallelExecutor;
    if ((parallelEncoding || parallelWriting) && !parallelExecutor) {
      ALPHA_RAISE_USER_ERROR(
          "parallelEncoding && parallelWriting && !parallelExecutor",
          "Parallel writing requires a parallel executor.",
          ::facebook::alpha::error_code::InvalidArgument,
          false);
    }

    if (!logger) {
      logger = std::make_shared<MetricsLogger>();
    }
  }

  void nextStripe() {
    totalFlushTiming.add(stripeFlushTiming);
    stripeFlushTiming.clear();
    rowsPerStripe.push_back(rowsInStripe);
    memoryUsed = 0;
    rowsInStripe = 0;
    stripeSize = 0;
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

class EncodingLayoutContext : public TypeBuilderContext {
 public:
  explicit EncodingLayoutContext(EncodingLayout encoding)
      : TypeBuilderContext(), encoding{std::move(encoding)} {}

  EncodingLayout encoding;
};

class FlatmapEncodingLayoutContext : public TypeBuilderContext {
 public:
  FlatmapEncodingLayoutContext(
      std::optional<EncodingLayout> encoding,
      folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
          keyEncodings)
      : TypeBuilderContext(),
        encoding{std::move(encoding)},
        keyEncodings{std::move(keyEncodings)} {}

  const std::optional<EncodingLayout> encoding;
  const folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
      keyEncodings;
};

template <typename T>
std::string_view encode(
    std::optional<EncodingLayout> encodingLayout,
    detail::WriterContext& context,
    Buffer& buffer,
    std::span<const bool>* nonNulls,
    std::span<const T> values) {
  std::unique_ptr<EncodingSelectionPolicy<T>> policy;
  if (encodingLayout.has_value()) {
    policy = std::make_unique<ReplayedEncodingSelectionPolicy<T>>(
        encodingLayout.value(),
        context.options.compressionOptions,
        context.options.encodingSelectionPolicyFactory);

  } else {
    policy = std::unique_ptr<EncodingSelectionPolicy<T>>(
        static_cast<EncodingSelectionPolicy<T>*>(
            context.options
                .encodingSelectionPolicyFactory(TypeTraits<T>::dataType)
                .release()));
  }

  if (nonNulls) {
    return EncodingFactory::encodeNullable<T>(
        std::move(policy), values, *nonNulls, buffer);
  } else {
    return EncodingFactory::encode<T>(std::move(policy), values, buffer);
  }
}

template <typename T>
std::string_view encodeStreamTyped(
    detail::WriterContext& context,
    Buffer& buffer,
    const TypeBuilder* type,
    std::span<const bool>* nonNulls,
    std::string_view data) {
  ALPHA_ASSERT(
      data.size() % sizeof(T) == 0,
      fmt::format("Unexpected size {}", data.size()));
  std::span<const T> values{
      reinterpret_cast<const T*>(data.data()), data.size() / sizeof(T)};

  const auto& typeContext = type->context();
  std::optional<EncodingLayout> encodingLayout;
  if (typeContext) {
    if (type->kind() == Kind::FlatMap) {
      auto* flatmapTypeContext =
          dynamic_cast<FlatmapEncodingLayoutContext*>(typeContext.get());
      ALPHA_DASSERT(
          flatmapTypeContext, "Expecting flatmap encoding layout context.");
      if (flatmapTypeContext->encoding.has_value()) {
        encodingLayout.emplace(flatmapTypeContext->encoding.value());
      }
    } else {
      auto nodeTypeContext =
          dynamic_cast<EncodingLayoutContext*>(typeContext.get());
      ALPHA_DASSERT(nodeTypeContext, "Expecting node encoding layout context.");
      encodingLayout.emplace(nodeTypeContext->encoding);
    }
  }

  try {
    return encode(encodingLayout, context, buffer, nonNulls, values);
  } catch (const AlphaUserError& e) {
    if (e.errorCode() != error_code::IncompatibleEncoding ||
        !encodingLayout.has_value()) {
      throw;
    }

    // Incompatible captured encoding.Try again without a captured encoding.
    return encode(std::nullopt, context, buffer, nonNulls, values);
  }
}

std::string_view encodeStream(
    detail::WriterContext& context,
    Buffer& buffer,
    const TypeBuilder* type,
    std::span<const bool>* nonNulls,
    std::string_view cols) {
  switch (type->kind()) {
    case Kind::Scalar: {
      auto scalarKind = type->asScalar().scalarKind();
      switch (scalarKind) {
        case ScalarKind::Bool:
          return encodeStreamTyped<bool>(context, buffer, type, nonNulls, cols);
        case ScalarKind::Int8:
          return encodeStreamTyped<int8_t>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::Int16:
          return encodeStreamTyped<int16_t>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::Int32:
          return encodeStreamTyped<int32_t>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::UInt32:
          return encodeStreamTyped<uint32_t>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::Int64:
          return encodeStreamTyped<int64_t>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::Float:
          return encodeStreamTyped<float>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::Double:
          return encodeStreamTyped<double>(
              context, buffer, type, nonNulls, cols);
        case ScalarKind::String:
        case ScalarKind::Binary:
          return encodeStreamTyped<std::string_view>(
              context, buffer, type, nonNulls, cols);
        default:
          ALPHA_UNREACHABLE(
              fmt::format("Unsupported scalar kind {}", scalarKind));
      }
      break;
    }
    case Kind::Row:
    case Kind::FlatMap:
      return encodeStreamTyped<bool>(context, buffer, type, nonNulls, cols);
    case Kind::Array:
    case Kind::ArrayWithOffsets:
    case Kind::Map:
      return encodeStreamTyped<int32_t>(context, buffer, type, nonNulls, cols);
    default:
      ALPHA_UNREACHABLE(fmt::format("Unsupported type kind {}", type->kind()));
  }
}

template <typename Set>
void findNodeIds(
    const velox::dwio::common::TypeWithId& typeWithId,
    Set& output,
    std::function<bool(const velox::dwio::common::TypeWithId&)> predicate) {
  if (predicate(typeWithId)) {
    output.insert(typeWithId.id());
  }

  for (const auto& child : typeWithId.getChildren()) {
    findNodeIds(*child, output, predicate);
  }
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

  return FieldWriter::create(context, type);
}

void initializeEncodingLayouts(
    const Type& typeBuilder,
    const EncodingLayoutTree& encodingLayoutTree) {
  {
    auto& encodingLayout = encodingLayoutTree.encodingLayout();
    if (typeBuilder.kind() == Kind::FlatMap) {
      if (encodingLayoutTree.schemaKind() == Kind::Map) {
        // Schema evolution - If a map is converted to flatmap, we should not
        // fail, but also not try to replay captured encodings.
        return;
      }
      ALPHA_CHECK(
          encodingLayoutTree.schemaKind() == Kind::FlatMap,
          "Incompatible encoding layout node. Expecting flatmap node.");
      folly::F14FastMap<std::string_view, const EncodingLayoutTree&>
          keyEncodings;
      keyEncodings.reserve(encodingLayoutTree.childrenCount());
      for (auto i = 0; i < encodingLayoutTree.childrenCount(); ++i) {
        auto& child = encodingLayoutTree.child(i);
        if (child.encodingLayout().has_value()) {
          keyEncodings.emplace(child.name(), child);
        }
      }
      dynamic_cast<const TypeBuilder&>(typeBuilder)
          .setContext(std::make_unique<FlatmapEncodingLayoutContext>(
              encodingLayout, std::move(keyEncodings)));

    } else {
      if (encodingLayout.has_value()) {
        dynamic_cast<const TypeBuilder&>(typeBuilder)
            .setContext(std::make_unique<EncodingLayoutContext>(
                encodingLayout.value()));
      }
      switch (typeBuilder.kind()) {
        case Kind::Scalar: {
          // Do nothing.
          ALPHA_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Scalar,
              "Incompatible encoding layout node. Expecting scalar node.");
          break;
        }
        case Kind::Row: {
          ALPHA_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Row,
              "Incompatible encoding layout node. Expecting row node.");
          auto& row = typeBuilder.asRow();
          for (auto i = 0; i < row.childrenCount() &&
               i < encodingLayoutTree.childrenCount();
               ++i) {
            initializeEncodingLayouts(
                *row.childAt(i), encodingLayoutTree.child(i));
          }
          break;
        }
        case Kind::Array: {
          ALPHA_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Array,
              "Incompatible encoding layout node. Expecting array node.");
          if (encodingLayoutTree.childrenCount() > 0) {
            ALPHA_CHECK(
                encodingLayoutTree.childrenCount() == 1,
                "Invalid encoding layout tree. Array node should have exactly one child.");
            auto& array = typeBuilder.asArray();
            initializeEncodingLayouts(
                *array.elements(), encodingLayoutTree.child(0));
          }
          break;
        }
        case Kind::Map: {
          if (encodingLayoutTree.schemaKind() == Kind::FlatMap) {
            // Schema evolution - If a flatmap is converted to map, we should
            // not fail, but also not try to replay captured encodings.
            return;
          }
          ALPHA_CHECK(
              encodingLayoutTree.schemaKind() == Kind::Map,
              "Incompatible encoding layout node. Expecting map node.");
          if (encodingLayoutTree.childrenCount() > 0) {
            ALPHA_CHECK(
                encodingLayoutTree.childrenCount() == 2,
                "Invalid encoding layout tree. Map node should have exactly two children.");
            auto& map = typeBuilder.asMap();
            initializeEncodingLayouts(*map.keys(), encodingLayoutTree.child(0));
            initializeEncodingLayouts(
                *map.values(), encodingLayoutTree.child(1));
          }

          break;
        }
        case Kind::ArrayWithOffsets: {
          ALPHA_CHECK(
              encodingLayoutTree.schemaKind() == Kind::ArrayWithOffsets,
              "Incompatible encoding layout node. Expecting offset array node.");
          if (encodingLayoutTree.childrenCount() > 0) {
            ALPHA_CHECK(
                encodingLayoutTree.childrenCount() == 2,
                "Invalid encoding layout tree. ArrayWithOffset node should have exactly two children.");
            auto& array = typeBuilder.asArrayWithOffsets();
            initializeEncodingLayouts(
                *array.elements(), encodingLayoutTree.child(0));
            initializeEncodingLayouts(
                *array.offsets(), encodingLayoutTree.child(1));
          }
          break;
        }
        case Kind::FlatMap: {
          ALPHA_UNREACHABLE("Flatmap handled already");
        }
      }
    }
  }
}

} // namespace

VeloxWriter::VeloxWriter(
    MemoryPool& memoryPool,
    const velox::TypePtr& schema,
    std::unique_ptr<velox::WriteFile> file,
    VeloxWriterOptions options)
    : schema_{velox::dwio::common::TypeWithId::create(schema)},
      file_{std::move(file)},
      writerMemoryPool_{memoryPool.addAggregateChild(
          fmt::format("velox_writer_{}", folly::Random::rand64()))},
      encodingMemoryPool_{writerMemoryPool_->addLeafChild("encoding")},
      context_{std::make_unique<detail::WriterContext>(
          *writerMemoryPool_,
          std::move(options))},
      writer_{
          *encodingMemoryPool_,
          file_.get(),
          {.layoutPlanner = context_->options.featureReordering.has_value()
               ? std::make_unique<FlatMapLayoutPlanner>(
                     [&sb = context_->schemaBuilder]() { return sb.getRoot(); },
                     context_->options.featureReordering.value())
               : nullptr}},
      root_{createRootField(*context_, schema_)} {
  ALPHA_CHECK(file_, "File is null");
  VLOG(1) << "Using Encoding Selection: " << std::boolalpha
          << context_->options.useEncodingSelectionPolicy;
  if (context_->options.encodingLayoutTree.has_value()) {
    context_->flatmapFieldAddedEventHandler = [](const TypeBuilder& flatmap,
                                                 std::string_view fieldKey,
                                                 const TypeBuilder& fieldType) {
      auto& context = flatmap.context();
      if (context) {
        auto& flatmapContext =
            dynamic_cast<FlatmapEncodingLayoutContext&>(*context);
        auto it = flatmapContext.keyEncodings.find(fieldKey);
        if (it != flatmapContext.keyEncodings.end()) {
          initializeEncodingLayouts(fieldType, it->second);
        }
      }
    };
    initializeEncodingLayouts(
        *root_->typeBuilder(), context_->options.encodingLayoutTree.value());
  }
}

VeloxWriter::~VeloxWriter() {
  try {
    close();
  } catch (const std::exception& ex) {
    LOG(WARNING) << "Failed to close velox writer: " << ex.what();
  }
}

bool VeloxWriter::write(const velox::VectorPtr& vector) {
  ALPHA_CHECK(file_, "Writer is already closed");
  auto size = vector->size();
  context_->memoryUsed += root_->write(vector, OrderedRanges::of(0, size));
  context_->rowsInFile += size;
  context_->rowsInStripe += size;
  return tryWriteStripe();
}

void VeloxWriter::close() {
  if (file_) {
    auto exitGuard =
        folly::makeGuard([this]() { context_->flushPolicy->onClose(); });
    flush();
    try {
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
      context_->logger->logException(
          MetricsLogger::kFileCloseOperation, e.what());
      file_ = nullptr;
      throw;
    } catch (...) {
      context_->logger->logException(
          MetricsLogger::kFileCloseOperation,
          folly::to<std::string>(
              folly::exceptionStr(std::current_exception())));
      file_ = nullptr;
      throw;
    }
  }
}

void VeloxWriter::flush() {
  tryWriteStripe(true);
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
    StreamCollector collector = [this, &chunkSize](
                                    const TypeBuilder* type,
                                    std::span<const bool>* nonNulls,
                                    std::string_view cols) {
      auto encoded =
          encodeStream(*context_, *encodingBuffer_, type, nonNulls, cols);
      if (!encoded.empty()) {
        streams_.at(type->offset()).add(encoded);
        chunkSize += encoded.size();
      }
    };

    if (context_->options.parallelEncoding) {
      context_->tasks.clear();
      context_->tasks.reserve(streams_.size());

      root_->flush(collector, lastChunk);
      folly::coro::blockingWait(
          folly::coro::collectAllRange(std::move(context_->tasks)));
    } else {
      root_->flush(collector, lastChunk);
    }

    context_->stripeSize += chunkSize;
  }

  // Consider getting this from flush timing.
  auto flushWallTimeMs =
      (context_->stripeFlushTiming.wallNanos - previousFlushWallTime) /
      1'000'000;
  VLOG(1) << "writeChunk milliseconds: " << flushWallTimeMs
          << ", chunk bytes: " << chunkSize;
}

uint32_t VeloxWriter::writeStripe() {
  writeChunk(true);

  uint64_t previousFlushWallTime = context_->stripeFlushTiming.wallNanos;
  uint64_t stripeSize = 0;
  {
    LoggingScope scope{*context_->logger};
    velox::CpuWallTimer veloxTimer{context_->stripeFlushTiming};

    size_t nonEmptyCount = 0;
    for (auto i = 0; i < streams_.size(); ++i) {
      auto& source = streams_[i];
      if (source.size > 0) {
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
    // TODO: once chunked string fields are supported, move string buffer reset
    // to writeChunk()
    context_->resetStringBuffer();
  }

  ALPHA_ASSERT(
      stripeSize < std::numeric_limits<uint32_t>::max(),
      fmt::format("unexpected stripe size {}", stripeSize));

  // Consider getting this from flush timing.
  auto flushWallTimeMs =
      (context_->stripeFlushTiming.wallNanos - previousFlushWallTime) /
      1'000'000;

  VLOG(1) << "writeStripe milliseconds: " << flushWallTimeMs
          << ", on disk stripe bytes: " << stripeSize;

  return static_cast<uint32_t>(stripeSize);
}

bool VeloxWriter::tryWriteStripe(bool force) {
  if (context_->rowsInStripe == 0) {
    return false;
  }

  auto shouldFlush = [&]() {
    return context_->flushPolicy->shouldFlush(StripeProgress{
        .rawStripeSize = context_->memoryUsed,
        .stripeSize = context_->stripeSize,
        .bufferSize =
            static_cast<uint64_t>(context_->bufferMemoryPool->currentBytes()),
    });
  };

  auto decision = force ? FlushDecision::Stripe : shouldFlush();
  if (decision == FlushDecision::None) {
    return false;
  }

  try {
    // TODO: we can improve merge the last chunk write with stripe
    if (decision == FlushDecision::Chunk && context_->options.enableChunking) {
      writeChunk(false);
      decision = shouldFlush();
    }

    if (decision != FlushDecision::Stripe) {
      return false;
    }

    StripeFlushMetrics metrics{
        .inputSize = context_->stripeSize,
        .rowCount = context_->rowsInStripe,
        .trackedMemory = context_->memoryUsed,
    };

    metrics.stripeSize = writeStripe();
    context_->logger->logStripeFlush(metrics);

    context_->nextStripe();
    return true;
  } catch (const std::exception& e) {
    context_->logger->logException(
        MetricsLogger::kStripeFlushOperation, e.what());
    throw;
  } catch (...) {
    context_->logger->logException(
        MetricsLogger::kStripeFlushOperation,
        folly::to<std::string>(folly::exceptionStr(std::current_exception())));
    throw;
  }
}

VeloxWriter::RunStats VeloxWriter::getRunStats() const {
  return RunStats{
      .bytesWritten = context_->bytesWritten,
      .stripeCount = folly::to<uint32_t>(context_->getStripeIndex()),
      .rowsPerStripe = context_->rowsPerStripe,
      .flushCpuTimeUsec = context_->totalFlushTiming.cpuNanos / 1000,
      .flushWallTimeUsec = context_->totalFlushTiming.wallNanos / 1000,
      .encodingSelectionCpuTimeUsec =
          context_->encodingSelectionTiming.cpuNanos / 1000,
      .inputBufferReallocCount = context_->inputBufferGrowthStats.count,
      .inputBufferReallocItemCount =
          context_->inputBufferGrowthStats.itemCount};
}
} // namespace facebook::alpha
