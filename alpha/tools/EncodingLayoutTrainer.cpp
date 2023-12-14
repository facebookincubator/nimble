// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "dwio/alpha/tools/EncodingLayoutTrainer.h"
#include <dwio/alpha/common/Vector.h>
#include <optional>
#include <string_view>
#include "common/strings/Zstd.h"
#include "dwio/alpha/common/Exceptions.h"
#include "dwio/alpha/common/Types.h"
#include "dwio/alpha/encodings/EncodingFactoryNew.h"
#include "dwio/alpha/encodings/EncodingLayoutCapture.h"
#include "dwio/alpha/velox/StreamInputDecoder.h"
#include "dwio/alpha/velox/VeloxReader.h"
#include "dwio/api/AlphaWriterOptionBuilder.h"
#include "fbjava/datainfra-metastore/api/if/gen-cpp2/hive_metastore_types.h"
#include "thrift/lib/cpp/protocol/TBase64Utils.h"
#include "thrift/lib/cpp2/protocol/CompactProtocol.h"

namespace facebook::alpha::tools {

namespace {
template <typename T>
T deserialize(const std::string& source) {
  T result;
  auto compressed = apache::thrift::protocol::base64Decode(source);
  std::string uncompressed;
  if (!strings::zstdDecompress(compressed->moveToFbString(), &uncompressed)) {
    throw std::runtime_error(
        fmt::format("Unable to decompress data: {}", source));
  }
  apache::thrift::CompactSerializer::deserialize(uncompressed, result);
  return result;
}

EncodingLayoutTree traverseSchema(
    const Type& type,
    const std::function<std::optional<EncodingLayout>(const Type& type)>&
        encodingLayoutHandler,
    const std::string& name = "") {
  switch (type.kind()) {
    case Kind::Scalar: {
      return {
          type.kind(),
          encodingLayoutHandler(type),
          name,
          /* children */ {}};
    }
    case Kind::Row: {
      auto& row = type.asRow();
      std::vector<EncodingLayoutTree> children;
      children.reserve(row.childrenCount());
      for (auto i = 0; i < row.childrenCount(); ++i) {
        children.push_back(
            traverseSchema(*row.childAt(i), encodingLayoutHandler));
      }
      return {
          type.kind(), encodingLayoutHandler(type), name, std::move(children)};
    }
    case Kind::Array: {
      auto& array = type.asArray();
      return {
          type.kind(),
          encodingLayoutHandler(type),
          name,
          {
              traverseSchema(*array.elements(), encodingLayoutHandler),
          }};
    }
    case Kind::Map: {
      auto& map = type.asMap();
      return {
          type.kind(),
          encodingLayoutHandler(type),
          name,
          {
              traverseSchema(*map.keys(), encodingLayoutHandler),
              traverseSchema(*map.values(), encodingLayoutHandler),
          }};
    }
    case Kind::ArrayWithOffsets: {
      auto& array = type.asArrayWithOffsets();
      return {
          type.kind(),
          encodingLayoutHandler(type),
          name,
          {
              traverseSchema(*array.elements(), encodingLayoutHandler),
              traverseSchema(*array.offsets(), encodingLayoutHandler),
          }};
    }
    case Kind::FlatMap: {
      auto& map = type.asFlatMap();
      std::vector<EncodingLayoutTree> children;
      children.reserve(map.childrenCount());
      for (auto i = 0; i < map.childrenCount(); ++i) {
        children.push_back(traverseSchema(
            *map.childAt(i), encodingLayoutHandler, map.nameAt(i)));
      }
      return {
          type.kind(), encodingLayoutHandler(type), name, std::move(children)};
    }
  }
}

template <typename T>
EncodingLayout trainEncoding(
    velox::memory::MemoryPool& memoryPool,
    const VeloxWriterOptions& options,
    const std::vector<StreamInput*>& streams) {
  // Train on a single schema node. Load all data from all stripes and perform
  // basic encoding selection

  std::vector<alpha::Vector<T>> chunks;
  std::vector<std::unique_ptr<Encoding>> encodings;
  uint64_t rowCount = 0;
  for (const auto stream : streams) {
    while (stream->hasNext()) {
      auto chunk = stream->nextChunk();
      auto encoding = alpha::EncodingFactory::decode(memoryPool, chunk);
      Vector<T> data{&memoryPool};
      data.resize(encoding->rowCount());
      encoding->materialize(encoding->rowCount(), data.data());
      rowCount += encoding->rowCount();
      chunks.push_back(std::move(data));
      encodings.push_back(std::move(encoding));
    }
  }

  Vector<T> data{&memoryPool};
  data.reserve(rowCount);
  for (const auto& chunk : chunks) {
    for (const auto& item : chunk) {
      data.push_back(item);
    }
  }

  auto policy = std::unique_ptr<EncodingSelectionPolicy<T>>(
      static_cast<EncodingSelectionPolicy<T>*>(
          options.encodingSelectionPolicyFactory(TypeTraits<T>::dataType)
              .release()));
  Buffer buffer{memoryPool};
  std::string_view encoding;
  encoding = alpha::EncodingFactory::encode<T>(std::move(policy), data, buffer);
  return EncodingLayoutCapture::capture(encoding);
}

} // namespace

EncodingLayoutTrainer::EncodingLayoutTrainer(
    velox::memory::MemoryPool& memoryPool,
    std::vector<std::string_view> files,
    std::string serializedSerde)
    : memoryPool_{memoryPool},
      files_{std::move(files)},
      serializedSerde_{std::move(serializedSerde)} {
  ALPHA_CHECK(!files_.empty(), "No files provided to train on");
}

EncodingLayoutTree EncodingLayoutTrainer::train() {
  // Initial "training" implementation is very basic.
  // It loads a single file, and for each schema node (stream), it loads all
  // data from the file and performs encoding selection on it.
  //
  // Future versions will:
  // * Support multiple files
  // * Verify encoding selection stability across files/stripes.
  // * Perform better encoding selection (brute forcing, etc.)
  // * Add parallelism
  // * Measure read/write performance
  // * Support different cost functions

  // One file for now
  ALPHA_CHECK(files_.size() == 1, "Only supporting single file training.");
  auto& file = files_.front();

  LOG(INFO) << "Opening file " << file;

  std::unique_ptr<velox::LocalReadFile> readFile =
      std::make_unique<velox::LocalReadFile>(file);

  std::shared_ptr<alpha::Tablet> tablet =
      std::make_shared<alpha::Tablet>(memoryPool_, std::move(readFile));
  std::unique_ptr<alpha::VeloxReader> reader =
      std::make_unique<alpha::VeloxReader>(memoryPool_, tablet);

  std::vector<std::vector<std::unique_ptr<StreamInput>>> stripeStreams;
  stripeStreams.reserve(tablet->stripeCount());
  for (auto i = 0; i < tablet->stripeCount(); ++i) {
    std::vector<uint32_t> identifiers;
    identifiers.resize(tablet->streamCount(i));
    std::iota(identifiers.begin(), identifiers.end(), 0);
    stripeStreams.push_back(tablet->load(i, identifiers));
  }

  dwio::api::AlphaWriterOptionBuilder optionBuilder;
  if (!serializedSerde_.empty()) {
    optionBuilder.withSerdeParams(
        reader->getType(),
        deserialize<Apache::Hadoop::Hive::SerDeInfo>(serializedSerde_)
            .get_parameters());
  }
  auto options = optionBuilder.build();

  LOG(INFO) << "Training parameters: CompressionAcceptRatio = "
            << options.compressionOptions.compressionAcceptRatio
            << ", Zstrong.CompressionLevel = "
            << options.compressionOptions.zstrongCompressionLevel
            << ", Zstrong.DecompressionLevel = "
            << options.compressionOptions.zstrongDecompressionLevel;

  // Traverse schema. For each node, load all data and capture basic encoding
  // selection on data.
  return traverseSchema(*reader->schema(), [&](const Type& type) {
    std::vector<StreamInput*> streams;
    for (const auto& stripeStream : stripeStreams) {
      if (stripeStream.size() > type.offset() && stripeStream[type.offset()]) {
        streams.push_back(stripeStream[type.offset()].get());
      }
    }

    switch (type.kind()) {
      case Kind::Scalar: {
        auto scalar = type.asScalar();
#define SCALAR_CASE(kind, dataType) \
  case ScalarKind::kind:            \
    return trainEncoding<dataType>(memoryPool_, options, streams);

        switch (scalar.scalarKind()) {
          SCALAR_CASE(Int8, int8_t);
          SCALAR_CASE(UInt8, uint8_t);
          SCALAR_CASE(Int16, int16_t);
          SCALAR_CASE(UInt16, uint16_t);
          SCALAR_CASE(Int32, int32_t);
          SCALAR_CASE(UInt32, uint32_t);
          SCALAR_CASE(Int64, int64_t);
          SCALAR_CASE(UInt64, uint64_t);
          SCALAR_CASE(Float, float);
          SCALAR_CASE(Double, double);
          SCALAR_CASE(Bool, bool);
          case ScalarKind::String:
          case ScalarKind::Binary:
            return trainEncoding<std::string_view>(
                memoryPool_, options, streams);
          case ScalarKind::Undefined:
            ALPHA_UNREACHABLE("Scalar kind cannot be undefined.");
        }

#undef SCALAR_KIND
      }
      case Kind::Row:
      case Kind::FlatMap: {
        // Main stream for complex types (row/flatmap) is the "nullable"
        // stream (boolean stream)
        return trainEncoding<bool>(memoryPool_, options, streams);
      }
      case Kind::Array:
      case Kind::Map:
      case Kind::ArrayWithOffsets: {
        // Main stream for multi-value types (array/map/arrayWithOffsets) is
        // the "length" stream (int stream)
        return trainEncoding<int32_t>(memoryPool_, options, streams);
      }
    }
  });
}

} // namespace facebook::alpha::tools
